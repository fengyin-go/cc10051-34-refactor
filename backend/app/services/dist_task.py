"""分发任务领域服务：管理把发布版本分发给订阅方的批次调度、送达确认与失败重试，完成导出动作。

数据链路位置：主数据分发层执行端，负责消费发布版本与订阅过滤，
把送达回执写入回执中心。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.dispatch_registry import registry
from app.store import store


class DistTaskService(BaseModuleService):
    MODULE = "dist_task"
    ENTITY = "分发批次"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["分发批次", "订阅方", "记录范围", "分发状态", "回执状态"]
    REQUIRED_FIELDS = ["分发批次", "订阅方", "记录范围"]
    STATUS_ORDER = ["排队中", "分发中", "已送达", "分发失败"]
    ACTION_RULES = {"开始分发": "分发中", "重试分发": "分发失败"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：版本分发与回执回收 ----

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """通用登记入口也使用统一编号锁，避免与两个业务入口争用 max(id)+1。"""
        with registry.lock:
            return super().create_entry(values)

    def dispatch_batch(self, entry_id: int) -> dict[str, Any]:
        """执行分发：校验订阅方生效后置为分发中。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "分发批次不存在"}
        subscriber = entry.get("订阅方", "")
        sub = next((s for s in store.rows("subscriber_mgr")
                    if s.get("订阅方标识") == subscriber), None)
        if sub is None or sub.get("status") != "已生效":
            return {"ok": False, "message": f"订阅方 {subscriber} 未生效，拒绝分发"}
        entry["status"] = "分发中"
        entry["pending"] = True
        return {"ok": True, "subscriber": subscriber, "scope": entry.get("记录范围")}

    def confirm_delivery(self, entry_id: int) -> dict[str, Any]:
        """确认送达并向回执中心登记已回收回执。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "分发中":
            return {"ok": False, "message": "批次不在分发中状态"}
        entry["status"] = "已送达"
        entry["分发状态"] = "已送达"
        entry["回执状态"] = "已回收"
        entry["pending"] = False
        receipt = registry.register_receipt(
            number_style="task",
            task_entry_id=entry_id,
            batch_no=str(entry.get("分发批次", "") or ""),
            subscriber=str(entry.get("订阅方", "") or ""),
            status="已回收",
            diff="",
            extra={"pending": False, "abnormal": False},
        )
        return {"ok": True, "receipt_id": receipt["id"]}

    def find_task_by_batch(self, batch_no: str) -> dict[str, Any] | None:
        """按旧批次号返回分发任务行本身。"""
        return registry.find_task_by_batch(batch_no)

    def get_by_batch(
        self,
        batch_no: str,
        *,
        credential: str = "",
        subscriber: str = "",
        version: str = "",
    ) -> dict[str, Any]:
        """按旧分发批次号读取本行，并明确返回原收件方与原发放版本。"""
        return registry.claim_batch(
            batch_no=batch_no,
            credential=credential,
            subscriber=subscriber,
            version=version,
        )

    claim_by_batch = get_by_batch
    open_registration = get_by_batch
    get_by_batch_no = get_by_batch
    open_by_batch_no = get_by_batch

    def sign_by_batch(self, batch_no: str) -> dict[str, Any]:
        """分发任务页按旧批次号办理签收；不使用其他行补齐收件方或版本。"""
        claimed = registry.claim_batch(batch_no=batch_no)
        if not claimed.get("ok"):
            return claimed
        task = claimed["task"]
        task_id = int(task["id"])
        if task.get("status") == "排队中":
            dispatch = self.dispatch_batch(task_id)
            if not dispatch.get("ok"):
                return dispatch
        if task.get("status") == "已送达":
            receipt = next((
                r for r in store.rows("dist_receipt")
                if r.get("分发批次") == batch_no and r.get("回执状态") == "已回收"
            ), None)
            return {
                "ok": True,
                "message": "批次已签收",
                "receipt_id": receipt.get("id") if receipt else None,
                **claimed,
            }
        result = self.confirm_delivery(task_id)
        if result.get("ok"):
            result.update(registry.describe_task(task) or {})
        return result

    def fail_and_requeue(self, entry_id: int, reason: str) -> dict[str, Any]:
        """标记分发失败，保留断点供重试。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "分发批次不存在"}
        entry["status"] = "分发失败"
        entry["abnormal"] = True
        entry["失败原因"] = reason
        return {"ok": False, "message": f"分发失败: {reason}", "retryable": True}

    def consume_publish_scope(self, entry_id: int) -> dict[str, Any] | None:
        """读取发布模块的版本范围，校验分发覆盖。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        scope = str(entry.get("记录范围", "")).replace("版本 ", "")
        return next((p for p in store.rows("golden_publish")
                     if p.get("版本标识") == scope), None)

    def resolve_backlog(self, entry_id: int) -> int:
        """按发布批次聚合积压分发任务数。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return 0
        scope = str(entry.get("记录范围", "")).replace("版本 ", "")
        return sum(1 for t in store.rows(self.MODULE)
                   if str(t.get("记录范围", "")).endswith(scope)
                   and t.get("status") in ("排队中", "分发中"))

    def notify_subscriber_rollback(self, entry_id: int, new_version: str) -> str:
        """回滚后通知订阅方新版本口径。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return "分发批次不存在"
        subscriber = entry.get("订阅方", "")
        for sub in store.rows("subscriber_mgr"):
            if sub.get("订阅方标识") == subscriber:
                sub["pending"] = True
        entry["记录范围"] = f"版本 {new_version}"
        entry["status"] = "排队中"
        entry["pending"] = True
        return f"已通知 {subscriber} 切换到 {new_version}"
