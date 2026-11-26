"""下游订阅方领域服务：管理下游系统对金记录版本的订阅关系、过滤条件与生效状态，完成导入动作。

数据链路位置：主数据分发层的消费端配置，负责消费发布版本，
按订阅过滤生成分发批次并衔接回执通道。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.dispatch_registry import registry
from app.store import store


class SubscriberMgrService(BaseModuleService):
    MODULE = "subscriber_mgr"
    ENTITY = "订阅方"
    OPERATION_TERM = "导入"
    LIST_FIELDS = ["订阅方标识", "订阅实体", "分发格式", "订阅过滤", "生效状态"]
    REQUIRED_FIELDS = ["订阅方标识", "订阅实体", "分发格式"]
    STATUS_ORDER = ["待生效", "已生效", "已暂停"]
    ACTION_RULES = {"生效订阅": "已生效", "暂停订阅": "已暂停"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：订阅生效与分发过滤 ----

    def run_action(self, entry_id: int, action: str, *_args: Any, **kwargs: Any) -> tuple[dict[str, Any] | None, str]:
        """兼容通用 /actions 入口中的单笔安排动作。"""
        produce_actions = {"单笔安排", "安排分发", "订阅派送", "produce_dist_batch", "produce"}
        if action in produce_actions:
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            payload = kwargs.get("payload")
            version = str(getattr(payload, "version", "") or "").strip()
            if not version and getattr(payload, "remark", None):
                version = str(payload.remark).strip()
            if not version:
                versions = self.consume_publish_versions(entry_id)
                version = versions[-1] if versions else ""
            if not version:
                return None, "缺少发布版本，不能安排分发"
            task_id = self.produce_dist_batch(entry_id, version)
            if not task_id:
                return None, "单笔分发登记失败"
            entry["最近分发任务ID"] = task_id
            entry["最近分发回执"] = self.submit_single_dispatch(entry_id, version)
            return entry, "单笔安排已登记"
        return super().run_action(entry_id, action, **kwargs)

    def activate_subscription(self, entry_id: int) -> dict[str, Any]:
        """生效订阅并把订阅方加入分发名单。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "订阅方不存在"}
        if not str(entry.get("分发格式", "")).strip():
            return {"ok": False, "message": "分发格式缺失，不能生效"}
        entry["status"] = "已生效"
        entry["pending"] = False
        return {"ok": True, "subscriber": entry.get("订阅方标识")}

    def apply_filter(self, entry_id: int, publish_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按订阅过滤条件筛选发布记录。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已生效":
            return []
        cond = str(entry.get("订阅过滤", "")).strip()
        if not cond:
            return publish_rows
        field, _, expected = cond.partition("=")
        return [r for r in publish_rows if str(r.get(field, "")) == expected]

    def pause_and_recall(self, entry_id: int) -> dict[str, Any]:
        """暂停订阅并回收未送达的分发批次。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "订阅方不存在"}
        entry["status"] = "已暂停"
        entry["pending"] = True
        recalled = 0
        for task in store.rows("dist_task"):
            if task.get("订阅方") == entry.get("订阅方标识") and task.get("status") == "排队中":
                task["status"] = "分发失败"
                task["abnormal"] = True
                recalled += 1
        return {"ok": True, "recalled_tasks": recalled}

    def consume_publish_versions(self, entry_id: int) -> list[str]:
        """读取订阅实体的已发布版本列表。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        entity = str(entry.get("订阅过滤", "")).partition("=")[2] or entry.get("订阅实体", "")
        return [p.get("版本标识", "") for p in store.rows("golden_publish")
                if p.get("status") == "已发布" and entity in str(p.get("实体编码", ""))]

    def produce_dist_batch(self, entry_id: int, version: str) -> int:
        """为订阅方生成分发批次，写入 dist_task。

        保留旧返回契约（任务自增 ID）和旧编号
        ``dist-{订阅方标识}-{任务ID}``；同一发布版本与同一订阅方由统一
        登记处保证最多一笔真正发放，重复安排取回原批次 ID。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return 0

        subscriber = str(entry.get("订阅方标识", "") or "")
        version = str(version or "")
        publish = next(
            (p for p in store.rows("golden_publish")
             if str(p.get("版本标识", "") or "") == version),
            None,
        )
        publish_id = int(publish["id"]) if publish is not None and publish.get("id") is not None else None
        key = registry.make_idempotency_key(
            publish_entry_id=publish_id,
            version=version,
            subscriber=subscriber,
        )
        app = registry.prepare_application(
            source="subscriber_mgr.produce_dist_batch",
            subscriber=subscriber,
            version=version,
            publish_entry_id=publish_id,
            publish_batch=str((publish or {}).get("发布批次", "") or ""),
            subscriber_entry_id=entry_id,
            number_style="subscriber",
            request_key=key,
        )
        result = registry.resume_application(app["id"])
        task_id = result.get("task_id") or result.get("id")
        if task_id is None and result.get("application"):
            task_id = result["application"].get("分发任务ID")
        return int(task_id or 0)

    def submit_single_dispatch(self, entry_id: int, version: str) -> dict[str, Any]:
        """订阅方页单笔安排的完整提交回执（旧 int 契约仍由 produce 方法保留）。"""
        task_id = self.produce_dist_batch(entry_id, version)
        entry = store.find(self.MODULE, entry_id)
        if entry is None or not task_id:
            return {"ok": False, "task_id": 0, "message": "订阅方不存在或登记失败"}
        task = store.find("dist_task", task_id)
        descriptor = registry.describe_task(task) or {}
        app = None
        if task is not None and task.get("申请编号"):
            app = registry.find_application(
                next((a["id"] for a in registry.applications()
                      if a.get("申请编号") == task.get("申请编号")), None)
            )
        if app is None:
            publish = next(
                (p for p in store.rows("golden_publish")
                 if str(p.get("版本标识", "") or "") == str(version or "")),
                None,
            )
            app = registry.find_application(
                request_key=registry.make_idempotency_key(
                    publish_entry_id=publish.get("id") if publish else None,
                    version=str(version or ""),
                    subscriber=str(entry.get("订阅方标识", "") or ""),
                )
            )
        return {
            "ok": True,
            "task_id": task_id,
            "id": task_id,
            **descriptor,
            "application": app,
            "receipt": registry.application_receipt(app) if app else None,
        }

    arrange_dispatch = submit_single_dispatch
    submit_arrangement = submit_single_dispatch

    def open_dispatch_registration(
        self,
        ref: str = "",
        *,
        batch_no: str = "",
        credential: str = "",
    ) -> dict[str, Any]:
        """订阅方页与回执中心共用的登记凭据/旧批次号打开方式。"""
        return registry.claim_batch(batch_no=batch_no, ref=ref, credential=credential)

    open_registration = open_dispatch_registration
    open_credential = open_dispatch_registration

    def ensure_receipt_collector(self, entry_id: int) -> dict[str, Any]:
        """确认订阅方回执通道可用，登记到回执中心。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "订阅方不存在"}
        subscriber = str(entry.get("订阅方标识", "") or "")
        receipt = registry.register_receipt(
            number_style="subscriber",
            subscriber=subscriber,
            status="未回收",
            diff="通道注册",
        )
        return {"ok": True, "receipt_id": receipt["id"]}
