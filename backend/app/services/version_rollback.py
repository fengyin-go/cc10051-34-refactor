"""版本回滚器领域服务：管理发布版本回滚任务的执行、影响订阅方通知与取消恢复，完成迁移动作。

数据链路位置：发布层逆向通道，负责消费发布历史与分发记录，
把回滚结果通知订阅方并登记回执。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.dispatch_registry import registry
from app.store import store


class VersionRollbackService(BaseModuleService):
    MODULE = "version_rollback"
    ENTITY = "回滚任务"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = ["回滚任务", "目标版本", "回滚原因", "执行状态", "影响订阅方"]
    REQUIRED_FIELDS = ["回滚任务", "目标版本", "回滚原因"]
    STATUS_ORDER = ["待执行", "已回滚", "回滚失败"]
    ACTION_RULES = {"执行回滚": "已回滚", "取消任务": "回滚失败"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：版本回滚与通知扇出 ----

    def execute_rollback(self, entry_id: int) -> dict[str, Any]:
        """执行回滚：校验目标版本存在后切换发布状态。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "待执行":
            return {"ok": False, "message": "回滚任务不在待执行状态"}
        target = str(entry.get("目标版本", ""))
        publish = next((p for p in store.rows("golden_publish")
                        if p.get("版本标识") == target), None)
        if publish is None:
            entry["status"] = "回滚失败"
            entry["abnormal"] = True
            return {"ok": False, "message": f"目标版本 {target} 不存在"}
        publish["status"] = "已回滚"
        publish["abnormal"] = True
        entry["status"] = "已回滚"
        entry["pending"] = False
        return {"ok": True, "target_version": target}

    def notify_subscribers(self, entry_id: int) -> list[str]:
        """回滚后通知受影响订阅方，返回通知名单。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已回滚":
            return []
        target = str(entry.get("目标版本", ""))
        notified: list[str] = []
        for task in store.rows("dist_task"):
            if str(task.get("记录范围", "")).endswith(target):
                subscriber = task.get("订阅方", "")
                if subscriber and subscriber not in notified:
                    notified.append(subscriber)
                task["status"] = "分发失败"
                task["abnormal"] = True
        entry["影响订阅方"] = ",".join(notified)
        return notified

    def consume_publish_history(self, entry_id: int) -> list[dict[str, Any]]:
        """读取发布历史，选择可回滚版本。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        return [p for p in store.rows("golden_publish")
                if p.get("status") == "已发布"]

    def estimate_impact(self, entry_id: int) -> dict[str, Any]:
        """估算回滚影响面：受影响订阅方与分发批次数。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {}
        target = str(entry.get("目标版本", ""))
        affected = [t for t in store.rows("dist_task")
                    if str(t.get("记录范围", "")).endswith(target)]
        subscribers = {t.get("订阅方", "") for t in affected}
        return {"affected_batches": len(affected), "affected_subscribers": len(subscribers)}

    def cancel_task(self, entry_id: int, reason: str) -> str:
        """取消回滚任务并恢复发布版本状态。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return "回滚任务不存在"
        entry["status"] = "回滚失败"
        entry["取消原因"] = reason
        for p in store.rows("golden_publish"):
            if p.get("版本标识") == entry.get("目标版本") and p.get("status") == "已回滚":
                p["status"] = "已发布"
                p["abnormal"] = False
        return "回滚任务已取消，发布版本已恢复"

    def record_rollback_receipt(self, entry_id: int) -> int:
        """登记回执中心的回滚知回执。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return 0
        receipt = registry.register_receipt(
            number_style="rollback",
            subscriber=str(entry.get("影响订阅方", "") or ""),
            status="已回收",
            diff=f"回滚到 {entry.get('目标版本', '')}",
            extra={"pending": False, "abnormal": False},
        )
        return int(receipt["id"])
