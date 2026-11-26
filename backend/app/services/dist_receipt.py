"""分发回执中心领域服务：管理订阅方分发回执的回收核对、差异标记与重发请求。

数据链路位置：分发层对账端，负责消费分发批次的送达结果，
把回执差异回写给分发任务与订阅方。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.dispatch_registry import registry
from app.store import store


class DistReceiptService(BaseModuleService):
    MODULE = "dist_receipt"
    ENTITY = "回执记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["回执编号", "分发批次", "订阅方", "回执状态", "差异说明"]
    REQUIRED_FIELDS = ["回执编号", "分发批次", "订阅方"]
    STATUS_ORDER = ["已回收", "部分回收", "未回收"]
    ACTION_RULES = {"重发回执请求": "部分回收", "标记差异": "未回收"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：回执核对与差异处置 ----

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """回执通用登记也纳入同一编号锁，避免和签收回执争用编号。"""
        with registry.lock:
            return super().create_entry(values)

    def verify_receipt(self, entry_id: int) -> dict[str, Any]:
        """核对回执：按分发批次校验记录数一致性。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回执记录不存在"}
        batch = entry.get("分发批次", "")
        task = registry.find_task_by_batch(str(batch))
        if task is None:
            entry["status"] = "未回收"
            entry["差异说明"] = "分发批次不存在"
            return {"ok": False, "message": f"分发批次 {batch} 不存在"}
        if task.get("status") == "已送达":
            entry["status"] = "已回收"
            entry["pending"] = False
            return {"ok": True, "message": "回执与分发结果一致"}
        entry["status"] = "部分回收"
        entry["差异说明"] = f"分发状态为 {task.get('status')}"
        return {"ok": False, "message": "回执与分发结果不一致"}

    def verify_by_batch(
        self,
        batch_no: str,
        *,
        subscriber: str = "",
        version: str = "",
        credential: str = "",
    ) -> dict[str, Any]:
        """回执中心按旧分发批次号核对，不依赖发布页或订阅方页上下文。"""
        claimed = registry.claim_batch(
            batch_no=batch_no,
            credential=credential,
            subscriber=subscriber,
            version=version,
        )
        if not claimed.get("ok"):
            return claimed

        task = claimed["task"]
        receipt = next((
            r for r in store.rows(self.MODULE)
            if r.get("分发批次") == batch_no
        ), None)
        if receipt is not None:
            result = self.verify_receipt(int(receipt["id"]))
            result["receipt_id"] = receipt.get("id")
            result["回执编号"] = receipt.get("回执编号", "")
        else:
            result = {
                "ok": task.get("status") == "已送达",
                "message": "尚未登记回执" if task.get("status") != "已送达" else "批次已送达但缺少回执",
                "batch_no": batch_no,
                "分发批次": batch_no,
            }
        # 新查证入口在独立返回中明确带回本行的原发放版本和原始订阅方；
        # 旧 verify_receipt(entry_id) 的返回结构保持原样。
        result.update(registry.describe_task(task) or {})
        result.setdefault("batch_no", batch_no)
        result["分发批次"] = batch_no
        return result

    def sign_and_verify_by_batch(self, batch_no: str) -> dict[str, Any]:
        """按旧批次完成原签收，再回到回执中心核对。"""
        from app.services.dist_task import DistTaskService

        sign_result = DistTaskService().sign_by_batch(batch_no)
        verify_result = self.verify_by_batch(batch_no)
        return {
            "ok": bool(sign_result.get("ok")) and bool(verify_result.get("ok")),
            "sign": sign_result,
            "verify": verify_result,
            **verify_result,
        }

    submit_receipt = verify_by_batch
    handle_by_old_number = sign_and_verify_by_batch
    verify_by_batch_no = verify_by_batch
    check_by_batch = verify_by_batch
    verify_old_number = verify_by_batch

    def mark_diff(self, entry_id: int, diff: str) -> str:
        """标记回执差异并把分发任务置为失败重试。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return "回执记录不存在"
        entry["status"] = "未回收"
        entry["差异说明"] = diff
        entry["abnormal"] = True
        batch = entry.get("分发批次", "")
        for task in store.rows("dist_task"):
            if task.get("分发批次") == batch:
                task["status"] = "分发失败"
                task["abnormal"] = True
        return f"差异已标记: {diff}"

    def resend_request(self, entry_id: int) -> dict[str, Any]:
        """重发回执请求并等待订阅方回传。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回执记录不存在"}
        entry["status"] = "部分回收"
        entry["pending"] = True
        entry["重发时间"] = datetime.now(timezone.utc).isoformat()
        return {"ok": True, "message": "回执请求已重发"}

    def consume_dist_results(self, entry_id: int) -> list[dict[str, Any]]:
        """读取分发模块的送达批次，作为核对输入。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        subscriber = entry.get("订阅方", "")
        return [t for t in store.rows("dist_task")
                if t.get("订阅方") == subscriber and t.get("status") in ("已送达", "分发失败")]

    def stats_by_subscriber(self) -> dict[str, dict[str, int]]:
        """按订阅方统计回执回收情况，供对账看板消费。"""
        stats: dict[str, dict[str, int]] = {}
        for row in store.rows(self.MODULE):
            subscriber = row.get("订阅方", "未知")
            bucket = stats.setdefault(subscriber, {"已回收": 0, "部分回收": 0, "未回收": 0})
            status = row.get("status", "未回收")
            if status in bucket:
                bucket[status] += 1
        return stats

    def notify_receipt_gap(self, entry_id: int) -> str:
        """把回执缺口通知订阅方重新上传。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return "回执记录不存在"
        subscriber = entry.get("订阅方", "")
        for sub in store.rows("subscriber_mgr"):
            if sub.get("订阅方标识") == subscriber:
                sub["pending"] = True
        return f"已通知 {subscriber} 补传回执"
