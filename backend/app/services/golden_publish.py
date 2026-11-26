"""金记录发布领域服务：管理金记录版本的发布批次、版本标识、扇出分发与回滚衔接，完成入库动作。

数据链路位置：主数据发布层，负责消费合并产物，把发布版本
扇出给订阅方并衔接版本回滚任务。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.dispatch_registry import registry
from app.store import store


class GoldenPublishService(BaseModuleService):
    MODULE = "golden_publish"
    ENTITY = "发布批次"
    OPERATION_TERM = "入库"
    LIST_FIELDS = ["发布批次", "实体编码", "金记录数", "发布时间", "版本标识"]
    REQUIRED_FIELDS = ["发布批次", "实体编码", "金记录数"]
    STATUS_ORDER = ["待发布", "已发布", "已回滚"]
    ACTION_RULES = {"发布批次": "已发布", "回滚批次": "已回滚"}
    NEGATIVE_ACTIONS = ["回滚批次"]

    # ---- 业务扩展：金记录发布与分发衔接 ----

    def run_action(self, entry_id: int, action: str, *_args: Any, **kwargs: Any) -> tuple[dict[str, Any] | None, str]:
        """兼容通用 /actions 入口中的整组派送/续办语义动作。"""
        fanout_actions = {
            "整组派送", "整组续办", "续办整组", "继续整组", "续办并核对整组",
            "核对整组", "fanout", "fanout_to_subscribers", "resume_fanout",
        }
        if action in fanout_actions:
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            if entry.get("status") != "已发布":
                return None, "发布批次不在已发布状态，不能整组派送"
            state = self.submit_fanout(entry_id)
            entry["最近整组回执"] = state
            return entry, str(state.get("message") or "整组派送已处理")
        return super().run_action(entry_id, action, **kwargs)

    def release_version(self, entry_id: int) -> dict[str, Any]:
        """发布金记录版本，写发布时间与版本标识。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "待发布":
            return {"ok": False, "message": "发布批次不在待发布状态"}
        now = datetime.now(timezone.utc).isoformat()
        version = f"pub-v{entry_id}-{now[:10]}"
        entry["发布时间"] = now
        entry["版本标识"] = version
        entry["status"] = "已发布"
        entry["pending"] = False
        return {"ok": True, "version": version, "released_at": now}

    def fanout_to_subscribers(self, entry_id: int) -> list[int]:
        """把已发布版本扇出给全部生效订阅方，生成分发批次。

        返回结构保持为分发任务自增 ID 列表；申请、幂等和编号争用由统一
        登记处收拢。重复调用会续办同一整组，已登记行保留原分发号。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已发布":
            return []
        state = self.submit_fanout(entry_id)
        return list(state.get("task_ids", []))

    def prepare_fanout_group(self, entry_id: int) -> dict[str, Any] | None:
        """按当前生效订阅方确定整组 A/B/C 申请，已存在的整组保留原顺序。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已发布":
            return None

        group_id = f"FG-P{entry_id}"
        group_key = f"golden-fanout-publish-{entry_id}"
        version = str(entry.get("版本标识", "") or "")
        active_subscribers = [
            sub for sub in store.rows("subscriber_mgr")
            if sub.get("status") == "已生效"
        ]

        with registry.lock:
            # 整组一旦受理就固定为当时选出的目标和次序；续办不能把中断后
            # 新出现的订阅方插进“A、B、C”的原位次。
            existing_state = registry.group_state(group_id)
            if existing_state is not None:
                return existing_state

            for position, sub in enumerate(active_subscribers, start=1):
                subscriber = str(sub.get("订阅方标识", "") or "")
                key = registry.make_idempotency_key(
                    publish_entry_id=entry_id,
                    version=version,
                    subscriber=subscriber,
                )
                app = registry.prepare_application(
                    source="golden_publish.fanout_to_subscribers",
                    subscriber=subscriber,
                    version=version,
                    publish_entry_id=entry_id,
                    publish_batch=str(entry.get("发布批次", "") or ""),
                    subscriber_entry_id=int(sub.get("id", 0) or 0) or None,
                    number_style="fanout",
                    group_id=group_id,
                    group_key=group_key,
                    position=position,
                    request_key=key,
                )
                # 若订阅方单笔入口已经先确认了同一发布版本，把原申请并入
                # 整组视图，但不改写它已经使用的另一种历史编号格式。
                if not app.get("整组编号"):
                    registry._mark_app(  # noqa: SLF001 - 同域共享登记处状态
                        app,
                        整组编号=group_id,
                        整组键=group_key,
                        顺序=position,
                    )
            state = registry.group_state(group_id)
            if state is None:
                state = {
                    "ok": True,
                    "group_id": group_id,
                    "整组编号": group_id,
                    "total": 0,
                    "registered_count": 0,
                    "pending_count": 0,
                    "interrupted": False,
                    "completed": True,
                    "application_ids": [],
                    "task_ids": [],
                    "batch_nos": [],
                    "items": [],
                    "applications": [],
                }
        return state

    def submit_fanout(self, entry_id: int) -> dict[str, Any]:
        """提交/续办整组派送，返回整组核对状态和每个申请的回执。"""
        state = self.prepare_fanout_group(entry_id)
        if state is None:
            return {
                "ok": False,
                "task_ids": [],
                "message": "发布批次不存在或不是已发布状态",
            }

        results: list[dict[str, Any]] = []
        for app_id in state.get("application_ids", []):
            result = registry.resume_application(app_id)
            results.append(result)
            # 注入式中断会从登记钩子抛出，保持“在某一位中断”的原始语义。
            # 普通业务冲突以结果字典返回，由整组状态展示失败/待办原因。

        latest = registry.group_state(str(state["group_id"])) or state
        latest["results"] = results
        latest["ok"] = all(item.get("ok") for item in results)
        latest["message"] = "整组派送已登记" if latest.get("completed") else "整组存在未完成条目"
        return latest

    # 语义化别名：发布页“续办整组/核对整组”可直接调用这些方法。
    resume_fanout = submit_fanout
    continue_fanout = submit_fanout
    resume_group = submit_fanout
    continue_group = submit_fanout

    def get_fanout_status(self, entry_id: int) -> dict[str, Any] | None:
        group_id = f"FG-P{entry_id}"
        state = registry.group_state(group_id)
        if state is None:
            prepared = self.prepare_fanout_group(entry_id)
            return prepared
        return state

    check_fanout = get_fanout_status
    reconcile_fanout = get_fanout_status
    get_group_status = get_fanout_status
    check_group = get_fanout_status
    reconcile_group = get_fanout_status

    def inspect_dispatch_conflicts(self, entry_id: int) -> dict[str, Any]:
        """核对整组中每位的原申请、实际登记行和可能的另一张确认单。"""
        state = self.get_fanout_status(entry_id)
        if state is None:
            return {"ok": False, "message": "发布批次不存在或不是已发布状态"}
        inspections = [
            registry.inspect_application(app_id)
            for app_id in state.get("application_ids", [])
        ]
        related = [
            registry.describe_task(task)
            for task in registry.tasks()
            if task.get("发布条目ID") == entry_id
        ]
        return {
            **state,
            "ok": True,
            "inspections": inspections,
            "confirmations": related,
        }

    def rollback_version(self, entry_id: int, reason: str) -> dict[str, Any]:
        """回滚发布版本并触发版本回滚任务。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已发布":
            return {"ok": False, "message": "批次未发布，无法回滚"}
        entry["status"] = "已回滚"
        entry["abnormal"] = True
        rollbacks = store.rows("version_rollback")
        rb_id = max((int(r.get("id", 0)) for r in rollbacks), default=0) + 1
        rollbacks.append({
            "id": rb_id,
            "回滚任务": f"rb-{entry_id}-{rb_id}",
            "目标版本": entry.get("版本标识", ""),
            "回滚原因": reason,
            "执行状态": "待执行",
            "影响订阅方": "",
            "status": "待执行",
            "pending": True,
            "abnormal": True,
        })
        return {"ok": True, "rollback_id": rb_id, "message": "回滚任务已创建"}

    def consume_merge_output(self, entry_id: int) -> list[dict[str, Any]]:
        """读取合并模块产出的金记录，作为发布输入。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        return [m for m in store.rows("golden_merge")
                if m.get("实体编码") == entry.get("实体编码") and m.get("status") == "已合并"]

    def emit_stats_snapshot(self, entry_id: int) -> int:
        """发布完成后向治理看板输黄金记录数快照。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return 0
        stats = store.rows("stats_overview")
        stat_id = max((int(r.get("id", 0)) for r in stats), default=0) + 1
        stats.append({
            "id": stat_id,
            "统计时间": datetime.now(timezone.utc).isoformat(),
            "实体编码": entry.get("实体编码", ""),
            "金记录数": str(entry.get("金记录数", "0")),
            "重复率": "0%",
            "分发延迟": "0",
            "status": "正常",
            "pending": False,
            "abnormal": False,
        })
        return stat_id

    def register_publish_lineage(self, entry_id: int) -> str | None:
        """登记发布批次到分发模块的血缘边。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        edges = store.rows("lineage_graph")
        edge_id = max((int(r.get("id", 0)) for r in edges), default=0) + 1
        edges.append({
            "id": edge_id,
            "血缘边标识": f"edge-pub-{edge_id}",
            "上游节点": entry.get("发布批次", ""),
            "下游节点": "dist_task",
            "变换规则": "版本分发",
            "登记时间": datetime.now(timezone.utc).isoformat(),
            "status": "已登记",
            "pending": False,
            "abnormal": False,
        })
        return f"edge-pub-{edge_id}"
