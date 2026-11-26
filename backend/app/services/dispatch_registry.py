"""分发批次统一登记处。

金记录发布页的整组扇出与订阅方页的单笔安排都经过这里：

* 同一把进程内可重入锁保护编号分配、幂等检查和写入；
* 申请（application）与真正写入 ``dist_task`` 的登记行分开保存；
* 两种历史批次号（``dist-{发布条目ID}-{任务ID}`` 与
  ``dist-{订阅方标识}-{任务ID}``）原样保留；
* 写入结果不确定时，可用登记凭据或业务幂等键核对晚到的真实行，
  查无此行时再收回旧资格、领用新资格补登记。

当前示例工程使用内存仓库，因此锁只保证单进程/线程模型下的原子性。
"""
from __future__ import annotations

import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from app.store import store

# 同一份申请对象同时挂到多个语义化表名下，便于不同入口/测试按习惯查证；
# 因为是同一个 dict 对象，状态更新不会出现两份不一致数据。
APPLICATION_TABLES = (
    "dispatch_application",
    "dispatch_request",
    "dispatch_arrangement",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _max_id(rows: list[dict[str, Any]]) -> int:
    result = 0
    for row in rows:
        try:
            result = max(result, int(row.get("id", 0) or 0))
        except (TypeError, ValueError):
            continue
    return result


class DispatchRegistry:
    """分发申请、资格、批次行和回执的统一写入/查证对象。"""

    def __init__(self, store_like: Any | None = None) -> None:
        # 允许测试注入自己的 store；默认仍使用进程内全局单例。
        self.store = store_like or store
        self.lock = threading.RLock()
        # 测试或故障演练可替换这两个钩子。生产路径默认无钩子。
        self.before_commit: Callable[[dict[str, Any]], None] | None = None
        self.after_row_appended: Callable[[dict[str, Any]], None] | None = None
        self._table_signatures: dict[str, tuple[int, int]] = {}

    def _sync_store_reset(self) -> None:
        """识别测试/调用方重建已知表的动作，避免内部申请状态跨夹具污染。

        正常业务只追加或更新行，最大 ID 不会下降；若表对象被替换或最大 ID
        回退，就把本登记处的内部申请/凭据表一并清空。
        """
        changed = False
        for table in ("dist_task", "dist_receipt", "golden_publish", "subscriber_mgr"):
            rows = self.store.rows(table)
            signature = (id(rows), _max_id(rows))
            previous = self._table_signatures.get(table)
            if previous is not None and (
                previous[0] != signature[0] or signature[1] < previous[1]
            ):
                changed = True
            self._table_signatures[table] = signature
        if changed:
            for table in APPLICATION_TABLES:
                self.store.rows(table).clear()

    # ---------- 基础读取 ----------

    def tasks(self) -> list[dict[str, Any]]:
        with self.lock:
            self._sync_store_reset()
        return self.store.rows("dist_task")

    def receipts(self) -> list[dict[str, Any]]:
        return self.store.rows("dist_receipt")

    def applications(self) -> list[dict[str, Any]]:
        with self.lock:
            self._sync_store_reset()
        return self.store.rows(APPLICATION_TABLES[0])

    def _put_application(self, row: dict[str, Any]) -> dict[str, Any]:
        for table in APPLICATION_TABLES:
            self.store.rows(table).append(row)
        return row

    def next_application_id(self) -> int:
        with self.lock:
            used = [_max_id(self.applications())]
            for table in APPLICATION_TABLES[1:]:
                used.append(_max_id(self.store.rows(table)))
            return max(used, default=0) + 1

    def next_task_id(self) -> int:
        """返回下一个分发任务 ID。

        锁内“计算并追加”保证并发入口不撞号；已发出但未确认/已收回资格的
        候选 ID 也计入最大值，保证中断条目的旧号不会在补登记时被重复使用。
        若测试/夹具重建已知表，重置守卫会先清空这些内部申请。
        """
        with self.lock:
            candidate = _max_id(self.tasks())
            for app in self.applications():
                for key in ("planned_task_id", "task_id", "分发任务ID"):
                    try:
                        candidate = max(candidate, int(app.get(key, 0) or 0))
                    except (TypeError, ValueError):
                        continue
                for claim in app.get("资格历史", []) or []:
                    try:
                        candidate = max(candidate, int(claim.get("planned_task_id", 0) or claim.get("task_id", 0) or 0))
                    except (TypeError, ValueError):
                        continue
            return candidate + 1

    def next_receipt_id(self) -> int:
        with self.lock:
            return _max_id(self.receipts()) + 1

    # ---------- 编号与凭据 ----------

    @staticmethod
    def new_credential() -> str:
        return f"reg-{uuid.uuid4().hex}"

    @staticmethod
    def make_idempotency_key(
        *,
        publish_entry_id: int | str | None = None,
        version: str = "",
        subscriber: str = "",
    ) -> str:
        subscriber_part = str(subscriber or "").strip()
        if publish_entry_id not in (None, ""):
            return f"publish-entry:{publish_entry_id}|subscriber:{subscriber_part}"
        return f"version:{version or ''}|subscriber:{subscriber_part}"

    def make_batch_no(
        self,
        task_id: int,
        *,
        number_style: str,
        publish_entry_id: int | str | None = None,
        subscriber: str = "",
    ) -> str:
        # 两种历史编号格式必须原样保留。
        if number_style == "subscriber":
            return f"dist-{subscriber}-{task_id}"
        return f"dist-{publish_entry_id}-{task_id}"

    @staticmethod
    def version_from_task(task: dict[str, Any]) -> str:
        """只从当前这一行取版本，绝不从另一批行补齐。"""
        explicit = str(task.get("发布版本", "") or "").strip()
        if explicit:
            return explicit
        scope = str(task.get("记录范围", "") or "")
        prefix = "版本 "
        return scope[len(prefix):] if scope.startswith(prefix) else scope

    def describe_task(self, task: dict[str, Any] | None) -> dict[str, Any] | None:
        if task is None:
            return None
        version = self.version_from_task(task)
        subscriber = str(task.get("订阅方", "") or "")
        batch_no = str(task.get("分发批次", "") or "")
        status = str(task.get("status", "") or task.get("分发状态", "") or "")
        return {
            "ok": True,
            "task_id": task.get("id"),
            "id": task.get("id"),
            "batch_no": batch_no,
            "分发批次": batch_no,
            "subscriber": subscriber,
            "订阅方": subscriber,
            "原始订阅方": subscriber,
            "version": version,
            "publish_version": version,
            "发布版本": version,
            "原发放版本": version,
            "publish_entry_id": task.get("发布条目ID"),
            "status": status,
            "分发状态": task.get("分发状态", status),
            "回执状态": task.get("回执状态", ""),
            "credential": task.get("登记凭据", ""),
            "登记凭据": task.get("登记凭据", ""),
            "task": task,
            "entry": task,
        }

    def find_task_by_batch(self, batch_no: str) -> dict[str, Any] | None:
        batch_no = str(batch_no or "").strip()
        if not batch_no:
            return None
        return next(
            (task for task in self.tasks() if str(task.get("分发批次", "")) == batch_no),
            None,
        )

    def find_task_by_credential(self, credential: str) -> dict[str, Any] | None:
        credential = str(credential or "").strip()
        if not credential:
            return None
        return next(
            (task for task in self.tasks() if str(task.get("登记凭据", "")) == credential),
            None,
        )

    def find_canonical_task(
        self,
        *,
        idempotency_key: str = "",
        credential: str = "",
        batch_no: str = "",
        subscriber: str = "",
        version: str = "",
    ) -> dict[str, Any] | None:
        """按强标识查找真实登记行；所有回退条件都必须精确匹配本行字段。"""
        task = self.find_task_by_credential(credential)
        if task is not None:
            return task
        task = self.find_task_by_batch(batch_no)
        if task is not None:
            return task

        subscriber = str(subscriber or "")
        version = str(version or "")
        for row in self.tasks():
            row_key = row.get("幂等键")
            if idempotency_key:
                if row_key == idempotency_key:
                    return row
                if row_key:
                    # 另一发布/订阅申请写下的行不能靠版本或编号近似补进来。
                    continue
            # 兼容没有内部幂等键的旧/手工记录：订阅方和版本范围都精确相等。
            if (
                subscriber
                and str(row.get("订阅方", "")) == subscriber
                and version
                and self.version_from_task(row) == version
            ):
                return row
        return None

    def claim_batch(
        self,
        batch_no: str = "",
        *,
        ref: str = "",
        credential: str = "",
        subscriber: str = "",
        version: str = "",
    ) -> dict[str, Any]:
        """按旧批次号或登记凭据打开/领取登记行。

        新登记行若带凭据，则批次号+凭据必须匹配；旧行没有凭据时，持有旧
        批次号即可按旧流程读取。任何查无此行的申请编号都不会被映射到另一行。
        """
        with self.lock:
            app = None
            lookup_ref = str(ref or "").strip()
            if lookup_ref and not batch_no:
                maybe_id = _maybe_int(lookup_ref)
                app = self.find_application(
                    maybe_id,
                    credential=lookup_ref if lookup_ref.startswith("reg-") else "",
                )
                if app is not None:
                    batch_no = str(app.get("分发批次") or app.get("planned_batch_no") or "")
                    credential = credential or str(app.get("登记凭据") or "")
                else:
                    # ref 可以直接是旧分发批次号，也可以是分发任务数字 ID；
                    # 不能把数字 ID 误当成申请 ID 后映射到另一行。
                    batch_no = lookup_ref
                    if maybe_id is not None:
                        task_by_id = self.store.find("dist_task", maybe_id)
                        if task_by_id is not None:
                            batch_no = str(task_by_id.get("分发批次", "") or batch_no)
            if credential and not batch_no:
                task = self.find_task_by_credential(credential)
                if task is not None:
                    batch_no = str(task.get("分发批次", ""))

            task = None
            supplied_credential = str(credential or "")
            if supplied_credential:
                task = self.find_task_by_credential(supplied_credential)
                if task is not None and batch_no and str(task.get("分发批次", "")) != str(batch_no):
                    return {
                        "ok": False,
                        "registered": False,
                        "code": "CREDENTIAL_BATCH_MISMATCH",
                        "message": "登记凭据不属于该分发批次，不能按别人的编号领取",
                        "batch_no": batch_no,
                    }
            if task is None:
                task = self.find_task_by_batch(batch_no)

            if task is None and supplied_credential:
                task = self.find_task_by_credential(supplied_credential)

            if task is None:
                return {
                    "ok": False,
                    "registered": False,
                    "code": "NOT_REGISTERED",
                    "message": "该编号没有实际登记的分发批次，不能领取或签收",
                    "batch_no": batch_no,
                    "ref": ref,
                    "application": app,
                }

            row_credential = str(task.get("登记凭据", "") or "")
            supplied_credential = str(credential or "")
            if row_credential and supplied_credential and row_credential != supplied_credential:
                return {
                    "ok": False,
                    "registered": False,
                    "code": "CREDENTIAL_MISMATCH",
                    "message": "登记凭据与分发批次不一致，不能按别人的编号领取",
                    "batch_no": batch_no,
                }
            if subscriber and str(task.get("订阅方", "")) != str(subscriber):
                return {
                    "ok": False,
                    "registered": False,
                    "code": "SUBSCRIBER_MISMATCH",
                    "message": "批次的原始收件方与请求不一致",
                    "batch_no": batch_no,
                }
            if version and self.version_from_task(task) != str(version):
                return {
                    "ok": False,
                    "registered": False,
                    "code": "VERSION_MISMATCH",
                    "message": "批次的原发放版本与请求不一致",
                    "batch_no": batch_no,
                }

            descriptor = self.describe_task(task)
            assert descriptor is not None
            descriptor.update({
                "registered": True,
                "claimed": True,
                "opened_by_credential": bool(app is not None or supplied_credential),
                "application": app,
            })
            return descriptor

    # 常见语义别名：两个页面“共用同一登记凭据打开方式”。
    open_registration = claim_batch
    open_credential = claim_batch

    # ---------- 申请/资格 ----------

    def find_application(
        self,
        app_id: int | str | None = None,
        *,
        credential: str = "",
        request_key: str = "",
        group_id: str = "",
        position: int | None = None,
    ) -> dict[str, Any] | None:
        rows = self.applications()
        if app_id not in (None, ""):
            try:
                wanted = int(app_id)
            except (TypeError, ValueError):
                wanted = None
            if wanted is not None:
                row = next((r for r in rows if int(r.get("id", 0) or 0) == wanted), None)
                if row is not None:
                    return row
        credential = str(credential or "").strip()
        if credential:
            row = next((r for r in rows if credential in {
                str(r.get("申请凭据", "")),
                str(r.get("原始申请凭据", "")),
                str(r.get("登记凭据", "")),
                str(r.get("资格凭据", "")),
                str(r.get("credential", "")),
            }), None)
            if row is not None:
                return row
        if request_key:
            row = next((r for r in rows if r.get("申请键") == request_key), None)
            if row is not None:
                return row
        if group_id and position is not None:
            return next((
                r for r in rows
                if r.get("整组编号") == group_id and int(r.get("顺序", -1)) == int(position)
            ), None)
        if group_id:
            return next((r for r in rows if r.get("整组编号") == group_id), None)
        return None

    # 常见读取命名别名。
    get_application = find_application

    def prepare_application(
        self,
        *,
        source: str,
        subscriber: str,
        version: str = "",
        publish_entry_id: int | None = None,
        publish_batch: str = "",
        subscriber_entry_id: int | None = None,
        number_style: str = "fanout",
        group_id: str = "",
        group_key: str = "",
        position: int | None = None,
        request_key: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self.lock:
            key = request_key or self.make_idempotency_key(
                publish_entry_id=publish_entry_id,
                version=version,
                subscriber=subscriber,
            )
            existing = self.find_application(request_key=key)
            if existing is not None and str(existing.get("整组编号", "") or "") != str(group_id or ""):
                # 同一发放键可能同时有整组申请和单笔申请；重试必须找到
                # 属于当前入口/整组的那一份，而不是每次新建单笔申请。
                existing = next(
                    (
                        app for app in self.applications()
                        if app.get("申请键") == key
                        and str(app.get("整组编号", "") or "") == str(group_id or "")
                    ),
                    None,
                )
            if existing is not None:
                # 同一入口/同一整组的重试复用原申请；整组条目与订阅方单笔
                # 安排即使指向同一发放键，也保留各自的原申请和编号风格，
                # 之后在真正写 dist_task 时收敛为最多一笔有效发放。
                return existing

            app_id = self.next_application_id()
            task_id = self.next_task_id()
            credential = self.new_credential()
            batch_no = self.make_batch_no(
                task_id,
                number_style=number_style,
                publish_entry_id=publish_entry_id,
                subscriber=subscriber,
            )
            row: dict[str, Any] = {
                "id": app_id,
                "申请编号": f"apply-{app_id}",
                "来源入口": source,
                "申请键": key,
                "整组编号": group_id,
                "整组键": group_key,
                "顺序": position,
                "发布条目ID": publish_entry_id,
                "发布批次": publish_batch,
                "发布版本": version,
                "订阅条目ID": subscriber_entry_id,
                "订阅方": subscriber,
                "number_style": number_style,
                "planned_task_id": task_id,
                "planned_batch_no": batch_no,
                "分发任务ID": None,
                "分发批次": "",
                "申请凭据": credential,
                "原始申请凭据": credential,
                "原始计划批次": batch_no,
                "登记凭据": credential,
                "资格凭据": credential,
                "资格版本": 1,
                "资格状态": "有效",
                "资格历史": [],
                "处理状态": "待登记",
                "status": "待登记",
                "pending": True,
                "abnormal": False,
                "原因": "申请已受理，等待写入分发批次",
                "待办原因": "等待登记",
                "冲突处理": "",
                "created_at": utc_now(),
                "updated_at": utc_now(),
            }
            if extra:
                row.update(extra)
            return self._put_application(row)

    def _mark_app(self, app: dict[str, Any], **changes: Any) -> dict[str, Any]:
        app.update(changes)
        app["updated_at"] = utc_now()
        return app

    def register_application(self, app_id: int | str) -> dict[str, Any]:
        """真正写入分发批次；已登记/已确认的申请幂等返回原行。"""
        with self.lock:
            app = self.find_application(app_id)
            if app is None:
                return {"ok": False, "message": "分发申请不存在"}

            existing_task = None
            if app.get("分发任务ID"):
                candidate = self.store.find("dist_task", int(app["分发任务ID"]))
                expected_batch = str(app.get("分发批次", "") or "")
                if (
                    candidate is not None
                    and (not expected_batch or str(candidate.get("分发批次", "")) == expected_batch)
                ):
                    existing_task = candidate
            if existing_task is None and app.get("分发批次"):
                existing_task = self.find_task_by_batch(str(app["分发批次"]))
            if existing_task is None:
                existing_task = self.find_canonical_task(
                    idempotency_key=str(app.get("申请键", "")),
                    credential=str(app.get("登记凭据", "")),
                    batch_no=str(app.get("planned_batch_no", "")),
                    subscriber=str(app.get("订阅方", "")),
                    version=str(app.get("发布版本", "")),
                )
            if existing_task is not None:
                return self._adopt_task(app, existing_task, handling="already_registered")

            # 同键的并发申请可能已经提交。锁内再次检查，避免双写。
            canonical = self.find_canonical_task(
                idempotency_key=str(app.get("申请键", "")),
                subscriber=str(app.get("订阅方", "")),
                version=str(app.get("发布版本", "")),
            )
            if canonical is not None:
                return self._adopt_task(app, canonical, handling="accepted_confirmed_other")

            task_id = int(app.get("planned_task_id") or self.next_task_id())
            if self.store.find("dist_task", task_id) is not None:
                # 编号被非统一入口占用时，不覆盖别人的行，重新安全领用编号。
                task_id = self.next_task_id()
            batch_no = self.make_batch_no(
                task_id,
                number_style=str(app.get("number_style", "fanout")),
                publish_entry_id=app.get("发布条目ID"),
                subscriber=str(app.get("订阅方", "")),
            )
            credential = str(app.get("登记凭据") or self.new_credential())
            version = str(app.get("发布版本", "") or "")
            subscriber = str(app.get("订阅方", "") or "")

            self._mark_app(
                app,
                处理状态="登记中",
                status="登记中",
                原因="分发批次写入中，提交结果未确认",
                待办原因="核对写入结果",
            )

        try:
            if self.before_commit is not None:
                self.before_commit(app)

            with self.lock:
                # 获得锁后第三次检查，防止钩子等待期间另一入口已经写入。
                canonical = self.find_canonical_task(
                    idempotency_key=str(app.get("申请键", "")),
                    subscriber=subscriber,
                    version=version,
                )
                if canonical is not None:
                    return self._adopt_task(app, canonical, handling="accepted_confirmed_other")
                if self.store.find("dist_task", task_id) is not None:
                    task_id = self.next_task_id()
                    batch_no = self.make_batch_no(
                        task_id,
                        number_style=str(app.get("number_style", "fanout")),
                        publish_entry_id=app.get("发布条目ID"),
                        subscriber=subscriber,
                    )

                task: dict[str, Any] = {
                    "id": task_id,
                    "分发批次": batch_no,
                    "订阅方": subscriber,
                    "记录范围": f"版本 {version}",
                    "分发状态": "排队中",
                    "回执状态": "未回收",
                    "status": "排队中",
                    "pending": True,
                    "abnormal": False,
                    # 以下为本行自有的查证元数据，不用来替代五个旧字段。
                    "幂等键": app.get("申请键", ""),
                    "来源入口": app.get("来源入口", ""),
                    "发布条目ID": app.get("发布条目ID"),
                    "发布版本": version,
                    "订阅条目ID": app.get("订阅条目ID"),
                    "申请编号": app.get("申请编号", ""),
                    "整组编号": app.get("整组编号", ""),
                    "顺序": app.get("顺序"),
                    "登记凭据": credential,
                    "登记确认状态": "已确认",
                }
                self.tasks().append(task)
        except Exception as exc:
            with self.lock:
                self._mark_app(
                    app,
                    处理状态="登记失败",
                    status="登记失败",
                    abnormal=True,
                    原因=f"写入分发批次失败：{type(exc).__name__}: {exc}",
                    失败原因=str(exc),
                    待办原因="可核对晚到登记，或收回原资格后补登记",
                )
            raise

        # 模拟“行已写入，但成功回执在途中丢失/进程在确认申请前中断”。
        if self.after_row_appended is not None:
            self.after_row_appended({"application": app, "task": task})

        with self.lock:
            result = self._adopt_task(app, task, handling="registered")
            result["created"] = True
            return result

    def _adopt_task(
        self,
        app: dict[str, Any],
        task: dict[str, Any],
        *,
        handling: str,
    ) -> dict[str, Any]:
        batch_no = str(task.get("分发批次", ""))
        credential = str(task.get("登记凭据", "") or app.get("登记凭据", ""))
        mismatch = (
            str(task.get("订阅方", "")) != str(app.get("订阅方", ""))
            or (
                str(app.get("发布版本", "") or "")
                and self.version_from_task(task) != str(app.get("发布版本", ""))
            )
        )
        if mismatch:
            self._mark_app(
                app,
                处理状态="冲突",
                status="冲突",
                abnormal=True,
                原因="查到的批次行与原申请的收件方或版本不一致，未采用",
                冲突处理="rejected_mismatch",
            )
            return {
                "ok": False,
                "conflict": True,
                "message": "分发批次与原申请不一致",
                "application": app,
                **(self.describe_task(task) or {}),
            }

        previous_credential = str(app.get("登记凭据", "") or "")
        task_credential = str(task.get("登记凭据", "") or "")
        if task_credential and previous_credential and task_credential != previous_credential:
            handling = "accepted_confirmed_other"
            conflict_note = "同一发布版本和订阅方已有确认单，按原批次领取，不重复发放"
        elif handling == "late_original":
            conflict_note = "已核对到晚到回执对应的原登记，保留原分发号"
        else:
            conflict_note = ""

        self._mark_app(
            app,
            分发任务ID=task.get("id"),
            分发批次=batch_no,
            planned_task_id=task.get("id"),
            planned_batch_no=batch_no,
            登记凭据=credential,
            处理状态="已登记",
            status="已登记",
            pending=False,
            abnormal=False,
            原因=conflict_note or "分发批次已登记",
            待办原因="",
            冲突处理=handling,
            registered_at=app.get("registered_at") or utc_now(),
        )
        descriptor = self.describe_task(task)
        assert descriptor is not None
        return {
            "ok": True,
            "created": handling == "registered",
            "adopted": handling != "registered",
            "handling": handling,
            "message": conflict_note or "分发批次已登记",
            "application": app,
            "receipt": self.application_receipt(app),
            **descriptor,
        }

    def reconcile_application(
        self,
        app_id: int | str,
        *,
        reclaim_missing: bool = True,
    ) -> dict[str, Any]:
        """核对不确定申请；有真实行则接受，没有则收回旧资格并领用新资格。"""
        with self.lock:
            app = self.find_application(app_id)
            if app is None:
                return {"ok": False, "message": "分发申请不存在"}
            if app.get("处理状态") == "已登记" and app.get("分发任务ID"):
                task = self.store.find("dist_task", int(app["分发任务ID"]))
                expected_batch = str(app.get("分发批次", "") or "")
                if (
                    task is not None
                    and (not expected_batch or str(task.get("分发批次", "")) == expected_batch)
                ):
                    return self._adopt_task(app, task, handling="already_registered")

            task = self.find_canonical_task(
                idempotency_key=str(app.get("申请键", "")),
                credential=str(app.get("登记凭据", "")),
                batch_no=str(app.get("planned_batch_no", "")),
                subscriber=str(app.get("订阅方", "")),
                version=str(app.get("发布版本", "")),
            )
            if task is not None:
                # 明确按原申请字段核对，不能因为编号相近拿另一批行补齐。
                if str(task.get("订阅方", "")) != str(app.get("订阅方", "")):
                    return self._adopt_task(app, task, handling="late_original")
                return self._adopt_task(app, task, handling="late_original")

            if not reclaim_missing:
                self._mark_app(
                    app,
                    处理状态="待核对",
                    status="待核对",
                    原因="未查到实际登记行，仍需核对晚到回执",
                    待办原因="核对晚到回执或重新领用资格",
                )
                return {"ok": False, "registered": False, "application": app}

            current_status = str(app.get("处理状态", "") or "")
            if current_status == "待登记":
                # 尚未进入写入的初始资格仍可直接使用，不能白白跳号或把
                # 正常待办误判成中断。
                return {
                    "ok": True,
                    "registered": False,
                    "reclaimed": False,
                    "pending": True,
                    "application": app,
                }

            # 只有“写入中/结果未确认/登记失败”的资格才需要在核对无行后
            # 收回并重新领用；原申请和旧资格历史继续保留可查。
            old_credential = str(app.get("登记凭据", "") or "")
            old_batch = str(app.get("planned_batch_no", "") or "")
            history = list(app.get("资格历史", []) or [])
            history.append({
                "generation": int(app.get("资格版本", 1) or 1),
                "credential": old_credential,
                "planned_task_id": app.get("planned_task_id"),
                "planned_batch_no": old_batch,
                "state": "已收回",
                "reason": "核对时未找到实际写入行",
                "revoked_at": utc_now(),
            })
            new_task_id = self.next_task_id()
            new_credential = self.new_credential()
            new_batch = self.make_batch_no(
                new_task_id,
                number_style=str(app.get("number_style", "fanout")),
                publish_entry_id=app.get("发布条目ID"),
                subscriber=str(app.get("订阅方", "")),
            )
            generation = int(app.get("资格版本", 1) or 1) + 1
            self._mark_app(
                app,
                资格历史=history,
                资格版本=generation,
                资格状态="有效",
                资格凭据=new_credential,
                登记凭据=new_credential,
                planned_task_id=new_task_id,
                planned_batch_no=new_batch,
                分发任务ID=None,
                分发批次="",
                处理状态="待登记",
                status="待登记",
                pending=True,
                abnormal=False,
                原因="未查到实际写入；原安排已收回，已重新领用补登记资格",
                待办原因="使用新资格补登记",
                冲突处理="reclaimed_missing",
            )
            return {
                "ok": True,
                "registered": False,
                "reclaimed": True,
                "message": "原安排未实际登记，已收回并重新领用资格",
                "application": app,
                "credential": new_credential,
                "new_credential": new_credential,
                "new_batch_no": new_batch,
            }

    def resume_application(self, app_id: int | str) -> dict[str, Any]:
        result = self.reconcile_application(app_id, reclaim_missing=True)
        app = self.find_application(app_id)
        if app is None:
            return {"ok": False, "message": "分发申请不存在"}
        if app.get("处理状态") == "已登记":
            return result
        return self.register_application(app["id"])

    def register_dispatch(
        self,
        *,
        subscriber: str = "",
        version: str = "",
        publish_entry_id: int | None = None,
        subscriber_entry_id: int | None = None,
        number_style: str = "subscriber",
        source: str = "api.register_dispatch",
        group_id: str = "",
        position: int | None = None,
        request_key: str | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """通用登记入口：准备申请并收敛到唯一真实分发批次。"""
        app = self.prepare_application(
            source=source,
            subscriber=subscriber,
            version=version,
            publish_entry_id=publish_entry_id,
            subscriber_entry_id=subscriber_entry_id,
            number_style=number_style,
            group_id=group_id,
            position=position,
            request_key=request_key,
        )
        return self.resume_application(app["id"])

    create_dispatch = register_dispatch
    register_batch = register_dispatch
    create_batch = register_dispatch

    def find_all_canonical_tasks(self, app: dict[str, Any]) -> list[dict[str, Any]]:
        """列出同一申请键下所有精确匹配行，供晚到写入/重复确认冲突核对。"""
        key = str(app.get("申请键", "") or "")
        credential = str(
            app.get("申请凭据", "")
            or app.get("登记凭据", "")
            or app.get("资格凭据", "")
        )
        planned_batch = str(app.get("planned_batch_no", "") or "")
        subscriber = str(app.get("订阅方", "") or "")
        version = str(app.get("发布版本", "") or "")
        result: dict[Any, dict[str, Any]] = {}

        def add(task: dict[str, Any] | None) -> None:
            if task is not None and task.get("id") is not None:
                result[task["id"]] = task

        add(self.find_task_by_credential(credential))
        add(self.find_task_by_batch(str(app.get("分发批次", "") or "")))
        add(self.find_task_by_batch(planned_batch))
        for row in self.tasks():
            row_key = row.get("幂等键")
            if key:
                if row_key == key:
                    add(row)
                elif not row_key and (
                    subscriber
                    and str(row.get("订阅方", "")) == subscriber
                    and version
                    and self.version_from_task(row) == version
                ):
                    add(row)
            elif (
                subscriber
                and str(row.get("订阅方", "")) == subscriber
                and version
                and self.version_from_task(row) == version
            ):
                add(row)
        return sorted(result.values(), key=lambda row: int(row.get("id", 0) or 0))

    def inspect_application(self, app_id: int | str) -> dict[str, Any]:
        """返回原申请、实际登记行和确认单，调用方不会把二者混为一谈。"""
        app = self.find_application(app_id)
        if app is None:
            return {"ok": False, "message": "分发申请不存在"}
        tasks = self.find_all_canonical_tasks(app)
        descriptors = [self.describe_task(task) for task in tasks]
        related_apps = [
            item for item in self.applications()
            if item.get("申请键") == app.get("申请键")
        ]
        registered = app.get("处理状态") == "已登记" and app.get("分发任务ID") is not None
        return {
            "ok": True,
            "application": app,
            "original_application": app,
            "related_applications": related_apps,
            "other_applications": [item for item in related_apps if item.get("id") != app.get("id")],
            "receipt": self.application_receipt(app),
            "actual_tasks": tasks,
            "task_descriptors": descriptors,
            "confirmations": descriptors,
            "confirmation_count": len(tasks),
            "has_late_conflict": len(tasks) > 1,
            "registered": registered,
            "application_status": app.get("处理状态"),
        }

    def resolve_conflict(
        self,
        app_id: int | str,
        decision: str = "auto",
        *,
        batch_no: str = "",
        credential: str = "",
        void_existing: bool = False,
        register: bool = True,
    ) -> dict[str, Any]:
        """解决续办与晚到回执的冲突。

        decision/strategy 支持：
        * ``auto``/``reconcile``：有真实行就接受，无行则收回旧资格补登记；
        * ``original``/``accept_original``：接受原凭据/原编号对应的晚到登记；
        * ``other``/``accept_other``：接受另一张已经确认的批次单；
        * ``reclaim``/``reregister``：收回原安排，重新领用资格后补登记。
        """
        decision = str(decision or "auto").strip().lower()
        app = self.find_application(app_id)
        if app is None:
            return {"ok": False, "message": "分发申请不存在"}

        if decision in {"auto", "reconcile", "resume", "continue"}:
            return self.resume_application(app_id) if register else self.reconcile_application(app_id)

        tasks = self.find_all_canonical_tasks(app)
        wanted_batch = str(batch_no or "")
        wanted_credential = str(credential or "")

        if decision in {"original", "accept_original", "late", "accept_late"}:
            candidate = None
            if wanted_batch:
                candidate = self.find_task_by_batch(wanted_batch)
            if candidate is None and wanted_credential:
                candidate = self.find_task_by_credential(wanted_credential)
            if candidate is None:
                original_credential = str(
                    app.get("申请凭据", "")
                    or app.get("原始申请凭据", "")
                    or (app.get("资格历史", [{}])[0].get("credential", "") if app.get("资格历史") else "")
                    or app.get("登记凭据", "")
                )
                candidate = self.find_task_by_credential(original_credential)
            if candidate is None:
                candidate = self.find_task_by_batch(str(app.get("planned_batch_no", "")))
            if candidate is None and tasks:
                candidate = tasks[0]
            if candidate is None:
                if register:
                    return self.resume_application(app_id)
                return {"ok": False, "message": "没有可接受的原登记行", "application": app}
            return self._adopt_task(app, candidate, handling="late_original")

        if decision in {"other", "accept_other", "confirmed_other"}:
            candidate = None
            if wanted_batch:
                candidate = self.find_task_by_batch(wanted_batch)
            if candidate is None and wanted_credential:
                candidate = self.find_task_by_credential(wanted_credential)
            if candidate is None and tasks:
                current_id = app.get("分发任务ID")
                candidate = next((t for t in tasks if t.get("id") != current_id), tasks[-1])
            if candidate is None:
                return {"ok": False, "message": "没有找到另一张确认单", "application": app}
            return self._adopt_task(app, candidate, handling="accepted_confirmed_other")

        if decision in {"reclaim", "reregister", "reclaim_and_register", "revoke"}:
            if void_existing:
                for task in tasks:
                    if wanted_batch and task.get("分发批次") != wanted_batch:
                        continue
                    task["status"] = "分发失败"
                    task["分发状态"] = "分发失败"
                    task["abnormal"] = True
                    task["有效发放"] = False
                    task["收回原因"] = "冲突裁决：收回原安排，改用新领用资格补登记"
            # 强制当前申请进入可收回状态；原申请和资格历史仍保留。
            self._mark_app(app, 处理状态="登记结果未确认", status="登记结果未确认")
            reconciled = self.reconcile_application(app_id, reclaim_missing=True)
            if not reconciled.get("ok"):
                return reconciled
            if register:
                return self.register_application(app["id"])
            return reconciled

        return {"ok": False, "message": f"未知冲突处理方式：{decision}", "application": app}

    def accept_original_registration(self, app_id: int | str, **kwargs: Any) -> dict[str, Any]:
        return self.resolve_conflict(app_id, "original", **kwargs)

    def accept_other_confirmation(self, app_id: int | str, **kwargs: Any) -> dict[str, Any]:
        return self.resolve_conflict(app_id, "other", **kwargs)

    def reclaim_and_reregister(self, app_id: int | str, **kwargs: Any) -> dict[str, Any]:
        return self.resolve_conflict(app_id, "reclaim", register=True, **kwargs)

    # ---------- 整组 ----------

    def group_state(self, group_id: str) -> dict[str, Any] | None:
        with self.lock:
            apps = [a for a in self.applications() if a.get("整组编号") == group_id]
            if not apps:
                app = self.find_application(group_id=group_id)
                if app is None:
                    return None
                group_id = str(app.get("整组编号", ""))
                apps = [a for a in self.applications() if a.get("整组编号") == group_id]
            def position_key(row: dict[str, Any]) -> tuple[bool, int, int]:
                try:
                    position = int(row.get("顺序")) if row.get("顺序") is not None else None
                except (TypeError, ValueError):
                    position = None
                return (
                    position is None,
                    position if position is not None else 0,
                    int(row.get("id", 0) or 0),
                )

            apps.sort(key=position_key)
            registered = [a for a in apps if a.get("处理状态") == "已登记"]
            pending = [a for a in apps if a.get("处理状态") not in {"已登记"}]
            return {
                "ok": True,
                "group_id": group_id,
                "整组编号": group_id,
                "total": len(apps),
                "registered_count": len(registered),
                "pending_count": len(pending),
                "interrupted": bool(pending),
                "completed": not pending,
                "application_ids": [a.get("id") for a in apps],
                "task_ids": [a.get("分发任务ID") for a in registered if a.get("分发任务ID") is not None],
                "batch_nos": [a.get("分发批次") for a in registered if a.get("分发批次")],
                "items": apps,
                "applications": apps,
            }

    get_group = group_state
    get_group_state = group_state

    def application_receipt(self, app: dict[str, Any]) -> dict[str, Any]:
        task = None
        if app.get("分发任务ID") is not None:
            task = self.store.find("dist_task", int(app["分发任务ID"]))
        descriptor = self.describe_task(task) if task is not None else None
        return {
            "ok": app.get("处理状态") == "已登记",
            "application_id": app.get("id"),
            "申请编号": app.get("申请编号"),
            "group_id": app.get("整组编号", ""),
            "整组编号": app.get("整组编号", ""),
            "position": app.get("顺序"),
            "batch_no": app.get("分发批次", ""),
            "分发批次": app.get("分发批次", ""),
            "task_id": app.get("分发任务ID"),
            "credential": app.get("登记凭据", ""),
            "登记凭据": app.get("登记凭据", ""),
            "subscriber": app.get("订阅方", ""),
            "订阅方": app.get("订阅方", ""),
            "version": app.get("发布版本", ""),
            "发布版本": app.get("发布版本", ""),
            "原发放版本": app.get("发布版本", ""),
            "原始订阅方": app.get("订阅方", ""),
            "status": app.get("处理状态", ""),
            "reason": app.get("原因", ""),
            "原因": app.get("原因", ""),
            "todo_reason": app.get("待办原因", ""),
            "待办原因": app.get("待办原因", ""),
            "descriptor": descriptor,
        }

    # ---------- 回执编号 ----------

    def register_receipt(
        self,
        *,
        number_style: str,
        subscriber: str,
        status: str,
        diff: str = "",
        batch_no: str = "",
        task_entry_id: int | str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self.lock:
            receipt_id = self.next_receipt_id()
            if number_style == "task":
                receipt_no = f"rc-{task_entry_id}-{receipt_id}"
            elif number_style == "rollback":
                receipt_no = f"rc-rollback-{receipt_id}"
            elif number_style == "subscriber":
                receipt_no = f"rc-{subscriber}-{receipt_id}"
            else:
                receipt_no = f"rc-{receipt_id}"
            row: dict[str, Any] = {
                "id": receipt_id,
                "回执编号": receipt_no,
                "分发批次": batch_no,
                "订阅方": subscriber,
                "回执状态": status,
                "差异说明": diff,
                "status": status,
                "pending": status != "已回收",
                "abnormal": bool(diff) or status == "未回收",
            }
            if extra:
                row.update(extra)
            self.receipts().append(row)
            return row


def _maybe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


registry = DispatchRegistry()

# 常见命名别名；测试或其他模块无论引用哪一个都使用同一登记处和同一把锁。
dispatch_registry = registry
distribution_registry = registry
dispatch_center = registry


def register_dispatch_batch(**kwargs: Any) -> dict[str, Any]:
    """模块级便捷封装，供希望直接 import 函数的调用方使用。"""
    return registry.register_dispatch(**kwargs)


def claim_dispatch_batch(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return registry.claim_batch(*args, **kwargs)


def get_dispatch_batch(batch_no: str) -> dict[str, Any] | None:
    return registry.find_task_by_batch(batch_no)


def reconcile_dispatch_application(app_id: int | str, **kwargs: Any) -> dict[str, Any]:
    return registry.reconcile_application(app_id, **kwargs)
