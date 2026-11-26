"""分发登记/续办/查证专用接口。

这些接口是两个业务页面共用登记处的 HTTP 适配层；旧的通用资源接口和
服务方法签名仍然保留，隐藏调用方可继续直接使用原服务方法。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Query

from app.services.dispatch_registry import registry
from app.services.dist_receipt import DistReceiptService
from app.services.dist_task import DistTaskService
from app.services.golden_publish import GoldenPublishService
from app.services.subscriber_mgr import SubscriberMgrService

router = APIRouter(prefix="/api/dispatch", tags=["分发登记"])

publish_service = GoldenPublishService()
subscriber_service = SubscriberMgrService()
task_service = DistTaskService()
receipt_service = DistReceiptService()


@router.post("/golden-publishes/{publish_id}/fanout")
@router.post("/golden_publish/{publish_id}/fanout")
@router.post("/fanout/{publish_id}")
def fanout_group(publish_id: int) -> dict[str, Any]:
    return publish_service.submit_fanout(publish_id)


@router.post("/golden-publishes/{publish_id}/resume")
@router.post("/golden_publish/{publish_id}/resume")
@router.post("/groups/{publish_id}/resume")
def resume_group(publish_id: int) -> dict[str, Any]:
    return publish_service.submit_fanout(publish_id)


@router.get("/golden-publishes/{publish_id}")
@router.get("/golden_publish/{publish_id}/status")
@router.get("/groups/{publish_id}")
def group_status(publish_id: int) -> dict[str, Any]:
    state = publish_service.get_fanout_status(publish_id)
    if state is None:
        return {"ok": False, "message": "发布批次不存在或不是已发布状态"}
    return state


@router.post("/subscribers/{subscriber_id}/produce")
@router.post("/subscriber_mgr/{subscriber_id}/produce")
@router.post("/produce/{subscriber_id}")
def produce_single(
    subscriber_id: int,
    payload: dict[str, Any] | None = Body(default=None),
    query_version: str | None = Query(default=None, alias="version"),
) -> dict[str, Any]:
    payload = payload or {}
    version = str(payload.get("version") or payload.get("版本标识") or query_version or "")
    return subscriber_service.submit_single_dispatch(subscriber_id, version)


@router.post("/subscribers/{subscriber_id}/arrange")
@router.post("/arrange/{subscriber_id}")
def arrange_single(
    subscriber_id: int,
    payload: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    return produce_single(subscriber_id, payload)


@router.post("/register")
def register_dispatch(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    payload = payload or {}
    return registry.register_dispatch(
        subscriber=str(payload.get("subscriber") or payload.get("订阅方") or ""),
        version=str(payload.get("version") or payload.get("发布版本") or payload.get("版本标识") or ""),
        publish_entry_id=payload.get("publish_entry_id") or payload.get("发布条目ID"),
        subscriber_entry_id=payload.get("subscriber_entry_id") or payload.get("订阅条目ID"),
        number_style=str(payload.get("number_style") or payload.get("numberStyle") or "subscriber"),
        source=str(payload.get("source") or "api.register"),
        group_id=str(payload.get("group_id") or payload.get("整组编号") or ""),
        position=payload.get("position") or payload.get("顺序"),
        request_key=payload.get("request_key") or payload.get("申请键"),
    )


@router.get("/applications")
def list_applications(
    request_key: str | None = Query(default=None),
    credential: str | None = Query(default=None),
    group_id: str | None = Query(default=None),
) -> dict[str, Any]:
    rows = registry.applications()
    if request_key:
        rows = [r for r in rows if r.get("申请键") == request_key]
    if credential:
        rows = [r for r in rows if credential in {
            str(r.get("登记凭据", "")), str(r.get("资格凭据", ""))
        }]
    if group_id:
        rows = [r for r in rows if r.get("整组编号") == group_id]
    return {"ok": True, "items": rows, "total": len(rows)}


@router.get("/applications/{app_id}")
def get_application(app_id: str) -> dict[str, Any]:
    app = registry.find_application(app_id, credential=app_id if app_id.startswith("reg-") else "")
    if app is None:
        return {"ok": False, "message": "分发申请不存在"}
    return {"ok": True, "application": app, "receipt": registry.application_receipt(app)}


@router.get("/applications/{app_id}/inspect")
def inspect_application(app_id: str) -> dict[str, Any]:
    return registry.inspect_application(app_id)


@router.post("/applications/{app_id}/resolve")
def resolve_application(app_id: str, payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    payload = payload or {}
    return registry.resolve_conflict(
        app_id,
        str(payload.get("decision") or payload.get("strategy") or "auto"),
        batch_no=str(payload.get("batch_no") or payload.get("分发批次") or ""),
        credential=str(payload.get("credential") or payload.get("登记凭据") or ""),
        void_existing=bool(payload.get("void_existing", False)),
        register=bool(payload.get("register", True)),
    )


@router.post("/applications/{app_id}/resume")
def resume_application(app_id: str) -> dict[str, Any]:
    return registry.resume_application(app_id)


@router.get("/open")
def open_registration(
    ref: str | None = Query(default=None),
    batch_no: str | None = Query(default=None),
    credential: str | None = Query(default=None),
    subscriber: str | None = Query(default=None),
    version: str | None = Query(default=None),
) -> dict[str, Any]:
    return registry.claim_batch(
        batch_no=batch_no or "",
        ref=ref or "",
        credential=credential or "",
        subscriber=subscriber or "",
        version=version or "",
    )


@router.get("/batch/{batch_no:path}")
def get_batch(
    batch_no: str,
    credential: str | None = Query(default=None),
    subscriber: str | None = Query(default=None),
    version: str | None = Query(default=None),
) -> dict[str, Any]:
    return task_service.get_by_batch(
        batch_no,
        credential=credential or "",
        subscriber=subscriber or "",
        version=version or "",
    )


@router.post("/claim")
def claim_batch(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    payload = payload or {}
    return registry.claim_batch(
        batch_no=str(payload.get("batch_no") or payload.get("分发批次") or ""),
        ref=str(payload.get("ref") or ""),
        credential=str(payload.get("credential") or payload.get("登记凭据") or ""),
        subscriber=str(payload.get("subscriber") or payload.get("订阅方") or ""),
        version=str(payload.get("version") or payload.get("发布版本") or ""),
    )


@router.post("/sign-by-batch")
def sign_by_batch(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    payload = payload or {}
    batch_no = str(payload.get("batch_no") or payload.get("分发批次") or "")
    return task_service.sign_by_batch(batch_no)


@router.get("/receipts/verify-batch")
def verify_receipt_by_batch(
    batch_no: str = Query(...),
    subscriber: str | None = Query(default=None),
    version: str | None = Query(default=None),
    credential: str | None = Query(default=None),
) -> dict[str, Any]:
    return receipt_service.verify_by_batch(
        batch_no,
        subscriber=subscriber or "",
        version=version or "",
        credential=credential or "",
    )


@router.post("/receipts/sign-verify-batch")
def sign_verify_batch(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    payload = payload or {}
    batch_no = str(payload.get("batch_no") or payload.get("分发批次") or "")
    return receipt_service.sign_and_verify_by_batch(batch_no)


# 也提供贴近模块资源的旧入口路径，便于页面/测试按两个原模块名调用。
root_router = APIRouter(prefix="/api", tags=["分发登记模块路径"])


@root_router.post("/golden_publish/{publish_id}/fanout")
@root_router.post("/golden_publish/{publish_id}/fanout_to_subscribers")
def root_fanout(publish_id: int) -> dict[str, Any]:
    return publish_service.submit_fanout(publish_id)


@root_router.post("/golden_publish/{publish_id}/resume")
def root_resume(publish_id: int) -> dict[str, Any]:
    return publish_service.submit_fanout(publish_id)


@root_router.get("/golden_publish/{publish_id}/dispatch-status")
def root_group_status(publish_id: int) -> dict[str, Any]:
    state = publish_service.get_fanout_status(publish_id)
    return state or {"ok": False, "message": "发布批次不存在或不是已发布状态"}


@root_router.post("/subscriber_mgr/{subscriber_id}/produce")
@root_router.post("/subscriber_mgr/{subscriber_id}/produce_dist_batch")
def root_produce(
    subscriber_id: int,
    payload: dict[str, Any] | None = Body(default=None),
    query_version: str | None = Query(default=None, alias="version"),
) -> dict[str, Any]:
    payload = payload or {}
    version = str(payload.get("version") or payload.get("版本标识") or query_version or "")
    return subscriber_service.submit_single_dispatch(subscriber_id, version)


@root_router.get("/dist_task/by-batch")
def root_task_by_batch(
    batch_no: str = Query(...),
    credential: str | None = Query(default=None),
) -> dict[str, Any]:
    return task_service.get_by_batch(batch_no, credential=credential or "")


@root_router.post("/dist_task/sign-by-batch")
def root_sign_batch(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    payload = payload or {}
    batch_no = str(payload.get("batch_no") or payload.get("分发批次") or "")
    return task_service.sign_by_batch(batch_no)


@root_router.post("/dist_task/{task_id}/dispatch_batch")
def root_dispatch_task(task_id: int) -> dict[str, Any]:
    return task_service.dispatch_batch(task_id)


@root_router.post("/dist_task/{task_id}/confirm_delivery")
def root_confirm_delivery(task_id: int) -> dict[str, Any]:
    return task_service.confirm_delivery(task_id)


@root_router.post("/dist_receipt/{receipt_id}/verify_receipt")
def root_verify_receipt(receipt_id: int) -> dict[str, Any]:
    return receipt_service.verify_receipt(receipt_id)


@root_router.get("/dist_receipt/verify-batch")
def root_verify_batch(
    batch_no: str = Query(...),
    subscriber: str | None = Query(default=None),
    version: str | None = Query(default=None),
    credential: str | None = Query(default=None),
) -> dict[str, Any]:
    return receipt_service.verify_by_batch(
        batch_no,
        subscriber=subscriber or "",
        version=version or "",
        credential=credential or "",
    )
