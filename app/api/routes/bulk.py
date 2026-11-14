import logging
from typing import Annotated, Any
from uuid import UUID

from fastapi import (
    APIRouter,
    File,
    Query,
    Request,
    Response,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)

from app.api.dependencies import BulkServiceDep, ProgressBrokerDep, SettingsDep
from app.api.uploads import read_csv_upload
from app.domain.models import BatchJob
from app.schemas.bulk import (
    BulkJobResponse,
    CsvErrorResponse,
    CsvValidationResponse,
    ErrorResponse,
    ProcessingMode,
)
from app.services.csv_parser import CsvValidationError, parse_hospital_csv
from app.services.progress import ProgressEvent

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/hospitals/bulk", tags=["Bulk processing"])

CsvFile = Annotated[
    UploadFile, File(description="CSV with header `name,address,phone` (phone optional)")
]
Mode = Annotated[
    ProcessingMode,
    Query(
        description=(
            "`sync` waits and returns the final result. `async` returns 202 immediately; "
            "follow progress via the status endpoint or the WebSocket."
        )
    ),
]

Responses = dict[int | str, dict[str, Any]]

_NOT_FOUND: Responses = {404: {"model": ErrorResponse, "description": "Unknown batch id"}}
_CONFLICT: Responses = {
    409: {"model": ErrorResponse, "description": "Batch is in a state that forbids this"}
}
_UPSTREAM: Responses = {
    502: {"model": ErrorResponse, "description": "Hospital Directory API failed"}
}


@router.post(
    "",
    response_model=BulkJobResponse,
    summary="Bulk create hospitals from a CSV",
    responses={
        202: {"model": BulkJobResponse, "description": "Accepted for background processing"},
        413: {"model": ErrorResponse},
        415: {"model": ErrorResponse},
        422: {
            "model": CsvErrorResponse,
            "description": "CSV failed validation; nothing was created",
        },
    },
)
async def bulk_create_hospitals(
    request: Request,
    response: Response,
    file: CsvFile,
    service: BulkServiceDep,
    settings: SettingsDep,
    mode: Mode = ProcessingMode.SYNC,
) -> BulkJobResponse:
    content = await read_csv_upload(file, max_bytes=settings.max_upload_bytes)
    parsed = parse_hospital_csv(content, max_rows=settings.max_csv_rows)
    if not parsed.is_valid:
        raise CsvValidationError(parsed)

    job = await service.create_job(parsed.records)
    if mode is ProcessingMode.ASYNC:
        service.start(job)
        _accepted(request, response, job)
    else:
        await service.run(job)
    return BulkJobResponse.from_job(job)


@router.post(
    "/validate",
    response_model=CsvValidationResponse,
    summary="Validate a CSV without creating anything",
    responses={413: {"model": ErrorResponse}, 415: {"model": ErrorResponse}},
)
async def validate_csv(file: CsvFile, settings: SettingsDep) -> CsvValidationResponse:
    content = await read_csv_upload(file, max_bytes=settings.max_upload_bytes)
    return CsvValidationResponse.from_result(
        parse_hospital_csv(content, max_rows=settings.max_csv_rows)
    )


@router.get(
    "/{batch_id}",
    name="get_bulk_job",
    response_model=BulkJobResponse,
    summary="Batch status and progress (polling)",
    responses=_NOT_FOUND,
)
async def get_bulk_job(batch_id: UUID, service: BulkServiceDep) -> BulkJobResponse:
    return BulkJobResponse.from_job(await service.get_job(batch_id))


@router.post(
    "/{batch_id}/resume",
    response_model=BulkJobResponse,
    summary="Resume a failed or partially failed batch",
    description=(
        "Reconciles with the Hospital Directory (adopting rows that were in fact created, "
        "removing duplicates), retries only the rows that are still missing, then activates."
    ),
    responses={202: {"model": BulkJobResponse}, **_NOT_FOUND, **_CONFLICT},
)
async def resume_bulk_job(
    batch_id: UUID,
    request: Request,
    response: Response,
    service: BulkServiceDep,
    mode: Mode = ProcessingMode.SYNC,
) -> BulkJobResponse:
    job = await service.get_job(batch_id)
    if mode is ProcessingMode.ASYNC:
        service.start(job, resume=True)
        _accepted(request, response, job)
    else:
        await service.run(job, resume=True)
    return BulkJobResponse.from_job(job)


@router.delete(
    "/{batch_id}",
    response_model=BulkJobResponse,
    summary="Roll back a batch (deletes its hospitals upstream)",
    responses={**_NOT_FOUND, **_CONFLICT, **_UPSTREAM},
)
async def rollback_bulk_job(batch_id: UUID, service: BulkServiceDep) -> BulkJobResponse:
    return BulkJobResponse.from_job(await service.rollback(batch_id))


@router.websocket("/{batch_id}/ws")
async def stream_progress(
    websocket: WebSocket,
    batch_id: UUID,
    service: BulkServiceDep,
    broker: ProgressBrokerDep,
) -> None:
    """Pushes the full batch state on every change; the last frame is always `finished`."""
    await websocket.accept()
    job = await service.find_job(batch_id)
    if job is None:
        await websocket.send_json({"event": "error", "detail": f"Batch {batch_id} not found"})
        await websocket.close(code=4404)
        return

    try:
        # Subscribe before taking the snapshot so no event can fall between the two.
        async with broker.subscribe(batch_id) as events:
            await websocket.send_json(_frame("snapshot", job))
            while job.status.is_active:
                event = await events.get()
                if event is not ProgressEvent.FINISHED:
                    await websocket.send_json(_frame(event.value, job))
            await websocket.send_json(_frame(ProgressEvent.FINISHED.value, job))
        await websocket.close()
    except WebSocketDisconnect:
        logger.debug("batch=%s progress subscriber disconnected", batch_id)


def _accepted(request: Request, response: Response, job: BatchJob) -> None:
    response.status_code = 202
    response.headers["Location"] = str(request.url_for("get_bulk_job", batch_id=job.batch_id))


def _frame(event: str, job: BatchJob) -> dict[str, Any]:
    return {"event": event, **BulkJobResponse.from_job(job).model_dump(mode="json")}
