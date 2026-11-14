from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field

from app.domain.models import BatchJob, JobStatus, RowResult, RowStatus
from app.services.csv_parser import CsvIssue, CsvParseResult


class ProcessingMode(str, Enum):
    SYNC = "sync"
    ASYNC = "async"


class HospitalRowResponse(BaseModel):
    row: int
    hospital_id: int | None
    name: str
    status: RowStatus
    error: str | None = None

    @classmethod
    def from_row(cls, row: RowResult) -> "HospitalRowResponse":
        return cls(
            row=row.record.row,
            hospital_id=row.hospital_id,
            name=row.record.name,
            status=row.status,
            error=row.error,
        )


class ProgressResponse(BaseModel):
    total: int
    completed: int = Field(description="Rows that reached a final state (created or failed)")
    succeeded: int
    failed: int
    percent: float


class BulkJobResponse(BaseModel):
    batch_id: UUID
    status: JobStatus
    total_hospitals: int
    processed_hospitals: int = Field(description="Hospitals successfully created upstream")
    failed_hospitals: int
    processing_time_seconds: float
    batch_activated: bool
    progress: ProgressResponse
    error: str | None = None
    hospitals: list[HospitalRowResponse]

    @classmethod
    def from_job(cls, job: BatchJob) -> "BulkJobResponse":
        succeeded, failed = job.succeeded, job.failed
        completed = succeeded + failed
        return cls(
            batch_id=job.batch_id,
            status=job.status,
            total_hospitals=job.total,
            processed_hospitals=succeeded,
            failed_hospitals=failed,
            processing_time_seconds=round(job.processing_time_seconds, 3),
            batch_activated=job.batch_activated,
            progress=ProgressResponse(
                total=job.total,
                completed=completed,
                succeeded=succeeded,
                failed=failed,
                percent=round(100 * completed / job.total, 1) if job.total else 100.0,
            ),
            error=job.error,
            hospitals=[HospitalRowResponse.from_row(row) for row in job.rows],
        )


class CsvIssueResponse(BaseModel):
    row: int | None = None
    field: str | None = None
    message: str

    @classmethod
    def from_issue(cls, issue: CsvIssue) -> "CsvIssueResponse":
        return cls(row=issue.row, field=issue.field, message=issue.message)


class CsvValidationResponse(BaseModel):
    valid: bool
    total_rows: int
    errors: list[CsvIssueResponse]
    warnings: list[CsvIssueResponse]

    @classmethod
    def from_result(cls, result: CsvParseResult) -> "CsvValidationResponse":
        return cls(
            valid=result.is_valid,
            total_rows=result.total_rows,
            errors=[CsvIssueResponse.from_issue(i) for i in result.errors],
            warnings=[CsvIssueResponse.from_issue(i) for i in result.warnings],
        )


class ErrorResponse(BaseModel):
    detail: str


class CsvErrorResponse(ErrorResponse):
    errors: list[CsvIssueResponse]
    warnings: list[CsvIssueResponse]
