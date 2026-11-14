import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

Fingerprint = tuple[str, str, str | None]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def fingerprint(name: str, address: str, phone: str | None) -> Fingerprint:
    """Identity used to match a CSV row to a record that already exists upstream."""
    return (name, address, phone or None)


class JobStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    PARTIALLY_FAILED = "partially_failed"  # some rows missing upstream; batch left inactive
    ACTIVATION_FAILED = "activation_failed"  # every row exists, activation call failed
    FAILED = "failed"  # upstream unreachable / unexpected error mid-run
    ROLLED_BACK = "rolled_back"

    @property
    def is_active(self) -> bool:
        return self in (JobStatus.PENDING, JobStatus.PROCESSING)

    @property
    def is_resumable(self) -> bool:
        return self in (JobStatus.PARTIALLY_FAILED, JobStatus.ACTIVATION_FAILED, JobStatus.FAILED)


class RowStatus(str, Enum):
    PENDING = "pending"
    CREATED = "created"
    CREATED_AND_ACTIVATED = "created_and_activated"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


_SUCCEEDED = (RowStatus.CREATED, RowStatus.CREATED_AND_ACTIVATED)


@dataclass(frozen=True)
class HospitalRecord:
    """A validated CSV row. Immutable, so every attempt sends exactly the same payload."""

    row: int
    name: str
    address: str
    phone: str | None = None

    @property
    def fingerprint(self) -> Fingerprint:
        return fingerprint(self.name, self.address, self.phone)


@dataclass
class RowResult:
    record: HospitalRecord
    status: RowStatus = RowStatus.PENDING
    hospital_id: int | None = None
    error: str | None = None
    retryable: bool = True
    attempts: int = 0

    def mark_created(self, hospital_id: int) -> None:
        self.hospital_id = hospital_id
        self.status = RowStatus.CREATED
        self.error = None

    def mark_failed(self, error: str, *, retryable: bool) -> None:
        self.status = RowStatus.FAILED
        self.error = error
        self.retryable = retryable

    def mark_missing(self) -> None:
        """Upstream no longer has the record we created; it must be created again."""
        self.hospital_id = None
        self.status = RowStatus.PENDING
        self.error = None


@dataclass
class BatchJob:
    batch_id: uuid.UUID
    rows: list[RowResult]
    status: JobStatus = JobStatus.PENDING
    batch_activated: bool = False
    error: str | None = None
    processing_time_seconds: float = 0.0
    runs: int = 0
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)

    @classmethod
    def from_records(cls, records: Iterable[HospitalRecord]) -> "BatchJob":
        return cls(batch_id=uuid.uuid4(), rows=[RowResult(record=r) for r in records])

    @property
    def total(self) -> int:
        return len(self.rows)

    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.rows if r.status in _SUCCEEDED)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.rows if r.status is RowStatus.FAILED)

    @property
    def missing_upstream(self) -> list[RowResult]:
        return [r for r in self.rows if r.hospital_id is None]

    def touch(self) -> None:
        self.updated_at = utcnow()
