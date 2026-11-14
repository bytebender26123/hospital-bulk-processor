from collections import OrderedDict
from typing import Protocol
from uuid import UUID

from app.domain.models import BatchJob


class JobRepository(Protocol):
    async def add(self, job: BatchJob) -> None: ...

    async def get(self, batch_id: UUID) -> BatchJob | None: ...

    async def save(self, job: BatchJob) -> None: ...


class InMemoryJobRepository:
    """Process-local job store.

    Jobs are live objects mutated in place by the service, so ``save`` only stamps
    ``updated_at`` here; a Redis/Postgres implementation would serialise in ``save``.
    There are no awaits inside these methods, so on a single event loop they are atomic
    and need no lock.

    Bounded: once ``max_jobs`` is exceeded the oldest *finished* jobs are evicted, so memory
    stays flat no matter how long the process runs. In-flight jobs are never evicted.
    """

    def __init__(self, max_jobs: int = 1000) -> None:
        self._jobs: OrderedDict[UUID, BatchJob] = OrderedDict()
        self._max_jobs = max_jobs

    async def add(self, job: BatchJob) -> None:
        self._jobs[job.batch_id] = job
        self._evict()

    async def get(self, batch_id: UUID) -> BatchJob | None:
        return self._jobs.get(batch_id)

    async def save(self, job: BatchJob) -> None:
        job.touch()
        self._jobs[job.batch_id] = job

    def _evict(self) -> None:
        overflow = len(self._jobs) - self._max_jobs
        if overflow <= 0:
            return
        evictable = [bid for bid, job in self._jobs.items() if not job.status.is_active]
        for batch_id in evictable[:overflow]:
            del self._jobs[batch_id]
