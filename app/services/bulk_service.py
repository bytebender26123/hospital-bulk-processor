"""Bulk import orchestration.

Lifecycle of one batch:

1. Create every row upstream under one ``creation_batch_id``, concurrently (the client's
   shared semaphore bounds total in-flight calls).
2. If some rows failed transiently, reconcile with the upstream batch listing and retry
   them (``auto_retry_passes``).
3. Activate the batch only once every CSV row exists upstream exactly once.
4. Otherwise leave the batch inactive; the caller can resume it or roll it back.

Because hospitals stay inactive until step 3, a failed import never exposes a partial
directory to readers -- the upstream's batch/activation model is used as a cheap
two-phase commit.
"""

import asyncio
import logging
import time
from collections import defaultdict, deque
from collections.abc import Iterable
from uuid import UUID

from app.clients.hospital_directory import Hospital, HospitalDirectoryClient, HospitalDirectoryError
from app.core.exceptions import JobConflictError, JobNotFoundError
from app.core.tasks import BackgroundTaskRegistry
from app.domain.models import (
    BatchJob,
    Fingerprint,
    HospitalRecord,
    JobStatus,
    RowResult,
    RowStatus,
    fingerprint,
)
from app.repositories.jobs import JobRepository
from app.services.progress import ProgressBroker, ProgressEvent

logger = logging.getLogger(__name__)


class BulkHospitalService:
    def __init__(
        self,
        client: HospitalDirectoryClient,
        jobs: JobRepository,
        broker: ProgressBroker,
        tasks: BackgroundTaskRegistry,
        *,
        auto_retry_passes: int = 1,
        retry_pass_delay_seconds: float = 1.0,
    ) -> None:
        self._client = client
        self._jobs = jobs
        self._broker = broker
        self._tasks = tasks
        self._auto_retry_passes = auto_retry_passes
        self._retry_pass_delay = retry_pass_delay_seconds

    # ------------------------------------------------------------------ queries

    async def create_job(self, records: Iterable[HospitalRecord]) -> BatchJob:
        job = BatchJob.from_records(records)
        await self._jobs.add(job)
        return job

    async def find_job(self, batch_id: UUID) -> BatchJob | None:
        return await self._jobs.get(batch_id)

    async def get_job(self, batch_id: UUID) -> BatchJob:
        job = await self._jobs.get(batch_id)
        if job is None:
            raise JobNotFoundError(f"Batch {batch_id} not found")
        return job

    # ----------------------------------------------------------------- commands

    def start(self, job: BatchJob, *, resume: bool = False) -> "asyncio.Task[None]":
        """Claim the job and process it in the background."""
        self._claim(job, resume=resume)
        return self._tasks.spawn(self._execute(job, resume=resume), name=f"bulk-{job.batch_id}")

    async def run(self, job: BatchJob, *, resume: bool = False) -> BatchJob:
        """Process and wait for the result.

        The work runs as an owned task behind ``shield``: if the HTTP client disconnects,
        the batch still finishes (and stays queryable) instead of being cut off mid-write.
        """
        await asyncio.shield(self.start(job, resume=resume))
        return job

    async def rollback(self, batch_id: UUID) -> BatchJob:
        job = await self.get_job(batch_id)
        if job.status is JobStatus.ROLLED_BACK:
            return job
        if job.status.is_active:
            raise JobConflictError(f"Batch {batch_id} is still {job.status.value}")

        previous = job.status
        job.status = JobStatus.PROCESSING  # claim, so resume/rollback can't run concurrently
        try:
            deleted = await self._client.delete_batch(batch_id)
        except HospitalDirectoryError:
            job.status = previous
            raise
        for row in job.rows:
            row.status = RowStatus.ROLLED_BACK
        job.batch_activated = False
        job.status = JobStatus.ROLLED_BACK
        job.error = None
        await self._jobs.save(job)
        self._broker.publish(job.batch_id, ProgressEvent.FINISHED)
        logger.info("batch=%s rolled back, %d hospital(s) deleted upstream", batch_id, deleted)
        return job

    # ---------------------------------------------------------------- internals

    def _claim(self, job: BatchJob, *, resume: bool) -> None:
        # No await between check and set: atomic on the event loop, so two concurrent
        # resume requests for the same batch cannot both start.
        allowed = job.status.is_resumable if resume else job.status is JobStatus.PENDING
        if not allowed:
            action = "resumed" if resume else "started"
            raise JobConflictError(
                f"Batch {job.batch_id} is {job.status.value} and cannot be {action}"
            )
        job.status = JobStatus.PROCESSING
        job.error = None

    async def _execute(self, job: BatchJob, *, resume: bool) -> None:
        job.runs += 1
        started = time.perf_counter()
        logger.info(
            "batch=%s run=%d started (%d rows, resume=%s)",
            job.batch_id,
            job.runs,
            job.total,
            resume,
        )
        self._broker.publish(job.batch_id, ProgressEvent.STARTED)
        try:
            await self._process(job, reconcile_first=resume)
        except HospitalDirectoryError as exc:
            job.status = JobStatus.FAILED
            job.error = f"Hospital Directory API error: {exc.detail}. The batch can be resumed."
        except Exception:
            logger.exception("batch=%s crashed", job.batch_id)
            job.status = JobStatus.FAILED
            job.error = "Unexpected internal error. The batch can be resumed."
        finally:
            if job.status is JobStatus.PROCESSING:  # cancelled, e.g. during shutdown
                job.status = JobStatus.FAILED
                job.error = "Processing was interrupted. The batch can be resumed."
            job.processing_time_seconds += time.perf_counter() - started
            await self._jobs.save(job)
            self._broker.publish(job.batch_id, ProgressEvent.FINISHED)
            logger.info(
                "batch=%s run=%d finished: status=%s created=%d failed=%d in %.2fs",
                job.batch_id,
                job.runs,
                job.status.value,
                job.succeeded,
                job.failed,
                time.perf_counter() - started,
            )

    async def _process(self, job: BatchJob, *, reconcile_first: bool) -> None:
        if reconcile_first:
            # A previous run may have died on ambiguous failures; learn what really exists.
            await self._reconcile(job)
        await self._create_rows(job, job.missing_upstream)

        for attempt in range(1, self._auto_retry_passes + 1):
            transient = [r for r in job.rows if r.status is RowStatus.FAILED and r.retryable]
            if not transient:
                break
            logger.info(
                "batch=%s retry pass %d for %d transient failure(s)",
                job.batch_id,
                attempt,
                len(transient),
            )
            await asyncio.sleep(self._retry_pass_delay)
            await self._reconcile(job)
            await self._create_rows(job, [r for r in transient if r.hospital_id is None])

        missing = job.missing_upstream
        if missing:
            job.status = JobStatus.PARTIALLY_FAILED
            job.error = (
                f"{len(missing)} of {job.total} hospital(s) could not be created; the batch was "
                "left inactive. Resume to retry the failed rows, or roll back."
            )
            return

        if any(r.attempts > 1 for r in job.rows):
            # Something was retried, so a timed-out POST may have landed late and left a
            # duplicate. Verify the upstream batch is exactly the CSV before activating it.
            await self._reconcile(job)
            if job.missing_upstream:
                await self._create_rows(job, job.missing_upstream)
                if job.missing_upstream:
                    job.status = JobStatus.PARTIALLY_FAILED
                    job.error = "Upstream batch changed during verification; resume to retry."
                    return

        await self._activate(job)

    async def _create_rows(self, job: BatchJob, rows: list[RowResult]) -> None:
        if not rows:
            return
        results = await asyncio.gather(
            *(self._create_row(job, row) for row in rows), return_exceptions=True
        )
        # Expected upstream failures are recorded per row; anything else is a bug and must
        # not be swallowed -- but only after every sibling request has settled.
        for outcome in results:
            if isinstance(outcome, BaseException):
                raise outcome

    async def _create_row(self, job: BatchJob, row: RowResult) -> None:
        row.status = RowStatus.PENDING
        row.attempts += 1
        try:
            hospital = await self._client.create_hospital(row.record, job.batch_id)
        except HospitalDirectoryError as exc:
            row.mark_failed(exc.detail, retryable=exc.retryable)
            logger.warning("batch=%s row=%d failed: %s", job.batch_id, row.record.row, exc.detail)
        else:
            row.mark_created(hospital.id)
        self._broker.publish(job.batch_id, ProgressEvent.ROW_UPDATED)

    async def _reconcile(self, job: BatchJob) -> None:
        """Make local row state agree with what actually exists upstream for this batch.

        ``POST /hospitals/`` is not idempotent: a call that timed out or returned 5xx may
        still have created the hospital. So before retrying anything we:

        * adopt upstream records matching a row we think failed (no duplicate is created),
        * forget ids that vanished upstream (they will be recreated),
        * delete upstream records that no row accounts for (late duplicates).

        Deleting is safe because the batch id is a UUID this job generated -- every record
        carrying it was created by us.
        """
        remote = await self._client.list_batch(job.batch_id)
        remote_ids = {h.id for h in remote}

        for row in job.rows:
            if row.hospital_id is not None and row.hospital_id not in remote_ids:
                logger.warning(
                    "batch=%s row=%d: hospital %d no longer exists upstream",
                    job.batch_id,
                    row.record.row,
                    row.hospital_id,
                )
                row.mark_missing()

        claimed = {row.hospital_id for row in job.rows if row.hospital_id is not None}
        unclaimed: defaultdict[Fingerprint, deque[Hospital]] = defaultdict(deque)
        for hospital in remote:
            if hospital.id not in claimed:
                unclaimed[fingerprint(hospital.name, hospital.address, hospital.phone)].append(
                    hospital
                )

        adopted = 0
        for row in job.missing_upstream:
            candidates = unclaimed.get(row.record.fingerprint)
            if candidates:
                row.mark_created(candidates.popleft().id)
                adopted += 1

        orphans = [hospital for queue in unclaimed.values() for hospital in queue]
        if orphans:
            await asyncio.gather(*(self._client.delete_hospital(h.id) for h in orphans))

        if adopted or orphans:
            self._broker.publish(job.batch_id, ProgressEvent.ROW_UPDATED)
        logger.info(
            "batch=%s reconciled: %d upstream, %d adopted, %d orphan(s) removed",
            job.batch_id,
            len(remote),
            adopted,
            len(orphans),
        )

    async def _activate(self, job: BatchJob) -> None:
        try:
            activated = await self._client.activate_batch(job.batch_id)
        except HospitalDirectoryError as exc:
            job.status = JobStatus.ACTIVATION_FAILED
            job.error = (
                f"All hospitals were created but activation failed: {exc.detail}. "
                "Resume to retry activation."
            )
            return

        if activated != job.total:
            # Expected on a resumed activation (rows may already be active); worth a trace.
            logger.info("batch=%s activated %d of %d", job.batch_id, activated, job.total)
        for row in job.rows:
            row.status = RowStatus.CREATED_AND_ACTIVATED
        job.batch_activated = True
        job.status = JobStatus.COMPLETED
        job.error = None
