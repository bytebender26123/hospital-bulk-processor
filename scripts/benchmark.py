"""Measure end-to-end bulk import time against the real Hospital Directory API.

Runs the service in-process with different concurrency limits, then rolls every batch
back so the shared upstream isn't left with benchmark data.

    python -m scripts.benchmark samples/hospitals_valid.csv --concurrency 1 5 10 20
"""

import argparse
import asyncio
import time
from pathlib import Path

import httpx

from app.clients.hospital_directory import HospitalDirectoryClient
from app.config import Settings
from app.core.retry import RetryPolicy
from app.core.tasks import BackgroundTaskRegistry
from app.repositories.jobs import InMemoryJobRepository
from app.services.bulk_service import BulkHospitalService
from app.services.csv_parser import parse_hospital_csv
from app.services.progress import ProgressBroker


async def run_once(settings: Settings, content: bytes, concurrency: int) -> tuple[float, str]:
    parsed = parse_hospital_csv(content, max_rows=settings.max_csv_rows)
    if not parsed.is_valid:
        raise SystemExit(f"Invalid CSV: {parsed.errors}")

    async with httpx.AsyncClient(
        base_url=settings.hospital_api_base_url,
        timeout=httpx.Timeout(settings.http_read_timeout_seconds),
        limits=httpx.Limits(max_connections=concurrency),
    ) as http:
        client = HospitalDirectoryClient(
            http, retry_policy=RetryPolicy(), max_concurrency=concurrency
        )
        service = BulkHospitalService(
            client, InMemoryJobRepository(), ProgressBroker(), BackgroundTaskRegistry()
        )
        job = await service.create_job(parsed.records)
        started = time.perf_counter()
        await service.run(job)
        elapsed = time.perf_counter() - started
        await service.rollback(job.batch_id)
        return elapsed, job.status.value


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 5, 10, 20])
    args = parser.parse_args()

    settings = Settings()
    content = args.csv.read_bytes()
    # Wake the upstream first so a Render cold start doesn't skew the first measurement.
    async with httpx.AsyncClient(timeout=90) as http:
        await http.get(f"{settings.hospital_api_base_url}/")

    print(f"{'concurrency':>11} | {'seconds':>7} | status")
    for concurrency in args.concurrency:
        elapsed, status = await run_once(settings, content, concurrency)
        print(f"{concurrency:>11} | {elapsed:>7.2f} | {status}")


if __name__ == "__main__":
    asyncio.run(main())
