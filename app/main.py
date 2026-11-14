import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from app import __version__
from app.api.errors import register_exception_handlers
from app.api.routes import bulk, health
from app.clients.hospital_directory import HospitalDirectoryClient
from app.config import Settings, get_settings
from app.core.logging import configure_logging
from app.core.retry import RetryPolicy
from app.core.tasks import BackgroundTaskRegistry
from app.repositories.jobs import InMemoryJobRepository
from app.services.bulk_service import BulkHospitalService
from app.services.progress import ProgressBroker

logger = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    upstream_transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    """Application factory. ``upstream_transport`` lets callers swap the network layer."""
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # One pooled client for the process: keep-alive connections are reused across every
        # row of every batch instead of paying a TLS handshake per hospital.
        async with httpx.AsyncClient(
            base_url=settings.hospital_api_base_url,
            transport=upstream_transport,
            timeout=httpx.Timeout(
                settings.http_read_timeout_seconds, connect=settings.http_connect_timeout_seconds
            ),
            limits=httpx.Limits(
                max_connections=settings.max_concurrency,
                max_keepalive_connections=settings.max_concurrency,
            ),
            headers={"User-Agent": f"hospital-bulk-processor/{__version__}"},
        ) as http:
            client = HospitalDirectoryClient(
                http,
                retry_policy=RetryPolicy(
                    max_attempts=settings.max_retries,
                    base_delay=settings.retry_backoff_base_seconds,
                    max_delay=settings.retry_backoff_max_seconds,
                ),
                max_concurrency=settings.max_concurrency,
            )
            tasks = BackgroundTaskRegistry()
            broker = ProgressBroker()
            app.state.progress_broker = broker
            app.state.bulk_service = BulkHospitalService(
                client,
                InMemoryJobRepository(max_jobs=settings.job_store_max_jobs),
                broker,
                tasks,
                auto_retry_passes=settings.auto_retry_passes,
                retry_pass_delay_seconds=settings.retry_pass_delay_seconds,
            )
            logger.info("Started; upstream=%s", settings.hospital_api_base_url)
            yield
            await tasks.shutdown(settings.shutdown_grace_seconds)

    app = FastAPI(
        title=settings.app_name,
        version=__version__,
        description=(
            "Bulk CSV import on top of the Hospital Directory API. Each upload becomes one "
            "batch: hospitals are created inactive, and the batch is activated only once "
            "every row exists upstream."
        ),
        lifespan=lifespan,
    )
    app.state.settings = settings
    register_exception_handlers(app)
    app.include_router(health.router)
    app.include_router(bulk.router)
    return app


app = create_app()
