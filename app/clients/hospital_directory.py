"""Async client for the Hospital Directory API.

Retry rules are driven by one question: *could the failed request have been applied?*

* Never reached the server (connect error / pool timeout), or the server explicitly
  refused it (429, 503)  -> safe to retry any call.
* Might have been applied (read timeout, 500/502/504)   -> retry only idempotent calls.
  ``POST /hospitals/`` is not idempotent, so those failures are surfaced to the caller,
  which reconciles against the batch listing before trying again (see BulkHospitalService).
* Any other 4xx -> permanent, never retried.
"""

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel, TypeAdapter, ValidationError

from app.core.exceptions import AppError
from app.core.retry import RetryPolicy
from app.domain.models import HospitalRecord

logger = logging.getLogger(__name__)

_REFUSED_STATUSES = frozenset({429, 503})
_AMBIGUOUS_STATUSES = frozenset({408, 500, 502, 504})
_NOT_SENT_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)

T = TypeVar("T")


class Hospital(BaseModel):
    id: int
    name: str
    address: str
    phone: str | None = None
    creation_batch_id: uuid.UUID | None = None
    active: bool = True


_HOSPITAL_LIST = TypeAdapter(list[Hospital])


class HospitalDirectoryError(AppError):
    status_code = 502  # surfaced to our callers as Bad Gateway

    def __init__(
        self, detail: str, *, upstream_status: int | None = None, retryable: bool = False
    ) -> None:
        super().__init__(detail)
        self.upstream_status = upstream_status
        self.retryable = retryable


class HospitalDirectoryClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        retry_policy: RetryPolicy,
        max_concurrency: int,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    ) -> None:
        self._http = http
        self._retry = retry_policy
        # Shared by every job: N concurrent uploads must not multiply load on the upstream.
        self._limiter = asyncio.Semaphore(max_concurrency)
        self._sleep = sleep

    async def create_hospital(self, record: HospitalRecord, batch_id: uuid.UUID) -> Hospital:
        payload = {
            "name": record.name,
            "address": record.address,
            "phone": record.phone,
            "creation_batch_id": str(batch_id),
        }
        response = await self._request("POST", "/hospitals/", idempotent=False, json=payload)
        return self._parse(response, Hospital.model_validate)

    async def list_batch(self, batch_id: uuid.UUID) -> list[Hospital]:
        response = await self._request(
            "GET", f"/hospitals/batch/{batch_id}", idempotent=True, allow_404=True
        )
        if response.status_code == 404:
            return []
        return self._parse(response, _HOSPITAL_LIST.validate_python)

    async def activate_batch(self, batch_id: uuid.UUID) -> int:
        response = await self._request(
            "PATCH", f"/hospitals/batch/{batch_id}/activate", idempotent=True
        )
        return self._count(response, "activated_count")

    async def delete_batch(self, batch_id: uuid.UUID) -> int:
        response = await self._request(
            "DELETE", f"/hospitals/batch/{batch_id}", idempotent=True, allow_404=True
        )
        return 0 if response.status_code == 404 else self._count(response, "deleted_count")

    async def delete_hospital(self, hospital_id: int) -> None:
        await self._request("DELETE", f"/hospitals/{hospital_id}", idempotent=True, allow_404=True)

    async def _request(
        self,
        method: str,
        url: str,
        *,
        idempotent: bool,
        allow_404: bool = False,
        **kwargs: Any,
    ) -> httpx.Response:
        max_attempts = self._retry.max_attempts
        for attempt in range(1, max_attempts + 1):
            retry_after: float | None = None
            try:
                async with self._limiter:
                    response = await self._http.request(method, url, **kwargs)
            except httpx.TransportError as exc:
                can_retry = idempotent or isinstance(exc, _NOT_SENT_ERRORS)
                failure = HospitalDirectoryError(
                    f"{method} {url} failed: {type(exc).__name__}", retryable=True
                )
            else:
                status = response.status_code
                if status < 400 or (allow_404 and status == 404):
                    return response
                transient = status in _REFUSED_STATUSES or status in _AMBIGUOUS_STATUSES
                failure = HospitalDirectoryError(
                    f"{method} {url} returned HTTP {status}: {_error_detail(response)}",
                    upstream_status=status,
                    retryable=transient,
                )
                if not transient:
                    raise failure
                can_retry = idempotent or status in _REFUSED_STATUSES
                retry_after = _retry_after_seconds(response)

            if not can_retry or attempt == max_attempts:
                raise failure
            delay = self._retry.delay(attempt, retry_after=retry_after)
            logger.warning(
                "%s (attempt %d/%d), retrying in %.2fs",
                failure.detail,
                attempt,
                max_attempts,
                delay,
            )
            await self._sleep(delay)

        raise AssertionError("unreachable")  # pragma: no cover

    @staticmethod
    def _parse(response: httpx.Response, validate: Callable[[Any], T]) -> T:
        try:
            return validate(response.json())
        except (ValueError, ValidationError) as exc:
            raise HospitalDirectoryError(
                f"Unexpected response from {response.request.method} {response.request.url.path}"
            ) from exc

    @staticmethod
    def _count(response: httpx.Response, key: str) -> int:
        try:
            body = response.json()
        except ValueError:
            return 0
        return int(body.get(key, 0)) if isinstance(body, dict) else 0


def _error_detail(response: httpx.Response) -> str:
    try:
        detail = response.json().get("detail", response.text)
    except (ValueError, AttributeError):
        detail = response.text
    return str(detail)[:300]


def _retry_after_seconds(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None  # HTTP-date form; fall back to our own backoff
