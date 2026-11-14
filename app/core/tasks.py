import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

logger = logging.getLogger(__name__)


class BackgroundTaskRegistry:
    """Owns fire-and-forget tasks.

    asyncio only keeps weak references to tasks, so an un-referenced task can be garbage
    collected mid-flight. Keeping them here also lets shutdown drain in-flight batches
    instead of abandoning half-created ones.
    """

    def __init__(self) -> None:
        self._tasks: set[asyncio.Task[Any]] = set()

    def spawn(self, coro: Coroutine[Any, Any, Any], *, name: str) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._on_done)
        return task

    def _on_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and (exc := task.exception()) is not None:
            logger.error("Background task %s crashed", task.get_name(), exc_info=exc)

    async def shutdown(self, grace_seconds: float) -> None:
        if not self._tasks:
            return
        logger.info(
            "Waiting up to %.0fs for %d in-flight batch(es)", grace_seconds, len(self._tasks)
        )
        _, pending = await asyncio.wait(set(self._tasks), timeout=grace_seconds)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
