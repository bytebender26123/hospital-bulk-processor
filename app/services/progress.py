import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from enum import Enum
from uuid import UUID


class ProgressEvent(str, Enum):
    STARTED = "started"
    ROW_UPDATED = "row_updated"
    FINISHED = "finished"


class ProgressBroker:
    """In-process pub/sub that fans job progress out to WebSocket subscribers.

    Events carry only their type; subscribers read the job's current state when they send,
    so a slow consumer that drops intermediate events still always shows correct totals.
    Each subscriber queue is bounded, and publishing never blocks the processing loop.
    """

    def __init__(self, queue_size: int = 64) -> None:
        self._queue_size = queue_size
        self._subscribers: defaultdict[UUID, set[asyncio.Queue[ProgressEvent]]] = defaultdict(set)

    @asynccontextmanager
    async def subscribe(self, batch_id: UUID) -> AsyncIterator[asyncio.Queue[ProgressEvent]]:
        queue: asyncio.Queue[ProgressEvent] = asyncio.Queue(maxsize=self._queue_size)
        self._subscribers[batch_id].add(queue)
        try:
            yield queue
        finally:
            subscribers = self._subscribers.get(batch_id)
            if subscribers is not None:
                subscribers.discard(queue)
                if not subscribers:
                    del self._subscribers[batch_id]

    def publish(self, batch_id: UUID, event: ProgressEvent) -> None:
        for queue in self._subscribers.get(batch_id, ()):
            if queue.full():
                queue.get_nowait()  # drop the oldest; the next frame carries full state anyway
            queue.put_nowait(event)
