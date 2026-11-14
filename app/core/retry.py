import random
from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with full jitter.

    Jitter matters here: a batch fans out up to `max_concurrency` requests at once, and if
    the upstream hiccups they all fail together. Without jitter they would all retry at the
    same instant and hit it again in lockstep.
    """

    max_attempts: int = 3
    base_delay: float = 0.5
    max_delay: float = 8.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

    def delay(self, attempt: int, *, retry_after: float | None = None) -> float:
        """Seconds to wait after the given (1-based) failed attempt."""
        if retry_after is not None:
            return min(max(retry_after, 0.0), self.max_delay)
        ceiling = min(self.max_delay, self.base_delay * 2 ** (attempt - 1))
        return random.uniform(0, ceiling)
