from __future__ import annotations
from dataclasses import dataclass
from agentexec.core.redis_client import get_redis, get_redis_sync


@dataclass
class RedisEvent:
    """Event primitive backed by Redis.

    Provides an interface similar to threading.Event/multiprocessing.Event,
    but backed by Redis for cross-process and cross-machine coordination.

    This class is fully picklable (just stores a key string) and works
    across any process that can connect to the same Redis instance.

    set() and clear() are synchronous for use from pool management code.
    is_set() and wait() are async for use from worker event loops.

    Example:
        event = RedisEvent("myapp:shutdown")

        # In pool (sync context)
        event.set()

        # In worker (async context)
        if await event.is_set():
            print("Shutdown signal received")
    """

    key: str

    def set(self) -> None:
        """Set the event flag to True."""
        get_redis_sync().set(self.key, "1")

    def clear(self) -> None:
        """Reset the event flag to False."""
        get_redis_sync().delete(self.key)

    async def is_set(self) -> bool:
        """Check if the event flag is True."""
        redis = get_redis()
        return await redis.get(self.key) is not None
