"""Key-value backend protocol.

Defines the interface for backends that provide key-value storage semantics:
get/set/delete, atomic counters, sorted sets, distributed locks, and pub/sub.

Redis is the canonical implementation. Any module exposing these functions
can serve as a KV backend.
"""

from typing import AsyncGenerator, Coroutine, Optional, Protocol, runtime_checkable


@runtime_checkable
class KVBackend(Protocol):
    """Protocol for key-value storage backends.

    Covers all state operations that rely on addressable keys:
    results, events, locks, counters, sorted sets, and pub/sub channels.

    Serialization and key formatting are handled by the operations layer
    above this protocol — backends deal only in raw bytes/strings.
    """

    # -- Connection management ------------------------------------------------

    @staticmethod
    async def close() -> None:
        """Close all connections and release resources."""
        ...

    # -- Key-value operations -------------------------------------------------

    @staticmethod
    def get(key: str) -> Optional[bytes]:
        """Get value for key (sync)."""
        ...

    @staticmethod
    def aget(key: str) -> Coroutine[None, None, Optional[bytes]]:
        """Get value for key (async)."""
        ...

    @staticmethod
    def set(key: str, value: bytes, ttl_seconds: Optional[int] = None) -> bool:
        """Set value for key with optional TTL (sync)."""
        ...

    @staticmethod
    def aset(
        key: str, value: bytes, ttl_seconds: Optional[int] = None
    ) -> Coroutine[None, None, bool]:
        """Set value for key with optional TTL (async)."""
        ...

    @staticmethod
    def delete(key: str) -> int:
        """Delete key (sync). Returns number of keys deleted (0 or 1)."""
        ...

    @staticmethod
    def adelete(key: str) -> Coroutine[None, None, int]:
        """Delete key (async). Returns number of keys deleted (0 or 1)."""
        ...

    # -- Atomic counters ------------------------------------------------------

    @staticmethod
    def incr(key: str) -> int:
        """Atomically increment counter. Returns value after increment."""
        ...

    @staticmethod
    def decr(key: str) -> int:
        """Atomically decrement counter. Returns value after decrement."""
        ...

    # -- Pub/sub --------------------------------------------------------------

    @staticmethod
    def publish(channel: str, message: str) -> None:
        """Publish a message to a channel (sync)."""
        ...

    @staticmethod
    def subscribe(channel: str) -> AsyncGenerator[str, None]:
        """Subscribe to a channel, yielding messages (async generator)."""
        ...

    # -- Distributed locks ----------------------------------------------------

    @staticmethod
    async def acquire_lock(key: str, value: str, ttl_seconds: int) -> bool:
        """Attempt to acquire a lock atomically.

        Args:
            key: Lock key.
            value: Lock holder identifier (for debugging).
            ttl_seconds: Safety-net expiry for dead processes.

        Returns:
            True if acquired, False if already held.
        """
        ...

    @staticmethod
    async def release_lock(key: str) -> int:
        """Release a lock. Returns number of keys deleted (0 or 1)."""
        ...

    # -- Sorted sets ----------------------------------------------------------

    @staticmethod
    def zadd(key: str, mapping: dict[str, float]) -> int:
        """Add members with scores to a sorted set. Returns count of new members."""
        ...

    @staticmethod
    async def zrangebyscore(
        key: str, min_score: float, max_score: float
    ) -> list[bytes]:
        """Get members with scores in [min_score, max_score]."""
        ...

    @staticmethod
    def zrem(key: str, *members: str) -> int:
        """Remove members from a sorted set. Returns count removed."""
        ...

    # -- Key formatting -------------------------------------------------------

    @staticmethod
    def format_key(*args: str) -> str:
        """Join key parts using the backend's separator convention."""
        ...

    # -- Cleanup --------------------------------------------------------------

    @staticmethod
    def clear_keys() -> int:
        """Delete all keys managed by this application. Returns count deleted."""
        ...
