"""Unified backend protocol for agentexec state operations.

Defines the semantic operations that agentexec needs — not Redis primitives,
not Kafka primitives. Each backend (Redis, Kafka) implements these in its
own way.

Pick one backend via AGENTEXEC_STATE_BACKEND:
  - 'agentexec.state.redis_backend'  (default)
  - 'agentexec.state.kafka_backend'
"""

from __future__ import annotations

from types import ModuleType
from typing import Any, AsyncGenerator, Coroutine, Optional, Protocol, runtime_checkable

from pydantic import BaseModel


@runtime_checkable
class StateBackend(Protocol):
    """Protocol for agentexec state backends.

    A backend is a module that exposes these functions. Any module conforming
    to this protocol can serve as the state backend.
    """

    # -- Connection management ------------------------------------------------

    @staticmethod
    async def close() -> None:
        """Close all connections and release resources."""
        ...

    # -- Queue operations -----------------------------------------------------

    @staticmethod
    def queue_push(
        queue_name: str,
        value: str,
        *,
        high_priority: bool = False,
        partition_key: str | None = None,
    ) -> None:
        """Push a serialized task onto the queue.

        Args:
            queue_name: Queue/topic name.
            value: Serialized task JSON string.
            high_priority: Push to front of queue (Redis) or set priority
                header (Kafka). Ignored when ordering is per-partition.
            partition_key: For stream backends, determines the partition.
                Typically the evaluated lock_key (e.g. 'user:42').
                Ignored by KV backends.
        """
        ...

    @staticmethod
    async def queue_pop(
        queue_name: str,
        *,
        timeout: int = 1,
    ) -> dict[str, Any] | None:
        """Pop the next task from the queue.

        Args:
            queue_name: Queue/topic name.
            timeout: Seconds to wait before returning None.

        Returns:
            Parsed task data dict, or None if nothing available.
        """
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

        Stream-based backends (Kafka) may return True unconditionally
        since partition assignment provides natural task isolation.

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
        """Release a lock. Returns number of keys deleted (0 or 1).

        Stream-based backends may no-op (return 0).
        """
        ...

    # -- Sorted sets (schedule index) -----------------------------------------

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

    # -- Serialization --------------------------------------------------------

    @staticmethod
    def serialize(obj: BaseModel) -> bytes:
        """Serialize a Pydantic BaseModel to bytes with type information."""
        ...

    @staticmethod
    def deserialize(data: bytes) -> BaseModel:
        """Deserialize bytes back to a typed Pydantic BaseModel instance."""
        ...

    # -- Key formatting -------------------------------------------------------

    @staticmethod
    def format_key(*args: str) -> str:
        """Join key parts using the backend's separator convention."""
        ...

    # -- Cleanup --------------------------------------------------------------

    @staticmethod
    def clear_keys() -> int:
        """Delete all keys/state managed by this application."""
        ...


def load_backend(module: ModuleType) -> StateBackend:
    """Load and validate a backend module conforms to StateBackend protocol.

    Args:
        module: Backend module to validate.

    Returns:
        The module typed as StateBackend.

    Raises:
        TypeError: If the module is missing required functions.
    """
    # Collect required methods from the Protocol class annotations.
    # __protocol_attrs__ is available in Python 3.12+; fall back to
    # inspecting __annotations__ and dir() for older versions.
    required = getattr(StateBackend, "__protocol_attrs__", None)
    if required is None:
        required = {
            name
            for name in dir(StateBackend)
            if not name.startswith("_") and callable(getattr(StateBackend, name, None))
        }

    missing = [name for name in required if not hasattr(module, name)]
    if missing:
        raise TypeError(
            f"Backend module '{module.__name__}' missing required functions: {missing}"
        )

    return module  # type: ignore[return-value]
