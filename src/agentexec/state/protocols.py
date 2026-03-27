"""Domain protocols for agentexec backend modules.

Each backend (Redis, Kafka) implements these three protocols:
- StateProtocol: KV, counters, locks, pub/sub, sorted sets, serialization
- QueueProtocol: Task queue push/pop/commit/nack
- ActivityProtocol: Task lifecycle tracking (create, update, query)

Backends also implement connection management (close, configure) which
is validated separately by load_backend().
"""

from __future__ import annotations

import uuid
from typing import Any, AsyncGenerator, Coroutine, Optional, Protocol, runtime_checkable

from pydantic import BaseModel


@runtime_checkable
class StateProtocol(Protocol):
    """KV store, counters, locks, pub/sub, sorted sets, serialization."""

    @staticmethod
    def get(key: str) -> Optional[bytes]: ...

    @staticmethod
    def aget(key: str) -> Coroutine[None, None, Optional[bytes]]: ...

    @staticmethod
    def set(key: str, value: bytes, ttl_seconds: Optional[int] = None) -> bool: ...

    @staticmethod
    def aset(
        key: str, value: bytes, ttl_seconds: Optional[int] = None
    ) -> Coroutine[None, None, bool]: ...

    @staticmethod
    def delete(key: str) -> int: ...

    @staticmethod
    def adelete(key: str) -> Coroutine[None, None, int]: ...

    @staticmethod
    def incr(key: str) -> int: ...

    @staticmethod
    def decr(key: str) -> int: ...

    @staticmethod
    def publish(channel: str, message: str) -> None: ...

    @staticmethod
    def subscribe(channel: str) -> AsyncGenerator[str, None]: ...

    @staticmethod
    async def acquire_lock(key: str, value: str, ttl_seconds: int) -> bool: ...

    @staticmethod
    async def release_lock(key: str) -> int: ...

    @staticmethod
    def zadd(key: str, mapping: dict[str, float]) -> int: ...

    @staticmethod
    async def zrangebyscore(
        key: str, min_score: float, max_score: float
    ) -> list[bytes]: ...

    @staticmethod
    def zrem(key: str, *members: str) -> int: ...

    @staticmethod
    def serialize(obj: BaseModel) -> bytes: ...

    @staticmethod
    def deserialize(data: bytes) -> BaseModel: ...

    @staticmethod
    def format_key(*args: str) -> str: ...

    @staticmethod
    def clear_keys() -> int: ...


@runtime_checkable
class QueueProtocol(Protocol):
    """Task queue operations with commit/nack semantics."""

    @staticmethod
    def queue_push(
        queue_name: str,
        value: str,
        *,
        high_priority: bool = False,
        partition_key: str | None = None,
    ) -> None: ...

    @staticmethod
    async def queue_pop(
        queue_name: str,
        *,
        timeout: int = 1,
    ) -> dict[str, Any] | None: ...

    @staticmethod
    async def queue_commit(queue_name: str) -> None: ...

    @staticmethod
    async def queue_nack(queue_name: str) -> None: ...


@runtime_checkable
class ActivityProtocol(Protocol):
    """Task lifecycle tracking — create, update, query."""

    @staticmethod
    def activity_create(
        agent_id: uuid.UUID,
        agent_type: str,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> None: ...

    @staticmethod
    def activity_append_log(
        agent_id: uuid.UUID,
        message: str,
        status: str,
        percentage: int | None = None,
    ) -> None: ...

    @staticmethod
    def activity_get(
        agent_id: uuid.UUID,
        metadata_filter: dict[str, Any] | None = None,
    ) -> Any: ...

    @staticmethod
    def activity_list(
        page: int = 1,
        page_size: int = 50,
        metadata_filter: dict[str, Any] | None = None,
    ) -> tuple[list[Any], int]: ...

    @staticmethod
    def activity_count_active() -> int: ...

    @staticmethod
    def activity_get_pending_ids() -> list[uuid.UUID]: ...
