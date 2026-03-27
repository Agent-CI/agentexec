"""Domain protocols for agentexec backend modules.

Each backend (Redis, Kafka) implements these three protocols:
- StateProtocol: KV store, counters, locks, pub/sub, sorted index, serialization
- QueueProtocol: Task queue push/pop/commit/nack
- ActivityProtocol: Task lifecycle tracking (create, update, query)

All I/O methods are async. Pure-CPU helpers (serialize, deserialize,
format_key) remain sync.
"""

from __future__ import annotations

import uuid
from typing import Any, AsyncGenerator, Optional, Protocol, runtime_checkable

from pydantic import BaseModel


@runtime_checkable
class StateProtocol(Protocol):
    """KV store, counters, locks, pub/sub, sorted index, serialization."""

    # -- KV store -------------------------------------------------------------

    @staticmethod
    async def store_get(key: str) -> Optional[bytes]: ...

    @staticmethod
    async def store_set(key: str, value: bytes, ttl_seconds: Optional[int] = None) -> bool: ...

    @staticmethod
    async def store_delete(key: str) -> int: ...

    # -- Counters -------------------------------------------------------------

    @staticmethod
    async def counter_incr(key: str) -> int: ...

    @staticmethod
    async def counter_decr(key: str) -> int: ...

    # -- Pub/sub (log streaming) ----------------------------------------------

    @staticmethod
    async def log_publish(channel: str, message: str) -> None: ...

    @staticmethod
    async def log_subscribe(channel: str) -> AsyncGenerator[str, None]: ...

    # -- Locks ----------------------------------------------------------------

    @staticmethod
    async def acquire_lock(key: str, agent_id: uuid.UUID, ttl_seconds: int) -> bool: ...

    @staticmethod
    async def release_lock(key: str) -> int: ...

    # -- Sorted index (schedule) ----------------------------------------------

    @staticmethod
    async def index_add(key: str, mapping: dict[str, float]) -> int: ...

    @staticmethod
    async def index_range(key: str, min_score: float, max_score: float) -> list[bytes]: ...

    @staticmethod
    async def index_remove(key: str, *members: str) -> int: ...

    # -- Serialization (sync — pure CPU, no I/O) ------------------------------

    @staticmethod
    def serialize(obj: BaseModel) -> bytes: ...

    @staticmethod
    def deserialize(data: bytes) -> BaseModel: ...

    # -- Key formatting (sync — pure string ops) ------------------------------

    @staticmethod
    def format_key(*args: str) -> str: ...

    # -- Cleanup --------------------------------------------------------------

    @staticmethod
    async def clear_keys() -> int: ...


@runtime_checkable
class QueueProtocol(Protocol):
    """Task queue operations with commit/nack semantics."""

    @staticmethod
    async def queue_push(
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



@runtime_checkable
class ActivityProtocol(Protocol):
    """Task lifecycle tracking — create, update, query."""

    @staticmethod
    async def activity_create(
        agent_id: uuid.UUID,
        agent_type: str,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> None: ...

    @staticmethod
    async def activity_append_log(
        agent_id: uuid.UUID,
        message: str,
        status: str,
        percentage: int | None = None,
    ) -> None: ...

    @staticmethod
    async def activity_get(
        agent_id: uuid.UUID,
        metadata_filter: dict[str, Any] | None = None,
    ) -> Any: ...

    @staticmethod
    async def activity_list(
        page: int = 1,
        page_size: int = 50,
        metadata_filter: dict[str, Any] | None = None,
    ) -> tuple[list[Any], int]: ...

    @staticmethod
    async def activity_count_active() -> int: ...

    @staticmethod
    async def activity_get_pending_ids() -> list[uuid.UUID]: ...
