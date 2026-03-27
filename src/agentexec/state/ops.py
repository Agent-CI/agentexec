"""Operations layer — the bridge between agentexec modules and the backend.

This module provides the high-level operations that queue.py, schedule.py,
tracker.py, and other modules call. It delegates to whichever backend is
configured (Redis or Kafka) via a single module reference.

Callers should never touch backend primitives directly — they go through
this layer, which keeps the rest of the codebase backend-agnostic.

All I/O methods are async. Pure-CPU helpers (serialize, deserialize,
format_key) remain sync.
"""

from __future__ import annotations

import importlib
import uuid
from typing import Any, AsyncGenerator, Optional
from uuid import UUID

from pydantic import BaseModel

from agentexec.config import CONF

# ---------------------------------------------------------------------------
# Backend reference (populated by init())
# ---------------------------------------------------------------------------

_backend: Any = None  # The loaded backend module


def init(backend_module: str) -> None:
    """Initialize the ops layer with the configured backend.

    Called once during application startup (from state/__init__.py).

    Args:
        backend_module: Fully-qualified module path
            (e.g. 'agentexec.state.redis_backend' or
             'agentexec.state.kafka_backend').
    """
    global _backend
    _backend = importlib.import_module(backend_module)


def get_backend():  # type: ignore[no-untyped-def]
    """Get the backend module. Raises if not initialized."""
    if _backend is None:
        raise RuntimeError(
            "State backend not initialized. Set AGENTEXEC_STATE_BACKEND."
        )
    return _backend


def configure(**kwargs: Any) -> None:
    """Pass per-process configuration to the backend.

    Currently used to set worker_id for Kafka client IDs.
    Backends that don't support configure() silently ignore the call.
    """
    b = get_backend()
    if hasattr(b, "configure"):
        b.configure(**kwargs)


async def close() -> None:
    """Close all backend connections."""
    await get_backend().close()


# ---------------------------------------------------------------------------
# Key constants
# ---------------------------------------------------------------------------

KEY_RESULT = (CONF.key_prefix, "result")
KEY_EVENT = (CONF.key_prefix, "event")
KEY_LOCK = (CONF.key_prefix, "lock")
KEY_SCHEDULE = (CONF.key_prefix, "schedule")
KEY_SCHEDULE_QUEUE = (CONF.key_prefix, "schedule_queue")
CHANNEL_LOGS = (CONF.key_prefix, "logs")


# ---------------------------------------------------------------------------
# Helpers (sync — pure CPU)
# ---------------------------------------------------------------------------


def format_key(*args: str) -> str:
    """Format a key using the backend's separator convention."""
    return get_backend().format_key(*args)


def serialize(obj: BaseModel) -> bytes:
    """Serialize a Pydantic BaseModel to bytes with type information."""
    return get_backend().serialize(obj)


def deserialize(data: bytes) -> BaseModel:
    """Deserialize bytes back to a typed Pydantic BaseModel instance."""
    return get_backend().deserialize(data)


# ---------------------------------------------------------------------------
# Queue operations
# ---------------------------------------------------------------------------


async def queue_push(
    queue_name: str,
    value: str,
    *,
    high_priority: bool = False,
    partition_key: str | None = None,
) -> None:
    """Push a serialized task onto the queue."""
    await get_backend().queue_push(
        queue_name, value,
        high_priority=high_priority,
        partition_key=partition_key,
    )


async def queue_pop(
    queue_name: str,
    *,
    timeout: int = 1,
) -> dict[str, Any] | None:
    """Pop the next task from the queue.

    The task is not acknowledged until queue_commit() is called.
    """
    return await get_backend().queue_pop(queue_name, timeout=timeout)


async def queue_commit(queue_name: str) -> None:
    """Acknowledge successful processing of the last task."""
    await get_backend().queue_commit(queue_name)


async def queue_nack(queue_name: str) -> None:
    """Signal that the last task should be retried."""
    await get_backend().queue_nack(queue_name)


# ---------------------------------------------------------------------------
# Result operations
# ---------------------------------------------------------------------------


async def set_result(
    agent_id: UUID | str,
    data: BaseModel,
    ttl_seconds: int | None = None,
) -> None:
    """Store a task result."""
    b = get_backend()
    await b.store_set(
        b.format_key(*KEY_RESULT, str(agent_id)),
        b.serialize(data),
        ttl_seconds=ttl_seconds,
    )


async def get_result(agent_id: UUID | str) -> BaseModel | None:
    """Retrieve a task result."""
    b = get_backend()
    data = await b.store_get(b.format_key(*KEY_RESULT, str(agent_id)))
    return b.deserialize(data) if data else None


async def delete_result(agent_id: UUID | str) -> int:
    """Delete a task result."""
    b = get_backend()
    return await b.store_delete(b.format_key(*KEY_RESULT, str(agent_id)))


# ---------------------------------------------------------------------------
# Event operations (shutdown, ready flags)
# ---------------------------------------------------------------------------


async def set_event(name: str, id: str) -> None:
    """Set an event flag."""
    b = get_backend()
    await b.store_set(b.format_key(*KEY_EVENT, name, id), b"1")


async def clear_event(name: str, id: str) -> None:
    """Clear an event flag."""
    b = get_backend()
    await b.store_delete(b.format_key(*KEY_EVENT, name, id))


async def check_event(name: str, id: str) -> bool:
    """Check if an event flag is set."""
    b = get_backend()
    return await b.store_get(b.format_key(*KEY_EVENT, name, id)) is not None


# ---------------------------------------------------------------------------
# Pub/sub (log streaming)
# ---------------------------------------------------------------------------


def publish_log(message: str) -> None:
    """Publish a log message. Sync — required by Python logging handlers."""
    b = get_backend()
    b.log_publish(b.format_key(*CHANNEL_LOGS), message)


async def subscribe_logs() -> AsyncGenerator[str, None]:
    """Subscribe to log messages."""
    b = get_backend()
    async for msg in b.log_subscribe(b.format_key(*CHANNEL_LOGS)):
        yield msg


# ---------------------------------------------------------------------------
# Lock operations
# ---------------------------------------------------------------------------


async def acquire_lock(lock_key: str, agent_id: str) -> bool:
    """Attempt to acquire a task lock."""
    b = get_backend()
    return await b.acquire_lock(
        b.format_key(*KEY_LOCK, lock_key),
        agent_id,
        CONF.lock_ttl,
    )


async def release_lock(lock_key: str) -> int:
    """Release a task lock."""
    b = get_backend()
    return await b.release_lock(b.format_key(*KEY_LOCK, lock_key))


# ---------------------------------------------------------------------------
# Counter operations (Tracker)
# ---------------------------------------------------------------------------


async def counter_incr(key: str) -> int:
    """Atomically increment a counter."""
    return await get_backend().counter_incr(key)


async def counter_decr(key: str) -> int:
    """Atomically decrement a counter."""
    return await get_backend().counter_decr(key)


async def counter_get(key: str) -> Optional[bytes]:
    """Get current counter value."""
    return await get_backend().store_get(key)


# ---------------------------------------------------------------------------
# Schedule operations
# ---------------------------------------------------------------------------


async def schedule_set(task_name: str, task_data: bytes) -> None:
    """Store a schedule definition."""
    b = get_backend()
    await b.store_set(b.format_key(*KEY_SCHEDULE, task_name), task_data)


async def schedule_get(task_name: str) -> Optional[bytes]:
    """Get a schedule definition."""
    b = get_backend()
    return await b.store_get(b.format_key(*KEY_SCHEDULE, task_name))


async def schedule_delete(task_name: str) -> None:
    """Delete a schedule definition."""
    b = get_backend()
    await b.store_delete(b.format_key(*KEY_SCHEDULE, task_name))


async def schedule_index_add(task_name: str, next_run: float) -> None:
    """Add a task to the schedule index with its next run time."""
    b = get_backend()
    await b.index_add(b.format_key(*KEY_SCHEDULE_QUEUE), {task_name: next_run})


async def schedule_index_due(max_time: float) -> list[str]:
    """Get task names that are due (next_run <= max_time)."""
    b = get_backend()
    raw = await b.index_range(b.format_key(*KEY_SCHEDULE_QUEUE), 0, max_time)
    return [item.decode("utf-8") for item in raw]


async def schedule_index_remove(task_name: str) -> None:
    """Remove a task from the schedule index."""
    b = get_backend()
    await b.index_remove(b.format_key(*KEY_SCHEDULE_QUEUE), task_name)


# ---------------------------------------------------------------------------
# Activity operations
# ---------------------------------------------------------------------------


async def activity_create(
    agent_id: uuid.UUID,
    agent_type: str,
    message: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Create a new activity record with initial QUEUED log entry."""
    await get_backend().activity_create(agent_id, agent_type, message, metadata)


async def activity_append_log(
    agent_id: uuid.UUID,
    message: str,
    status: str,
    percentage: int | None = None,
) -> None:
    """Append a log entry to an existing activity record."""
    await get_backend().activity_append_log(agent_id, message, status, percentage)


async def activity_get(
    agent_id: uuid.UUID,
    metadata_filter: dict[str, Any] | None = None,
) -> Any:
    """Get a single activity record by agent_id."""
    return await get_backend().activity_get(agent_id, metadata_filter)


async def activity_list(
    page: int = 1,
    page_size: int = 50,
    metadata_filter: dict[str, Any] | None = None,
) -> tuple[list[Any], int]:
    """List activity records with pagination. Returns (items, total)."""
    return await get_backend().activity_list(page, page_size, metadata_filter)


async def activity_count_active() -> int:
    """Count activities with QUEUED or RUNNING status."""
    return await get_backend().activity_count_active()


async def activity_get_pending_ids() -> list[uuid.UUID]:
    """Get agent_ids for all activities with QUEUED or RUNNING status."""
    return await get_backend().activity_get_pending_ids()


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


async def clear_keys() -> int:
    """Clear all managed state."""
    return await get_backend().clear_keys()
