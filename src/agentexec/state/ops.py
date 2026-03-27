"""Operations layer — the bridge between agentexec modules and backends.

This module provides high-level operations (enqueue, dequeue, store result,
publish log, etc.) that are backend-agnostic. Each operation delegates to
either a KV backend (Redis) or a stream backend (Kafka) depending on config.

Modules like queue.py, schedule.py, and tracker.py call into this layer
instead of touching backend primitives directly.
"""

from __future__ import annotations

import importlib
import json
from typing import Any, AsyncGenerator, Coroutine, Optional
from uuid import UUID

from pydantic import BaseModel

from agentexec.config import CONF


# ---------------------------------------------------------------------------
# Serialization helpers (shared across both backend types)
# ---------------------------------------------------------------------------


def serialize(obj: BaseModel) -> bytes:
    """Serialize a Pydantic BaseModel to JSON bytes with type information."""
    if not isinstance(obj, BaseModel):
        raise TypeError(f"Expected BaseModel, got {type(obj)}")

    cls = type(obj)
    wrapper = {
        "__class__": f"{cls.__module__}.{cls.__qualname__}",
        "__data__": obj.model_dump_json(),
    }
    return json.dumps(wrapper).encode("utf-8")


def deserialize(data: bytes) -> BaseModel:
    """Deserialize JSON bytes back to a typed Pydantic BaseModel instance."""
    wrapper = json.loads(data.decode("utf-8"))
    class_path = wrapper["__class__"]
    json_data = wrapper["__data__"]

    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)

    result: BaseModel = cls.model_validate_json(json_data)
    return result


def format_key(*args: str) -> str:
    """Format a key by joining parts with the configured separator.

    This is a convenience that delegates to the KV backend's convention,
    or uses ':' as the default separator.
    """
    if _kv is not None:
        return _kv.format_key(*args)
    return ":".join(args)


# ---------------------------------------------------------------------------
# Backend references (populated by init())
# ---------------------------------------------------------------------------

_kv: Any = None  # KVBackend module or None
_stream: Any = None  # StreamBackend module or None


def init(
    *,
    kv_backend: str | None = None,
    stream_backend: str | None = None,
) -> None:
    """Initialize the operations layer with the configured backends.

    Called once during application startup (from state/__init__.py).

    Args:
        kv_backend: Fully-qualified module path for the KV backend
            (e.g. 'agentexec.state.redis_kv_backend'). None to skip.
        stream_backend: Fully-qualified module path for the stream backend
            (e.g. 'agentexec.state.kafka_stream_backend'). None to skip.
    """
    global _kv, _stream

    if kv_backend:
        _kv = importlib.import_module(kv_backend)
    if stream_backend:
        _stream = importlib.import_module(stream_backend)


def get_kv():  # type: ignore[no-untyped-def]
    """Get the KV backend module. Raises if not configured."""
    if _kv is None:
        raise RuntimeError(
            "No KV backend configured. Set AGENTEXEC_KV_BACKEND or "
            "AGENTEXEC_STATE_BACKEND in your environment."
        )
    return _kv


def get_stream():  # type: ignore[no-untyped-def]
    """Get the stream backend module. Raises if not configured."""
    if _stream is None:
        raise RuntimeError(
            "No stream backend configured. Set AGENTEXEC_STREAM_BACKEND "
            "in your environment."
        )
    return _stream


def has_kv() -> bool:
    """Check if a KV backend is configured."""
    return _kv is not None


def has_stream() -> bool:
    """Check if a stream backend is configured."""
    return _stream is not None


async def close() -> None:
    """Close all backend connections."""
    if _kv is not None:
        await _kv.close()
    if _stream is not None:
        await _stream.close()


# ---------------------------------------------------------------------------
# Queue operations
# ---------------------------------------------------------------------------
# With a KV backend (Redis): uses rpush/lpush/brpop on a list.
# With a stream backend (Kafka): produces/consumes on a task topic.
#   The partition key is derived from the task's lock_key (if set),
#   giving natural per-key ordering and eliminating distributed locks.
# ---------------------------------------------------------------------------


def queue_push(
    queue_name: str,
    value: str,
    *,
    high_priority: bool = False,
    partition_key: str | None = None,
) -> None:
    """Push a task onto the queue.

    Args:
        queue_name: Queue/topic name.
        value: Serialized task JSON.
        high_priority: If True and using KV backend, push to front.
            Ignored for stream backends (ordering is per-partition).
        partition_key: For stream backends, determines the partition.
            Typically the evaluated lock_key (e.g. 'user:42').
    """
    if has_stream():
        import asyncio

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(
                get_stream().produce(
                    _topic_name(queue_name),
                    value.encode("utf-8"),
                    key=partition_key,
                )
            )
        except RuntimeError:
            asyncio.run(
                get_stream().produce(
                    _topic_name(queue_name),
                    value.encode("utf-8"),
                    key=partition_key,
                )
            )
    else:
        kv = get_kv()
        if high_priority:
            kv.rpush(queue_name, value)
        else:
            kv.lpush(queue_name, value)


async def queue_pop(
    queue_name: str,
    *,
    group_id: str | None = None,
    timeout: int = 1,
) -> dict[str, Any] | None:
    """Pop the next task from the queue.

    Args:
        queue_name: Queue/topic name.
        group_id: Consumer group (stream backend only).
        timeout: Timeout in seconds (KV) or milliseconds conversion (stream).

    Returns:
        Parsed task dict, or None if nothing available.
    """
    if has_stream():
        stream = get_stream()
        gid = group_id or f"{CONF.key_prefix}-workers"
        async for record in stream.consume(
            _topic_name(queue_name), gid, timeout_ms=timeout * 1000
        ):
            return json.loads(record.value)
        return None
    else:
        kv = get_kv()
        result = await kv.brpop(queue_name, timeout=timeout)
        if result is None:
            return None
        _, task_data = result
        return json.loads(task_data)


# ---------------------------------------------------------------------------
# Result operations
# ---------------------------------------------------------------------------


def set_result(
    agent_id: UUID | str,
    data: BaseModel,
    ttl_seconds: int | None = None,
) -> None:
    """Store a task result.

    KV backend: stores as a key with optional TTL.
    Stream backend: produces to a compacted results topic keyed by agent_id.
    """
    key = _result_key(str(agent_id))
    payload = serialize(data)

    if has_stream():
        import asyncio

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(get_stream().put(_results_topic(), key, payload))
        except RuntimeError:
            asyncio.run(get_stream().put(_results_topic(), key, payload))
    else:
        get_kv().set(
            format_key(*_KEY_RESULT, str(agent_id)),
            payload,
            ttl_seconds=ttl_seconds,
        )


async def aget_result(agent_id: UUID | str) -> BaseModel | None:
    """Retrieve a task result (async).

    KV backend: gets from key-value store.
    Stream backend: reads from compacted results topic by key.
    """
    if has_stream():
        # For stream backends, results are retrieved by consuming the
        # compacted results topic. The caller (results.py) polls this.
        stream = get_stream()
        async for record in stream.consume(
            _results_topic(),
            group_id=f"{CONF.key_prefix}-result-{agent_id}",
            timeout_ms=500,
        ):
            if record.key == str(agent_id):
                return deserialize(record.value)
        return None
    else:
        data = await get_kv().aget(format_key(*_KEY_RESULT, str(agent_id)))
        return deserialize(data) if data else None


def get_result(agent_id: UUID | str) -> BaseModel | None:
    """Retrieve a task result (sync). KV backend only."""
    kv = get_kv()
    data = kv.get(format_key(*_KEY_RESULT, str(agent_id)))
    return deserialize(data) if data else None


async def aset_result(
    agent_id: UUID | str,
    data: BaseModel,
    ttl_seconds: int | None = None,
) -> None:
    """Store a task result (async)."""
    payload = serialize(data)

    if has_stream():
        await get_stream().put(_results_topic(), str(agent_id), payload)
    else:
        await get_kv().aset(
            format_key(*_KEY_RESULT, str(agent_id)),
            payload,
            ttl_seconds=ttl_seconds,
        )


async def adelete_result(agent_id: UUID | str) -> None:
    """Delete a task result."""
    if has_stream():
        await get_stream().tombstone(_results_topic(), str(agent_id))
    else:
        await get_kv().adelete(format_key(*_KEY_RESULT, str(agent_id)))


def delete_result(agent_id: UUID | str) -> int:
    """Delete a task result (sync). KV backend only."""
    return get_kv().delete(format_key(*_KEY_RESULT, str(agent_id)))


# ---------------------------------------------------------------------------
# Event operations (shutdown, ready flags)
# ---------------------------------------------------------------------------


def set_event(name: str, id: str) -> None:
    """Set an event flag.

    KV backend: sets a key.
    Stream backend: produces to a compacted events topic.
    """
    if has_stream():
        import asyncio

        key = f"{name}:{id}"
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(get_stream().put(_events_topic(), key, b"1"))
        except RuntimeError:
            asyncio.run(get_stream().put(_events_topic(), key, b"1"))
    else:
        get_kv().set(format_key(*_KEY_EVENT, name, id), b"1")


def clear_event(name: str, id: str) -> None:
    """Clear an event flag."""
    if has_stream():
        import asyncio

        key = f"{name}:{id}"
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(get_stream().tombstone(_events_topic(), key))
        except RuntimeError:
            asyncio.run(get_stream().tombstone(_events_topic(), key))
    else:
        get_kv().delete(format_key(*_KEY_EVENT, name, id))


def check_event(name: str, id: str) -> bool:
    """Check if an event flag is set (sync). KV backend only."""
    return get_kv().get(format_key(*_KEY_EVENT, name, id)) is not None


async def acheck_event(name: str, id: str) -> bool:
    """Check if an event flag is set (async)."""
    if has_stream():
        # For stream backends, consume events topic looking for our key
        stream = get_stream()
        key = f"{name}:{id}"
        async for record in stream.consume(
            _events_topic(),
            group_id=f"{CONF.key_prefix}-event-check-{key}",
            timeout_ms=200,
        ):
            if record.key == key and record.value:
                return True
        return False
    else:
        return await get_kv().aget(format_key(*_KEY_EVENT, name, id)) is not None


# ---------------------------------------------------------------------------
# Pub/sub (log streaming)
# ---------------------------------------------------------------------------


def publish_log(message: str) -> None:
    """Publish a log message.

    KV backend: publishes to a channel.
    Stream backend: produces to a logs topic.
    """
    if has_stream():
        get_stream().produce_sync(
            _logs_topic(),
            message.encode("utf-8"),
        )
    else:
        get_kv().publish(format_key(*_CHANNEL_LOGS), message)


async def subscribe_logs() -> AsyncGenerator[str, None]:
    """Subscribe to log messages.

    KV backend: subscribes to a channel.
    Stream backend: consumes from a logs topic.
    """
    if has_stream():
        stream = get_stream()
        async for record in stream.consume(
            _logs_topic(),
            group_id=f"{CONF.key_prefix}-log-collector",
        ):
            yield record.value.decode("utf-8")
    else:
        async for msg in get_kv().subscribe(format_key(*_CHANNEL_LOGS)):
            yield msg


# ---------------------------------------------------------------------------
# Lock operations
# ---------------------------------------------------------------------------
# With a stream backend, locks are unnecessary — partition assignment
# provides natural serialization. These operations become no-ops.
# ---------------------------------------------------------------------------


async def acquire_lock(lock_key: str, agent_id: str) -> bool:
    """Attempt to acquire a task lock.

    Stream backend: always returns True (partitioning handles isolation).
    KV backend: uses distributed lock with TTL safety net.
    """
    if has_stream():
        # Kafka partitioning guarantees one consumer per partition —
        # no explicit locking needed.
        return True
    else:
        return await get_kv().acquire_lock(
            format_key(*_KEY_LOCK, lock_key),
            agent_id,
            CONF.lock_ttl,
        )


async def release_lock(lock_key: str) -> int:
    """Release a task lock.

    Stream backend: no-op (returns 0).
    KV backend: deletes the lock key.
    """
    if has_stream():
        return 0
    else:
        return await get_kv().release_lock(
            format_key(*_KEY_LOCK, lock_key),
        )


# ---------------------------------------------------------------------------
# Counter operations (Tracker)
# ---------------------------------------------------------------------------


def counter_incr(key: str) -> int:
    """Atomically increment a counter."""
    return get_kv().incr(key)


def counter_decr(key: str) -> int:
    """Atomically decrement a counter."""
    return get_kv().decr(key)


def counter_get(key: str) -> Optional[bytes]:
    """Get current counter value."""
    return get_kv().get(key)


# ---------------------------------------------------------------------------
# Schedule operations (sorted set index)
# ---------------------------------------------------------------------------


def schedule_set(task_name: str, task_data: bytes) -> None:
    """Store a schedule definition.

    KV backend: stores as a key.
    Stream backend: produces to a compacted schedules topic.
    """
    if has_stream():
        import asyncio

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(
                get_stream().put(_schedules_topic(), task_name, task_data)
            )
        except RuntimeError:
            asyncio.run(
                get_stream().put(_schedules_topic(), task_name, task_data)
            )
    else:
        get_kv().set(format_key(*_KEY_SCHEDULE, task_name), task_data)


def schedule_get(task_name: str) -> Optional[bytes]:
    """Get a schedule definition (sync). KV backend only."""
    return get_kv().get(format_key(*_KEY_SCHEDULE, task_name))


def schedule_delete(task_name: str) -> None:
    """Delete a schedule definition."""
    if has_stream():
        import asyncio

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(get_stream().tombstone(_schedules_topic(), task_name))
        except RuntimeError:
            asyncio.run(get_stream().tombstone(_schedules_topic(), task_name))
    else:
        get_kv().delete(format_key(*_KEY_SCHEDULE, task_name))


def schedule_index_add(task_name: str, next_run: float) -> None:
    """Add a task to the schedule index with its next run time.

    KV backend: adds to a sorted set.
    Stream backend: schedule index is managed in-memory by the scheduler
        process, rebuilt from the schedules topic on startup. This is a no-op.
    """
    if has_stream():
        pass  # Index maintained in-memory
    else:
        get_kv().zadd(format_key(*_KEY_SCHEDULE_QUEUE), {task_name: next_run})


async def schedule_index_due(max_time: float) -> list[str]:
    """Get task names that are due (next_run <= max_time).

    KV backend: queries the sorted set.
    Stream backend: not used (scheduler manages in-memory).
    """
    if has_stream():
        return []  # Scheduler manages its own in-memory index
    else:
        raw = await get_kv().zrangebyscore(
            format_key(*_KEY_SCHEDULE_QUEUE), 0, max_time
        )
        return [item.decode("utf-8") for item in raw]


def schedule_index_remove(task_name: str) -> None:
    """Remove a task from the schedule index."""
    if has_stream():
        pass  # Index maintained in-memory
    else:
        get_kv().zrem(format_key(*_KEY_SCHEDULE_QUEUE), task_name)


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def clear_keys() -> int:
    """Clear all managed state.

    KV backend: scans and deletes matching keys.
    Stream backend: topic cleanup is handled externally (retention policies).
    """
    if has_kv():
        return get_kv().clear_keys()
    return 0


# ---------------------------------------------------------------------------
# Internal key/topic helpers
# ---------------------------------------------------------------------------

_KEY_RESULT = (CONF.key_prefix, "result")
_KEY_EVENT = (CONF.key_prefix, "event")
_KEY_LOCK = (CONF.key_prefix, "lock")
_KEY_SCHEDULE = (CONF.key_prefix, "schedule")
_KEY_SCHEDULE_QUEUE = (CONF.key_prefix, "schedule_queue")
_CHANNEL_LOGS = (CONF.key_prefix, "logs")


def _topic_name(base: str) -> str:
    """Build a Kafka topic name from a base name."""
    return f"{CONF.key_prefix}.{base}"


def _results_topic() -> str:
    return f"{CONF.key_prefix}.results"


def _events_topic() -> str:
    return f"{CONF.key_prefix}.events"


def _logs_topic() -> str:
    return f"{CONF.key_prefix}.logs"


def _schedules_topic() -> str:
    return f"{CONF.key_prefix}.schedules"
