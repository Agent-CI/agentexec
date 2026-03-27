# cspell:ignore rpush lpush brpop RPUSH LPUSH BRPOP
"""Redis implementation of the agentexec state backend.

Provides all state operations via Redis:
- Queue: Redis lists with rpush/lpush/brpop
- KV: Redis strings with optional TTL
- Counters: Redis INCR/DECR
- Pub/sub: Redis pub/sub channels
- Locks: SET NX EX (atomic set-if-not-exists with expiry)
- Sorted sets: Redis ZADD/ZRANGEBYSCORE/ZREM
"""

from __future__ import annotations

import importlib
import json
import uuid
from typing import Any, AsyncGenerator, Coroutine, Optional, TypedDict

import redis
import redis.asyncio
from pydantic import BaseModel

from agentexec.config import CONF

__all__ = [
    "close",
    "queue_push",
    "queue_pop",
    "get",
    "aget",
    "set",
    "aset",
    "delete",
    "adelete",
    "incr",
    "decr",
    "publish",
    "subscribe",
    "acquire_lock",
    "release_lock",
    "zadd",
    "zrangebyscore",
    "zrem",
    "serialize",
    "deserialize",
    "format_key",
    "clear_keys",
]

_redis_client: redis.asyncio.Redis | None = None
_redis_sync_client: redis.Redis | None = None
_pubsub: redis.asyncio.client.PubSub | None = None


# -- Connection management ----------------------------------------------------


def _get_async_client() -> redis.asyncio.Redis:
    """Get async Redis client, initializing lazily if needed."""
    global _redis_client

    if _redis_client is None:
        if CONF.redis_url is None:
            raise ValueError("REDIS_URL must be configured")

        _redis_client = redis.asyncio.Redis.from_url(
            CONF.redis_url,
            max_connections=CONF.redis_pool_size,
            socket_connect_timeout=CONF.redis_pool_timeout,
            decode_responses=False,
        )

    return _redis_client


def _get_sync_client() -> redis.Redis:
    """Get sync Redis client, initializing lazily if needed."""
    global _redis_sync_client

    if _redis_sync_client is None:
        if CONF.redis_url is None:
            raise ValueError("REDIS_URL must be configured")

        _redis_sync_client = redis.Redis.from_url(
            CONF.redis_url,
            max_connections=CONF.redis_pool_size,
            socket_connect_timeout=CONF.redis_pool_timeout,
            decode_responses=False,
        )

    return _redis_sync_client


async def close() -> None:
    """Close all Redis connections and clean up resources."""
    global _redis_client, _redis_sync_client, _pubsub

    if _pubsub is not None:
        await _pubsub.close()
        _pubsub = None

    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None

    if _redis_sync_client is not None:
        _redis_sync_client.close()
        _redis_sync_client = None


# -- Queue operations ---------------------------------------------------------


def queue_push(
    queue_name: str,
    value: str,
    *,
    high_priority: bool = False,
    partition_key: str | None = None,
) -> None:
    """Push a task onto the Redis list queue.

    HIGH priority: rpush (right/front, dequeued first).
    LOW priority: lpush (left/back, dequeued later).
    partition_key is ignored (Redis uses locks for isolation).
    """
    client = _get_sync_client()
    if high_priority:
        client.rpush(queue_name, value)
    else:
        client.lpush(queue_name, value)


async def queue_pop(
    queue_name: str,
    *,
    timeout: int = 1,
) -> dict[str, Any] | None:
    """Pop the next task from the Redis list queue (blocking).

    Note: BRPOP atomically removes the message. There is no way to
    "un-pop" it, so Redis provides at-most-once delivery.
    queue_commit/queue_nack are no-ops for Redis.
    """
    client = _get_async_client()
    result = await client.brpop([queue_name], timeout=timeout)  # type: ignore[misc]
    if result is None:
        return None
    _, value = result
    return json.loads(value.decode("utf-8"))


async def queue_commit(queue_name: str) -> None:
    """No-op for Redis — BRPOP already removed the message."""
    pass


async def queue_nack(queue_name: str) -> None:
    """No-op for Redis — BRPOP already removed the message."""
    pass


# -- Key-value operations -----------------------------------------------------


def get(key: str) -> Optional[bytes]:
    """Get value for key synchronously."""
    client = _get_sync_client()
    return client.get(key)  # type: ignore[return-value]


def aget(key: str) -> Coroutine[None, None, Optional[bytes]]:
    """Get value for key asynchronously."""
    client = _get_async_client()
    return client.get(key)  # type: ignore[return-value]


def set(key: str, value: bytes, ttl_seconds: Optional[int] = None) -> bool:
    """Set value for key synchronously with optional TTL."""
    client = _get_sync_client()
    if ttl_seconds is not None:
        return client.set(key, value, ex=ttl_seconds)  # type: ignore[return-value]
    else:
        return client.set(key, value)  # type: ignore[return-value]


def aset(key: str, value: bytes, ttl_seconds: Optional[int] = None) -> Coroutine[None, None, bool]:
    """Set value for key asynchronously with optional TTL."""
    client = _get_async_client()
    if ttl_seconds is not None:
        return client.set(key, value, ex=ttl_seconds)  # type: ignore[return-value]
    else:
        return client.set(key, value)  # type: ignore[return-value]


def delete(key: str) -> int:
    """Delete key synchronously."""
    client = _get_sync_client()
    return client.delete(key)  # type: ignore[return-value]


def adelete(key: str) -> Coroutine[None, None, int]:
    """Delete key asynchronously."""
    client = _get_async_client()
    return client.delete(key)  # type: ignore[return-value]


# -- Atomic counters ----------------------------------------------------------


def incr(key: str) -> int:
    """Atomically increment counter."""
    client = _get_sync_client()
    return client.incr(key)  # type: ignore[return-value]


def decr(key: str) -> int:
    """Atomically decrement counter."""
    client = _get_sync_client()
    return client.decr(key)  # type: ignore[return-value]


# -- Pub/sub ------------------------------------------------------------------


def publish(channel: str, message: str) -> None:
    """Publish message to a channel."""
    client = _get_sync_client()
    client.publish(channel, message)


async def subscribe(channel: str) -> AsyncGenerator[str, None]:
    """Subscribe to a channel and yield messages."""
    global _pubsub

    client = _get_async_client()
    _pubsub = client.pubsub()
    await _pubsub.subscribe(channel)

    try:
        async for message in _pubsub.listen():
            if message["type"] == "message":
                data = message["data"]
                if isinstance(data, bytes):
                    yield data.decode("utf-8")
                else:
                    yield data
    finally:
        await _pubsub.unsubscribe(channel)
        await _pubsub.close()
        _pubsub = None


# -- Distributed locks --------------------------------------------------------


async def acquire_lock(key: str, value: str, ttl_seconds: int) -> bool:
    """Attempt to acquire a distributed lock using SET NX EX."""
    client = _get_async_client()
    result = await client.set(key, value, nx=True, ex=ttl_seconds)
    return result is not None


async def release_lock(key: str) -> int:
    """Release a distributed lock."""
    client = _get_async_client()
    return await client.delete(key)  # type: ignore[return-value]


# -- Sorted sets --------------------------------------------------------------


def zadd(key: str, mapping: dict[str, float]) -> int:
    """Add members to a sorted set with scores."""
    client = _get_sync_client()
    return client.zadd(key, mapping)  # type: ignore[return-value]


async def zrangebyscore(
    key: str, min_score: float, max_score: float
) -> list[bytes]:
    """Get members with scores between min and max."""
    client = _get_async_client()
    return await client.zrangebyscore(key, min_score, max_score)  # type: ignore[return-value]


def zrem(key: str, *members: str) -> int:
    """Remove members from a sorted set."""
    client = _get_sync_client()
    return client.zrem(key, *members)  # type: ignore[return-value]


# -- Activity operations (SQLAlchemy / Postgres) ------------------------------


def activity_create(
    agent_id: uuid.UUID,
    agent_type: str,
    message: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Create a new activity record with initial QUEUED log entry."""
    from agentexec.activity.models import Activity, ActivityLog, Status
    from agentexec.core.db import get_global_session

    db = get_global_session()
    activity_record = Activity(
        agent_id=agent_id,
        agent_type=agent_type,
        metadata_=metadata,
    )
    db.add(activity_record)
    db.flush()

    log = ActivityLog(
        activity_id=activity_record.id,
        message=message,
        status=Status.QUEUED,
        percentage=0,
    )
    db.add(log)
    db.commit()


def activity_append_log(
    agent_id: uuid.UUID,
    message: str,
    status: str,
    percentage: int | None = None,
) -> None:
    """Append a log entry to an existing activity record."""
    from agentexec.activity.models import Activity, Status as ActivityStatus
    from agentexec.core.db import get_global_session

    db = get_global_session()
    Activity.append_log(
        session=db,
        agent_id=agent_id,
        message=message,
        status=ActivityStatus(status),
        percentage=percentage,
    )


def activity_get(
    agent_id: uuid.UUID,
    metadata_filter: dict[str, Any] | None = None,
) -> Any:
    """Get a single activity record by agent_id.

    Returns an Activity ORM object (compatible with ActivityDetailSchema
    via from_attributes=True), or None if not found.
    """
    from agentexec.activity.models import Activity
    from agentexec.core.db import get_global_session

    db = get_global_session()
    return Activity.get_by_agent_id(db, agent_id, metadata_filter=metadata_filter)


def activity_list(
    page: int = 1,
    page_size: int = 50,
    metadata_filter: dict[str, Any] | None = None,
) -> tuple[list[Any], int]:
    """List activity records with pagination.

    Returns (rows, total) where rows are RowMapping objects compatible
    with ActivityListItemSchema via from_attributes=True.
    """
    from agentexec.activity.models import Activity
    from agentexec.core.db import get_global_session

    db = get_global_session()

    query = db.query(Activity)
    if metadata_filter:
        for key, value in metadata_filter.items():
            query = query.filter(Activity.metadata_[key].as_string() == str(value))
    total = query.count()

    rows = Activity.get_list(
        db, page=page, page_size=page_size, metadata_filter=metadata_filter,
    )
    return rows, total


def activity_count_active() -> int:
    """Count activities with QUEUED or RUNNING status."""
    from agentexec.activity.models import Activity
    from agentexec.core.db import get_global_session

    db = get_global_session()
    return Activity.get_active_count(db)


def activity_get_pending_ids() -> list[uuid.UUID]:
    """Get agent_ids for all activities with QUEUED or RUNNING status."""
    from agentexec.activity.models import Activity
    from agentexec.core.db import get_global_session

    db = get_global_session()
    return Activity.get_pending_ids(db)


# -- Serialization ------------------------------------------------------------


class _SerializeWrapper(TypedDict):
    __class__: str
    __data__: str


def serialize(obj: BaseModel) -> bytes:
    """Serialize a Pydantic BaseModel to JSON bytes with type information."""
    if not isinstance(obj, BaseModel):
        raise TypeError(f"Expected BaseModel, got {type(obj)}")

    cls = type(obj)
    wrapper: _SerializeWrapper = {
        "__class__": f"{cls.__module__}.{cls.__qualname__}",
        "__data__": obj.model_dump_json(),
    }
    return json.dumps(wrapper).encode("utf-8")


def deserialize(data: bytes) -> BaseModel:
    """Deserialize JSON bytes back to a typed Pydantic BaseModel instance."""
    wrapper: _SerializeWrapper = json.loads(data.decode("utf-8"))
    class_path = wrapper["__class__"]
    json_data = wrapper["__data__"]

    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)

    result: BaseModel = cls.model_validate_json(json_data)
    return result


# -- Key formatting -----------------------------------------------------------


def format_key(*args: str) -> str:
    """Format a Redis key by joining parts with colons."""
    return ":".join(args)


# -- Cleanup ------------------------------------------------------------------


def clear_keys() -> int:
    """Clear all Redis keys managed by this application."""
    if CONF.redis_url is None:
        return 0

    client = _get_sync_client()
    deleted = 0

    deleted += client.delete(CONF.queue_name)

    pattern = f"{CONF.key_prefix}:*"
    cursor = 0

    while True:
        cursor, keys = client.scan(cursor=cursor, match=pattern, count=100)
        if keys:
            deleted += client.delete(*keys)
        if cursor == 0:
            break

    return deleted
