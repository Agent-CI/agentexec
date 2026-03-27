# cspell:ignore rpush lpush brpop RPUSH LPUSH BRPOP
"""Redis state operations: KV, counters, pub/sub, locks, sorted sets, serialization."""

from __future__ import annotations

import importlib
import json
from typing import Any, AsyncGenerator, Coroutine, Optional, TypedDict

from pydantic import BaseModel

from agentexec.config import CONF
from agentexec.state.redis_backend.connection import (
    get_async_client,
    get_pubsub,
    get_sync_client,
    set_pubsub,
)


# -- Key-value operations -----------------------------------------------------


def get(key: str) -> Optional[bytes]:
    """Get value for key synchronously."""
    client = get_sync_client()
    return client.get(key)  # type: ignore[return-value]


def aget(key: str) -> Coroutine[None, None, Optional[bytes]]:
    """Get value for key asynchronously."""
    client = get_async_client()
    return client.get(key)  # type: ignore[return-value]


def set(key: str, value: bytes, ttl_seconds: Optional[int] = None) -> bool:
    """Set value for key synchronously with optional TTL."""
    client = get_sync_client()
    if ttl_seconds is not None:
        return client.set(key, value, ex=ttl_seconds)  # type: ignore[return-value]
    else:
        return client.set(key, value)  # type: ignore[return-value]


def aset(key: str, value: bytes, ttl_seconds: Optional[int] = None) -> Coroutine[None, None, bool]:
    """Set value for key asynchronously with optional TTL."""
    client = get_async_client()
    if ttl_seconds is not None:
        return client.set(key, value, ex=ttl_seconds)  # type: ignore[return-value]
    else:
        return client.set(key, value)  # type: ignore[return-value]


def delete(key: str) -> int:
    """Delete key synchronously."""
    client = get_sync_client()
    return client.delete(key)  # type: ignore[return-value]


def adelete(key: str) -> Coroutine[None, None, int]:
    """Delete key asynchronously."""
    client = get_async_client()
    return client.delete(key)  # type: ignore[return-value]


# -- Atomic counters ----------------------------------------------------------


def incr(key: str) -> int:
    """Atomically increment counter."""
    client = get_sync_client()
    return client.incr(key)  # type: ignore[return-value]


def decr(key: str) -> int:
    """Atomically decrement counter."""
    client = get_sync_client()
    return client.decr(key)  # type: ignore[return-value]


# -- Pub/sub ------------------------------------------------------------------


def publish(channel: str, message: str) -> None:
    """Publish message to a channel."""
    client = get_sync_client()
    client.publish(channel, message)


async def subscribe(channel: str) -> AsyncGenerator[str, None]:
    """Subscribe to a channel and yield messages."""
    client = get_async_client()
    ps = client.pubsub()
    set_pubsub(ps)
    await ps.subscribe(channel)

    try:
        async for message in ps.listen():
            if message["type"] == "message":
                data = message["data"]
                if isinstance(data, bytes):
                    yield data.decode("utf-8")
                else:
                    yield data
    finally:
        await ps.unsubscribe(channel)
        await ps.close()
        set_pubsub(None)


# -- Distributed locks --------------------------------------------------------


async def acquire_lock(key: str, value: str, ttl_seconds: int) -> bool:
    """Attempt to acquire a distributed lock using SET NX EX."""
    client = get_async_client()
    result = await client.set(key, value, nx=True, ex=ttl_seconds)
    return result is not None


async def release_lock(key: str) -> int:
    """Release a distributed lock."""
    client = get_async_client()
    return await client.delete(key)  # type: ignore[return-value]


# -- Sorted sets --------------------------------------------------------------


def zadd(key: str, mapping: dict[str, float]) -> int:
    """Add members to a sorted set with scores."""
    client = get_sync_client()
    return client.zadd(key, mapping)  # type: ignore[return-value]


async def zrangebyscore(
    key: str, min_score: float, max_score: float
) -> list[bytes]:
    """Get members with scores between min and max."""
    client = get_async_client()
    return await client.zrangebyscore(key, min_score, max_score)  # type: ignore[return-value]


def zrem(key: str, *members: str) -> int:
    """Remove members from a sorted set."""
    client = get_sync_client()
    return client.zrem(key, *members)  # type: ignore[return-value]


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

    client = get_sync_client()
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
