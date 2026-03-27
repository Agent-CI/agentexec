# cspell:ignore rpush lpush brpop RPUSH LPUSH BRPOP
"""Redis implementation of the KV backend protocol.

Provides key-value storage, atomic counters, sorted sets, distributed locks,
and pub/sub via Redis. This module is loaded dynamically by the state layer
based on configuration.
"""

from typing import AsyncGenerator, Coroutine, Optional

import redis
import redis.asyncio

from agentexec.config import CONF

__all__ = [
    "close",
    "format_key",
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
    "clear_keys",
]

_redis_client: redis.asyncio.Redis | None = None
_redis_sync_client: redis.Redis | None = None
_pubsub: redis.asyncio.client.PubSub | None = None


def format_key(*args: str) -> str:
    """Format a Redis key by joining parts with colons."""
    return ":".join(args)


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
