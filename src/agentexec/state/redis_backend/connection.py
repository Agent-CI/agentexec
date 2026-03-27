# cspell:ignore aclose
"""Redis connection management."""

from __future__ import annotations

import redis
import redis.asyncio

from agentexec.config import CONF

_redis_client: redis.asyncio.Redis | None = None
_redis_sync_client: redis.Redis | None = None
_pubsub: redis.asyncio.client.PubSub | None = None


def get_async_client() -> redis.asyncio.Redis:
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


def get_sync_client() -> redis.Redis:
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


def get_pubsub() -> redis.asyncio.client.PubSub | None:
    """Get the current pubsub instance."""
    return _pubsub


def set_pubsub(ps: redis.asyncio.client.PubSub | None) -> None:
    """Set the pubsub instance."""
    global _pubsub
    _pubsub = ps


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
