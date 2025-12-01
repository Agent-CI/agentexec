import redis
import redis.asyncio

from agentexec.config import CONF

_redis_client: redis.asyncio.Redis | None = None
_redis_sync_client: redis.Redis | None = None


def get_redis() -> redis.asyncio.Redis:
    """Get the async Redis client instance.

    Creates and caches a Redis client on first call. Subsequent calls
    return the cached instance.

    The client is configured from agentexec.CONF settings:
    - redis_url: Connection URL
    - redis_pool_size: Connection pool size
    - redis_pool_timeout: Connection timeout

    Returns:
        Async Redis client instance.
    """
    global _redis_client

    if _redis_client is None:
        if CONF.redis_url is None:
            raise ValueError("REDIS_URL must be configured")

        _redis_client = redis.asyncio.Redis.from_url(
            CONF.redis_url,
            max_connections=CONF.redis_pool_size,
            socket_connect_timeout=CONF.redis_pool_timeout,
            decode_responses=False,  # Handle binary data (pickled results)
        )

    return _redis_client


def get_redis_sync() -> redis.Redis:
    """Get the synchronous Redis client instance.

    Creates and caches a Redis client on first call. Subsequent calls
    return the cached instance. Used by worker processes for log publishing.

    Returns:
        Synchronous Redis client instance.
    """
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


async def close_redis() -> None:
    """Close the Redis connection and reset the client."""
    global _redis_client

    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
