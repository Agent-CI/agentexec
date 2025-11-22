from redis import Redis
from agentexec.config import CONF

_redis_client: Redis | None = None


def get_redis() -> Redis:
    """Get the Redis client instance.

    Creates and caches a Redis client on first call. Subsequent calls
    return the cached instance.

    The client is configured from agentexec.CONF settings:
    - redis_url: Connection URL
    - redis_pool_size: Connection pool size
    - redis_pool_timeout: Connection timeout

    Returns:
        Redis client instance.
    """
    global _redis_client

    if _redis_client is None:
        _redis_client = Redis.from_url(
            CONF.redis_url,
            max_connections=CONF.redis_pool_size,
            socket_connect_timeout=CONF.redis_pool_timeout,
            decode_responses=True,  # Automatically decode bytes to strings
        )

    return _redis_client


def close_redis() -> None:
    """Close the Redis connection and reset the client."""
    global _redis_client

    if _redis_client is not None:
        _redis_client.close()
        _redis_client = None
