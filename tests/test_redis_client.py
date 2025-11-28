"""Test Redis client connection management."""

import pytest


@pytest.fixture(autouse=True)
async def cleanup_redis(monkeypatch) -> None:
    """Setup test redis URL and cleanup after each test."""
    from agentexec.config import CONF

    monkeypatch.setattr(CONF, "redis_url", "redis://localhost:6379/0")

    yield

    from agentexec.core.redis_client import close_redis

    await close_redis()


def test_get_redis() -> None:
    """Test that get_redis returns a Redis client."""
    from agentexec.core.redis_client import get_redis

    redis = get_redis()

    assert redis is not None
    # Verify it's configured with decode_responses=True
    assert redis.connection_pool.connection_kwargs.get("decode_responses") is True


def test_redis_singleton() -> None:
    """Test that get_redis returns the same instance."""
    from agentexec.core.redis_client import get_redis

    redis1 = get_redis()
    redis2 = get_redis()

    assert redis1 is redis2


def test_redis_uses_config() -> None:
    """Test that Redis client uses configuration settings."""
    from agentexec import CONF
    from agentexec.core.redis_client import get_redis

    redis = get_redis()

    # Verify connection pool settings
    pool = redis.connection_pool
    assert pool.max_connections == CONF.redis_pool_size
    assert pool.connection_kwargs.get("socket_connect_timeout") == CONF.redis_pool_timeout


async def test_close_redis() -> None:
    """Test that close_redis resets the client."""
    from agentexec.core.redis_client import close_redis, get_redis

    redis1 = get_redis()
    await close_redis()
    redis2 = get_redis()

    # Should be a new instance after close
    assert redis1 is not redis2
