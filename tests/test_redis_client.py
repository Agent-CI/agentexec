"""Test Redis client connection management."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import agentexec.core.redis_client as redis_module


@pytest.fixture(autouse=True)
def reset_redis_clients(monkeypatch):
    """Reset Redis client state before and after each test."""
    # Set a test redis URL
    from agentexec.config import CONF

    monkeypatch.setattr(CONF, "redis_url", "redis://localhost:6379/0")

    # Reset clients before test
    redis_module._redis_client = None
    redis_module._redis_sync_client = None

    yield

    # Reset clients after test
    redis_module._redis_client = None
    redis_module._redis_sync_client = None


def test_get_redis_returns_client():
    """Test that get_redis returns a Redis client."""
    redis = redis_module.get_redis()

    assert redis is not None
    # Verify it's configured with decode_responses=False (for binary data like pickled results)
    assert redis.connection_pool.connection_kwargs.get("decode_responses") is False


def test_redis_singleton():
    """Test that get_redis returns the same instance."""
    redis1 = redis_module.get_redis()
    redis2 = redis_module.get_redis()

    assert redis1 is redis2


def test_redis_uses_config():
    """Test that Redis client uses configuration settings."""
    from agentexec import CONF

    redis = redis_module.get_redis()

    # Verify connection pool settings
    pool = redis.connection_pool
    assert pool.max_connections == CONF.redis_pool_size
    assert pool.connection_kwargs.get("socket_connect_timeout") == CONF.redis_pool_timeout


async def test_close_redis_resets_client():
    """Test that close_redis resets the client."""
    # Create initial client
    redis1 = redis_module.get_redis()

    # Mock aclose to avoid actual Redis connection
    redis1.aclose = AsyncMock()

    # Close it
    await redis_module.close_redis()

    # Verify aclose was called
    redis1.aclose.assert_called_once()

    # Getting client again should return a different instance
    redis2 = redis_module.get_redis()

    assert redis1 is not redis2


async def test_close_redis_when_none():
    """Test that close_redis handles None client gracefully."""
    # Ensure client is None
    redis_module._redis_client = None

    # Should not raise
    await redis_module.close_redis()

    # Should still be None
    assert redis_module._redis_client is None


def test_get_redis_raises_without_url(monkeypatch):
    """Test that get_redis raises ValueError when no URL configured."""
    from agentexec.config import CONF

    monkeypatch.setattr(CONF, "redis_url", None)
    # Reset to force new client creation
    redis_module._redis_client = None

    with pytest.raises(ValueError, match="REDIS_URL must be configured"):
        redis_module.get_redis()


def test_get_redis_sync_returns_client():
    """Test that get_redis_sync returns a synchronous Redis client."""
    redis = redis_module.get_redis_sync()

    assert redis is not None
    # Verify it's configured correctly
    assert redis.connection_pool.connection_kwargs.get("decode_responses") is False


def test_redis_sync_singleton():
    """Test that get_redis_sync returns the same instance."""
    redis1 = redis_module.get_redis_sync()
    redis2 = redis_module.get_redis_sync()

    assert redis1 is redis2


def test_get_redis_sync_raises_without_url(monkeypatch):
    """Test that get_redis_sync raises ValueError when no URL configured."""
    from agentexec.config import CONF

    monkeypatch.setattr(CONF, "redis_url", None)
    # Reset to force new client creation
    redis_module._redis_sync_client = None

    with pytest.raises(ValueError, match="REDIS_URL must be configured"):
        redis_module.get_redis_sync()
