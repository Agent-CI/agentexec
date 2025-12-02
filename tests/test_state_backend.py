"""Tests for state backend module."""

import pickle
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agentexec.state import redis_backend


@pytest.fixture(autouse=True)
def reset_redis_clients():
    """Reset Redis client state before and after each test."""
    redis_backend._redis_client = None
    redis_backend._redis_sync_client = None
    redis_backend._pubsub = None
    yield
    redis_backend._redis_client = None
    redis_backend._redis_sync_client = None
    redis_backend._pubsub = None


@pytest.fixture
def mock_sync_client():
    """Mock synchronous Redis client."""
    client = MagicMock()
    with patch.object(redis_backend, "_get_sync_client", return_value=client):
        yield client


@pytest.fixture
def mock_async_client():
    """Mock asynchronous Redis client."""
    client = AsyncMock()
    with patch.object(redis_backend, "_get_async_client", return_value=client):
        yield client


class TestFormatKey:
    """Tests for format_key function."""

    def test_format_single_part(self):
        """Test formatting key with single part."""
        result = redis_backend.format_key("result")
        assert result == "result"

    def test_format_multiple_parts(self):
        """Test formatting key with multiple parts."""
        result = redis_backend.format_key("agentexec", "result", "123")
        assert result == "agentexec:result:123"

    def test_format_empty_parts(self):
        """Test formatting with no parts returns empty string."""
        result = redis_backend.format_key()
        assert result == ""


class TestSerialization:
    """Tests for serialize and deserialize functions."""

    def test_serialize_dict(self):
        """Test serializing a dictionary."""
        data = {"status": "success", "value": 42}
        result = redis_backend.serialize(data)
        assert isinstance(result, bytes)
        assert pickle.loads(result) == data

    def test_serialize_list(self):
        """Test serializing a list."""
        data = [1, 2, 3, "test"]
        result = redis_backend.serialize(data)
        assert isinstance(result, bytes)
        assert pickle.loads(result) == data

    def test_deserialize_bytes(self):
        """Test deserializing pickled bytes."""
        data = {"key": "value", "number": 123}
        pickled = pickle.dumps(data)
        result = redis_backend.deserialize(pickled)
        assert result == data

    def test_serialize_deserialize_roundtrip(self):
        """Test serialize then deserialize returns original."""
        data = {"complex": [1, 2, {"nested": True}], "tuple": (1, 2, 3)}
        serialized = redis_backend.serialize(data)
        deserialized = redis_backend.deserialize(serialized)
        assert deserialized == data


class TestQueueOperations:
    """Tests for queue operations (rpush, lpush, brpop)."""

    def test_rpush(self, mock_sync_client):
        """Test rpush adds to right of list."""
        mock_sync_client.rpush.return_value = 5

        result = redis_backend.rpush("tasks", "task_data")

        mock_sync_client.rpush.assert_called_once_with("tasks", "task_data")
        assert result == 5

    def test_lpush(self, mock_sync_client):
        """Test lpush adds to left of list."""
        mock_sync_client.lpush.return_value = 3

        result = redis_backend.lpush("tasks", "task_data")

        mock_sync_client.lpush.assert_called_once_with("tasks", "task_data")
        assert result == 3

    async def test_brpop_with_result(self, mock_async_client):
        """Test brpop returns decoded result."""
        mock_async_client.brpop.return_value = (b"tasks", b"task_value")

        result = await redis_backend.brpop("tasks", timeout=5)

        mock_async_client.brpop.assert_called_once_with(["tasks"], timeout=5)
        assert result == ("tasks", "task_value")

    async def test_brpop_timeout(self, mock_async_client):
        """Test brpop returns None on timeout."""
        mock_async_client.brpop.return_value = None

        result = await redis_backend.brpop("tasks", timeout=1)

        assert result is None


class TestKeyValueOperations:
    """Tests for get/set/delete operations."""

    def test_get_sync(self, mock_sync_client):
        """Test synchronous get."""
        mock_sync_client.get.return_value = b"value"

        result = redis_backend.get("mykey")

        mock_sync_client.get.assert_called_once_with("mykey")
        assert result == b"value"

    def test_get_sync_missing_key(self, mock_sync_client):
        """Test get returns None for missing key."""
        mock_sync_client.get.return_value = None

        result = redis_backend.get("missing")

        assert result is None

    async def test_aget(self, mock_async_client):
        """Test asynchronous get."""
        mock_async_client.get.return_value = b"async_value"

        result = await redis_backend.aget("mykey")

        mock_async_client.get.assert_called_once_with("mykey")
        assert result == b"async_value"

    def test_set_sync(self, mock_sync_client):
        """Test synchronous set without TTL."""
        mock_sync_client.set.return_value = True

        result = redis_backend.set("mykey", b"value")

        mock_sync_client.set.assert_called_once_with("mykey", b"value")
        assert result is True

    def test_set_sync_with_ttl(self, mock_sync_client):
        """Test synchronous set with TTL."""
        mock_sync_client.set.return_value = True

        result = redis_backend.set("mykey", b"value", ttl_seconds=3600)

        mock_sync_client.set.assert_called_once_with("mykey", b"value", ex=3600)
        assert result is True

    async def test_aset(self, mock_async_client):
        """Test asynchronous set with TTL."""
        mock_async_client.set.return_value = True

        result = await redis_backend.aset("mykey", b"value", ttl_seconds=7200)

        mock_async_client.set.assert_called_once_with("mykey", b"value", ex=7200)
        assert result is True

    def test_delete_sync(self, mock_sync_client):
        """Test synchronous delete."""
        mock_sync_client.delete.return_value = 1

        result = redis_backend.delete("mykey")

        mock_sync_client.delete.assert_called_once_with("mykey")
        assert result == 1

    async def test_adelete(self, mock_async_client):
        """Test asynchronous delete."""
        mock_async_client.delete.return_value = 1

        result = await redis_backend.adelete("mykey")

        mock_async_client.delete.assert_called_once_with("mykey")
        assert result == 1


class TestPubSubOperations:
    """Tests for pub/sub operations."""

    def test_publish(self, mock_sync_client):
        """Test publishing message to channel."""
        redis_backend.publish("logs", "log message")

        mock_sync_client.publish.assert_called_once_with("logs", "log message")

    async def test_subscribe(self, mock_async_client):
        """Test subscribing to channel."""
        mock_pubsub = AsyncMock()
        # Make pubsub() return the mock directly (not a coroutine)
        mock_async_client.pubsub = MagicMock(return_value=mock_pubsub)

        # Create async iterator for messages
        async def mock_listen():
            yield {"type": "subscribe"}
            yield {"type": "message", "data": b"message1"}
            yield {"type": "message", "data": "message2"}

        # Make listen() return the generator directly (not wrapped in AsyncMock)
        mock_pubsub.listen = MagicMock(return_value=mock_listen())

        messages = []
        async for msg in redis_backend.subscribe("test_channel"):
            messages.append(msg)

        assert messages == ["message1", "message2"]
        mock_pubsub.subscribe.assert_called_once_with("test_channel")
        mock_pubsub.unsubscribe.assert_called_once_with("test_channel")
        mock_pubsub.close.assert_called_once()


class TestConnectionManagement:
    """Tests for connection lifecycle."""

    async def test_close_all_connections(self):
        """Test close cleans up all resources."""
        # Set up mock clients
        mock_async = AsyncMock()
        mock_sync = MagicMock()
        mock_ps = AsyncMock()

        redis_backend._redis_client = mock_async
        redis_backend._redis_sync_client = mock_sync
        redis_backend._pubsub = mock_ps

        await redis_backend.close()

        mock_ps.close.assert_called_once()
        mock_async.aclose.assert_called_once()
        mock_sync.close.assert_called_once()

        assert redis_backend._redis_client is None
        assert redis_backend._redis_sync_client is None
        assert redis_backend._pubsub is None

    async def test_close_handles_none_clients(self):
        """Test close handles None clients gracefully."""
        redis_backend._redis_client = None
        redis_backend._redis_sync_client = None
        redis_backend._pubsub = None

        # Should not raise
        await redis_backend.close()

        assert redis_backend._redis_client is None
        assert redis_backend._redis_sync_client is None
