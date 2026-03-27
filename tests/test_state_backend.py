"""Tests for state backend module."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from agentexec.state import redis_backend
from agentexec.state.redis_backend import connection


class SampleModel(BaseModel):
    """Sample model for serialization tests."""

    status: str
    value: int


class NestedModel(BaseModel):
    """Model with nested structure for serialization tests."""

    items: list[int]
    metadata: dict[str, str]


@pytest.fixture(autouse=True)
def reset_redis_clients():
    """Reset Redis client state before and after each test."""
    connection._redis_client = None
    connection._redis_sync_client = None
    connection._pubsub = None
    yield
    connection._redis_client = None
    connection._redis_sync_client = None
    connection._pubsub = None



@pytest.fixture
def mock_async_client():
    """Mock asynchronous Redis client."""
    client = AsyncMock()
    with patch("agentexec.state.redis_backend.state.get_async_client", return_value=client):
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

    def test_serialize_basemodel(self):
        """Test serializing a BaseModel."""
        data = SampleModel(status="success", value=42)
        result = redis_backend.serialize(data)
        assert isinstance(result, bytes)

    def test_serialize_rejects_dict(self):
        """Test that serialize rejects raw dicts."""
        with pytest.raises(TypeError, match="Expected BaseModel"):
            redis_backend.serialize({"key": "value"})  # type: ignore[arg-type]

    def test_serialize_rejects_list(self):
        """Test that serialize rejects raw lists."""
        with pytest.raises(TypeError, match="Expected BaseModel"):
            redis_backend.serialize([1, 2, 3])  # type: ignore[arg-type]

    def test_serialize_deserialize_roundtrip(self):
        """Test serialize then deserialize returns equivalent model."""
        data = SampleModel(status="success", value=42)
        serialized = redis_backend.serialize(data)
        deserialized = redis_backend.deserialize(serialized)
        assert isinstance(deserialized, SampleModel)
        assert deserialized.status == data.status
        assert deserialized.value == data.value

    def test_serialize_deserialize_nested_model(self):
        """Test roundtrip with nested structures."""
        data = NestedModel(items=[1, 2, 3], metadata={"key": "value"})
        serialized = redis_backend.serialize(data)
        deserialized = redis_backend.deserialize(serialized)
        assert isinstance(deserialized, NestedModel)
        assert deserialized.items == data.items
        assert deserialized.metadata == data.metadata


class TestKeyValueOperations:
    """Tests for store_get/store_set/store_delete operations."""

    async def test_store_get(self, mock_async_client):
        """Test async get."""
        mock_async_client.get.return_value = b"value"

        result = await redis_backend.store_get("mykey")

        mock_async_client.get.assert_called_once_with("mykey")
        assert result == b"value"

    async def test_store_get_missing_key(self, mock_async_client):
        """Test get returns None for missing key."""
        mock_async_client.get.return_value = None

        result = await redis_backend.store_get("missing")

        assert result is None

    async def test_store_set_without_ttl(self, mock_async_client):
        """Test set without TTL."""
        mock_async_client.set.return_value = True

        result = await redis_backend.store_set("mykey", b"value")

        mock_async_client.set.assert_called_once_with("mykey", b"value")
        assert result is True

    async def test_store_set_with_ttl(self, mock_async_client):
        """Test set with TTL."""
        mock_async_client.set.return_value = True

        result = await redis_backend.store_set("mykey", b"value", ttl_seconds=3600)

        mock_async_client.set.assert_called_once_with("mykey", b"value", ex=3600)
        assert result is True

    async def test_store_delete(self, mock_async_client):
        """Test delete."""
        mock_async_client.delete.return_value = 1

        result = await redis_backend.store_delete("mykey")

        mock_async_client.delete.assert_called_once_with("mykey")
        assert result == 1


class TestCounterOperations:
    """Tests for counter operations."""

    async def test_counter_incr(self, mock_async_client):
        """Test atomic increment."""
        mock_async_client.incr.return_value = 5

        result = await redis_backend.counter_incr("mycount")

        mock_async_client.incr.assert_called_once_with("mycount")
        assert result == 5

    async def test_counter_decr(self, mock_async_client):
        """Test atomic decrement."""
        mock_async_client.decr.return_value = 3

        result = await redis_backend.counter_decr("mycount")

        mock_async_client.decr.assert_called_once_with("mycount")
        assert result == 3


class TestPubSubOperations:
    """Tests for pub/sub operations."""

    async def test_log_publish(self, mock_async_client):
        """Test publishing message to channel."""
        await redis_backend.log_publish("logs", "log message")

        mock_async_client.publish.assert_called_once_with("logs", "log message")

    async def test_log_subscribe(self, mock_async_client):
        """Test subscribing to channel."""
        mock_pubsub = AsyncMock()
        mock_async_client.pubsub = MagicMock(return_value=mock_pubsub)

        async def mock_listen():
            yield {"type": "subscribe"}
            yield {"type": "message", "data": b"message1"}
            yield {"type": "message", "data": "message2"}

        mock_pubsub.listen = MagicMock(return_value=mock_listen())

        messages = []
        async for msg in redis_backend.log_subscribe("test_channel"):
            messages.append(msg)

        assert messages == ["message1", "message2"]
        mock_pubsub.subscribe.assert_called_once_with("test_channel")
        mock_pubsub.unsubscribe.assert_called_once_with("test_channel")
        mock_pubsub.close.assert_called_once()


class TestConnectionManagement:
    """Tests for connection lifecycle."""

    async def test_close_all_connections(self):
        """Test close cleans up all resources."""
        mock_async = AsyncMock()
        mock_sync = MagicMock()
        mock_ps = AsyncMock()

        connection._redis_client = mock_async
        connection._redis_sync_client = mock_sync
        connection._pubsub = mock_ps

        await redis_backend.close()

        mock_ps.close.assert_called_once()
        mock_async.aclose.assert_called_once()
        mock_sync.close.assert_called_once()

        assert connection._redis_client is None
        assert connection._redis_sync_client is None
        assert connection._pubsub is None

    async def test_close_handles_none_clients(self):
        """Test close handles None clients gracefully."""
        connection._redis_client = None
        connection._redis_sync_client = None
        connection._pubsub = None

        await redis_backend.close()

        assert connection._redis_client is None
        assert connection._redis_sync_client is None
