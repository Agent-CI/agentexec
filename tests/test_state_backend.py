"""Tests for Redis state backend class."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from agentexec.state import backend
from agentexec.state.redis_backend.backend import RedisBackend


class SampleModel(BaseModel):
    status: str
    value: int


class NestedModel(BaseModel):
    items: list[int]
    metadata: dict[str, str]


@pytest.fixture
def mock_client(monkeypatch):
    """Inject a mock async Redis client into the backend."""
    client = AsyncMock()
    monkeypatch.setattr(backend, "_client", client)
    yield client


class TestFormatKey:
    def test_format_single_part(self):
        assert backend.format_key("result") == "result"

    def test_format_multiple_parts(self):
        assert backend.format_key("agentexec", "result", "123") == "agentexec:result:123"

    def test_format_empty_parts(self):
        assert backend.format_key() == ""


class TestSerialization:
    def test_serialize_basemodel(self):
        data = SampleModel(status="success", value=42)
        result = backend.serialize(data)
        assert isinstance(result, bytes)

    def test_serialize_deserialize_roundtrip(self):
        data = SampleModel(status="success", value=42)
        serialized = backend.serialize(data)
        deserialized = backend.deserialize(serialized)
        assert isinstance(deserialized, SampleModel)
        assert deserialized == data

    def test_serialize_deserialize_nested_model(self):
        data = NestedModel(items=[1, 2, 3], metadata={"key": "value"})
        serialized = backend.serialize(data)
        deserialized = backend.deserialize(serialized)
        assert isinstance(deserialized, NestedModel)
        assert deserialized == data


class TestKeyValueOperations:
    async def test_get(self, mock_client):
        mock_client.get.return_value = b"value"
        result = await backend.state.get("mykey")
        mock_client.get.assert_called_once_with("mykey")
        assert result == b"value"

    async def test_get_missing_key(self, mock_client):
        mock_client.get.return_value = None
        result = await backend.state.get("missing")
        assert result is None

    async def test_set_without_ttl(self, mock_client):
        mock_client.set.return_value = True
        result = await backend.state.set("mykey", b"value")
        mock_client.set.assert_called_once_with("mykey", b"value")
        assert result is True

    async def test_set_with_ttl(self, mock_client):
        mock_client.set.return_value = True
        result = await backend.state.set("mykey", b"value", ttl_seconds=3600)
        mock_client.set.assert_called_once_with("mykey", b"value", ex=3600)
        assert result is True

    async def test_delete(self, mock_client):
        mock_client.delete.return_value = 1
        result = await backend.state.delete("mykey")
        mock_client.delete.assert_called_once_with("mykey")
        assert result == 1


class TestCounterOperations:
    async def test_counter_incr(self, mock_client):
        mock_client.incr.return_value = 5
        result = await backend.state.counter_incr("mycount")
        mock_client.incr.assert_called_once_with("mycount")
        assert result == 5

    async def test_counter_decr(self, mock_client):
        mock_client.decr.return_value = 3
        result = await backend.state.counter_decr("mycount")
        mock_client.decr.assert_called_once_with("mycount")
        assert result == 3


class TestPubSubOperations:
    async def test_log_publish(self, mock_client):
        await backend.state.log_publish("logs", "log message")
        mock_client.publish.assert_called_once_with("logs", "log message")

    async def test_log_subscribe(self, mock_client):
        mock_pubsub = AsyncMock()
        mock_client.pubsub = MagicMock(return_value=mock_pubsub)

        async def mock_listen():
            yield {"type": "subscribe"}
            yield {"type": "message", "data": b"message1"}
            yield {"type": "message", "data": "message2"}

        mock_pubsub.listen = MagicMock(return_value=mock_listen())

        messages = []
        async for msg in backend.state.log_subscribe("test_channel"):
            messages.append(msg)

        assert messages == ["message1", "message2"]
        mock_pubsub.subscribe.assert_called_once_with("test_channel")


class TestConnectionManagement:
    async def test_close_all_connections(self):
        mock_client = AsyncMock()
        mock_ps = AsyncMock()

        backend._client = mock_client
        backend._pubsub = mock_ps

        await backend.close()

        mock_ps.close.assert_called_once()
        mock_client.aclose.assert_called_once()
        assert backend._client is None
        assert backend._pubsub is None

    async def test_close_handles_none_clients(self):
        backend._client = None
        backend._pubsub = None

        await backend.close()

        assert backend._client is None
