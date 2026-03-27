from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel

from agentexec.state import KEY_RESULT, CHANNEL_LOGS, backend


class ResultModel(BaseModel):
    status: str
    value: int


class TestSerialization:
    """Tests for serialize/deserialize on the backend."""

    def test_roundtrip(self):
        model = ResultModel(status="success", value=42)
        data = backend.serialize(model)
        restored = backend.deserialize(data)
        assert isinstance(restored, ResultModel)
        assert restored == model


class TestFormatKey:
    """Tests for key formatting."""

    def test_result_key(self):
        key = backend.format_key(*KEY_RESULT, "agent-123")
        assert "result" in key
        assert "agent-123" in key

    def test_logs_channel(self):
        channel = backend.format_key(*CHANNEL_LOGS)
        assert "logs" in channel


class TestStateBackend:
    """Tests for state.get/set/delete via backend.state."""

    async def test_set_and_get(self):
        result = ResultModel(status="success", value=42)
        serialized = backend.serialize(result)

        async def mock_get(key):
            return serialized

        with patch.object(backend.state, "get", side_effect=mock_get):
            data = await backend.state.get("test-key")
            restored = backend.deserialize(data)
            assert isinstance(restored, ResultModel)
            assert restored == result

    async def test_get_missing(self):
        async def mock_get(key):
            return None

        with patch.object(backend.state, "get", side_effect=mock_get):
            result = await backend.state.get("missing-key")
            assert result is None


class TestLogOperations:
    """Tests for log pub/sub."""

    async def test_publish(self):
        with patch.object(backend.state, "log_publish", new_callable=AsyncMock) as mock:
            channel = backend.format_key(*CHANNEL_LOGS)
            await backend.state.log_publish(channel, "test message")
            mock.assert_called_once_with(channel, "test message")

    async def test_subscribe(self):
        messages = ["msg1", "msg2"]

        async def mock_subscribe(channel):
            for msg in messages:
                yield msg

        with patch.object(backend.state, "log_subscribe", side_effect=mock_subscribe):
            received = []
            channel = backend.format_key(*CHANNEL_LOGS)
            async for msg in backend.state.log_subscribe(channel):
                received.append(msg)
            assert received == messages
