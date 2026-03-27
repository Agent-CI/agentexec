"""Tests for state module public API."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from agentexec import state
from agentexec.state import ops


# Test models for result serialization
class ResultModel(BaseModel):
    """Test result model."""

    status: str
    value: int


class OutputModel(BaseModel):
    """Test output model."""

    status: str
    output: str


class TestResultOperations:
    """Tests for result get/set/delete operations."""

    async def test_get_result_found(self):
        """Test getting an existing result returns deserialized BaseModel."""
        result_model = ResultModel(status="success", value=42)
        serialized = state.backend.serialize(result_model)

        async def mock_store_get(key):
            return serialized

        with patch.object(state.backend, "store_get", side_effect=mock_store_get):
            result = await state.get_result("agent123")

            assert isinstance(result, ResultModel)
            assert result == result_model

    async def test_get_result_not_found(self):
        """Test getting a non-existent result returns None."""
        async def mock_store_get(key):
            return None

        with patch.object(state.backend, "store_get", side_effect=mock_store_get):
            result = await state.get_result("agent456")

            assert result is None

    async def test_set_result_without_ttl(self):
        """Test setting a result without TTL."""
        result_model = ResultModel(status="success", value=42)
        stored = {}

        async def mock_store_set(key, value, ttl_seconds=None):
            stored["key"] = key
            stored["value"] = value
            stored["ttl_seconds"] = ttl_seconds
            return True

        with patch.object(state.backend, "store_set", side_effect=mock_store_set):
            success = await state.set_result("agent123", result_model)

            assert stored["key"] == "agentexec:result:agent123"
            assert isinstance(stored["value"], bytes)
            deserialized = state.backend.deserialize(stored["value"])
            assert isinstance(deserialized, ResultModel)
            assert deserialized == result_model
            assert stored["ttl_seconds"] is None
            assert success is True

    async def test_set_result_with_ttl(self):
        """Test setting a result with TTL."""
        result_model = ResultModel(status="success", value=100)
        stored = {}

        async def mock_store_set(key, value, ttl_seconds=None):
            stored["key"] = key
            stored["ttl_seconds"] = ttl_seconds
            return True

        with patch.object(state.backend, "store_set", side_effect=mock_store_set):
            success = await state.set_result("agent456", result_model, ttl_seconds=3600)

            assert stored["key"] == "agentexec:result:agent456"
            assert stored["ttl_seconds"] == 3600
            assert success is True

    async def test_delete_result(self):
        """Test deleting a result."""
        async def mock_store_delete(key):
            return 1

        with patch.object(state.backend, "store_delete", side_effect=mock_store_delete):
            count = await state.delete_result("agent123")

            assert count == 1


class TestLogOperations:
    """Tests for log pub/sub operations."""

    async def test_publish_log(self):
        """Test publishing a log message."""
        log_message = '{"level": "info", "message": "test log"}'

        with patch.object(state.backend, "log_publish", new_callable=AsyncMock) as mock_publish:
            await state.publish_log(log_message)

            mock_publish.assert_called_once_with("agentexec:logs", log_message)

    async def test_subscribe_logs(self):
        """Test subscribing to logs."""
        log_messages = [
            '{"level": "info", "message": "log1"}',
            '{"level": "error", "message": "log2"}'
        ]

        async def mock_subscribe(channel):
            for msg in log_messages:
                yield msg

        with patch.object(state.backend, "log_subscribe", side_effect=mock_subscribe):
            messages = []
            async for msg in state.subscribe_logs():
                messages.append(msg)

            assert messages == log_messages


class TestKeyGeneration:
    """Tests for key generation with format_key."""

    async def test_result_key_format(self):
        """Test that result keys are formatted correctly."""
        async def mock_store_get(key):
            assert key == "agentexec:result:test-id"
            return None

        with patch.object(state.backend, "store_get", side_effect=mock_store_get):
            await state.get_result("test-id")

    async def test_logs_channel_format(self):
        """Test that log channel is formatted correctly."""
        with patch.object(state.backend, "log_publish", new_callable=AsyncMock) as mock_publish:
            await state.publish_log("test")

            mock_publish.assert_called_once_with("agentexec:logs", "test")
