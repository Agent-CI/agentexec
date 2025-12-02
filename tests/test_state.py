"""Tests for state module public API."""

import pickle
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agentexec import state


class TestResultOperations:
    """Tests for result get/set/delete operations."""

    def test_get_result_found(self):
        """Test getting an existing result deserializes correctly."""
        result_data = {"status": "success", "value": 42}
        pickled = pickle.dumps(result_data)

        with patch.object(state.backend, "get", return_value=pickled) as mock_get:
            result = state.get_result("agent123")

            mock_get.assert_called_once_with("agentexec:result:agent123")
            assert result == result_data

    def test_get_result_not_found(self):
        """Test getting a non-existent result returns None."""
        with patch.object(state.backend, "get", return_value=None) as mock_get:
            result = state.get_result("agent456")

            mock_get.assert_called_once_with("agentexec:result:agent456")
            assert result is None

    async def test_aget_result_found(self):
        """Test async getting an existing result."""
        result_data = {"status": "complete", "output": "test"}
        pickled = pickle.dumps(result_data)

        async def mock_aget(key):
            return pickled

        with patch.object(state.backend, "aget", side_effect=mock_aget):
            result = await state.aget_result("agent789")

            assert result == result_data

    async def test_aget_result_not_found(self):
        """Test async getting a non-existent result."""
        async def mock_aget(key):
            return None

        with patch.object(state.backend, "aget", side_effect=mock_aget):
            result = await state.aget_result("missing")

            assert result is None

    def test_set_result_without_ttl(self):
        """Test setting a result without TTL."""
        result_data = {"key": "value"}

        with patch.object(state.backend, "set", return_value=True) as mock_set, \
             patch.object(state.backend, "serialize", wraps=pickle.dumps) as mock_serialize:
            success = state.set_result("agent123", result_data)

            mock_serialize.assert_called_once_with(result_data)
            mock_set.assert_called_once()
            call_args = mock_set.call_args
            assert call_args[0][0] == "agentexec:result:agent123"
            assert call_args[1]["ttl_seconds"] is None
            assert success is True

    def test_set_result_with_ttl(self):
        """Test setting a result with TTL."""
        result_data = {"temporary": "data"}

        with patch.object(state.backend, "set", return_value=True) as mock_set, \
             patch.object(state.backend, "serialize", wraps=pickle.dumps):
            success = state.set_result("agent456", result_data, ttl_seconds=3600)

            call_args = mock_set.call_args
            assert call_args[0][0] == "agentexec:result:agent456"
            assert call_args[1]["ttl_seconds"] == 3600
            assert success is True

    async def test_aset_result(self):
        """Test async setting a result."""
        result_data = {"async": "result"}

        async def mock_aset(key, value, ttl_seconds=None):
            return True

        with patch.object(state.backend, "aset", side_effect=mock_aset) as mock_aset_patch, \
             patch.object(state.backend, "serialize", wraps=pickle.dumps):
            success = await state.aset_result("agent789", result_data, ttl_seconds=7200)

            assert success is True

    def test_delete_result(self):
        """Test deleting a result."""
        with patch.object(state.backend, "delete", return_value=1) as mock_delete:
            count = state.delete_result("agent123")

            mock_delete.assert_called_once_with("agentexec:result:agent123")
            assert count == 1

    async def test_adelete_result(self):
        """Test async deleting a result."""
        async def mock_adelete(key):
            return 1

        with patch.object(state.backend, "adelete", side_effect=mock_adelete):
            count = await state.adelete_result("agent456")

            assert count == 1


class TestLogOperations:
    """Tests for log pub/sub operations."""

    def test_publish_log(self):
        """Test publishing a log message."""
        log_message = '{"level": "info", "message": "test log"}'

        with patch.object(state.backend, "publish") as mock_publish:
            state.publish_log(log_message)

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

        with patch.object(state.backend, "subscribe", side_effect=mock_subscribe):
            messages = []
            async for msg in state.subscribe_logs():
                messages.append(msg)

            assert messages == log_messages


class TestKeyGeneration:
    """Tests for key generation with format_key."""

    def test_result_key_format(self):
        """Test that result keys are formatted correctly."""
        with patch.object(state.backend, "get", return_value=None) as mock_get:
            state.get_result("test-id")

            mock_get.assert_called_once_with("agentexec:result:test-id")

    def test_logs_channel_format(self):
        """Test that log channel is formatted correctly."""
        with patch.object(state.backend, "publish") as mock_publish:
            state.publish_log("test")

            mock_publish.assert_called_once_with("agentexec:logs", "test")
