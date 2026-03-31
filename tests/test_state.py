from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel

from agentexec.state import KEY_RESULT, backend


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


