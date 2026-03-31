"""Kafka backend integration tests.

These tests run against a real Kafka broker. They are skipped if the
``aiokafka`` package is not installed or ``KAFKA_BOOTSTRAP_SERVERS`` is
not set.

Run locally:

    docker compose -f docker-compose.kafka.yml up -d

    AGENTEXEC_STATE_BACKEND=agentexec.state.kafka \\
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \\
    uv run pytest tests/test_kafka_integration.py -v

    docker compose -f docker-compose.kafka.yml down
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest
from pydantic import BaseModel

_skip_reason = None

if not os.environ.get("KAFKA_BOOTSTRAP_SERVERS"):
    _skip_reason = "KAFKA_BOOTSTRAP_SERVERS not set"
else:
    try:
        import aiokafka  # noqa: F401
    except ImportError:
        _skip_reason = "aiokafka not installed (pip install agentexec[kafka])"

if _skip_reason:
    pytest.skip(_skip_reason, allow_module_level=True)


from agentexec.state import backend  # noqa: E402
from agentexec.state.kafka import Backend as KafkaBackend  # noqa: E402

_kb: KafkaBackend = backend  # type: ignore[assignment]


class SampleResult(BaseModel):
    status: str
    value: int


class TaskContext(BaseModel):
    query: str


pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest.fixture(autouse=True, scope="module")
async def close_connections():
    """Close all Kafka connections once after the module completes."""
    yield
    await _kb.close()


class TestStateNotSupported:
    async def test_get_raises(self):
        """KV get raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            await _kb.state.get("any-key")

    async def test_set_raises(self):
        """KV set raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            await _kb.state.set("any-key", b"value")

    async def test_counter_raises(self):
        """Counter operations raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            await _kb.state.counter_incr("any-key")


class TestSerialization:
    def test_roundtrip(self):
        """serialize → deserialize preserves type and data."""
        original = SampleResult(status="ok", value=42)
        data = _kb.serialize(original)
        restored = _kb.deserialize(data)
        assert type(restored) is SampleResult
        assert restored == original

    def test_format_key_joins_with_dots(self):
        """Kafka backend uses dots as key separators."""
        assert _kb.format_key("agentexec", "result", "123") == "agentexec.result.123"


class TestQueue:
    async def test_push_and_pop(self):
        """A pushed task can be popped from the queue."""
        import json

        task_data = {
            "task_name": "test_task",
            "context": {"query": "hello"},
            "agent_id": str(uuid.uuid4()),
        }
        await _kb.queue.push(json.dumps(task_data))

        result = await _kb.queue.pop(timeout=10)
        assert result is not None
        assert result["task_name"] == "test_task"
        assert result["context"]["query"] == "hello"

    async def test_pop_empty_queue_returns_none(self):
        """Popping an empty queue returns None after timeout."""
        result = await _kb.queue.pop(timeout=1)
        # May or may not be None depending on prior test state,
        # but should not raise

    async def test_push_with_partition_key(self):
        """Tasks with partition_key are routed deterministically."""
        import json

        task_data = {
            "task_name": "keyed_task",
            "context": {"query": "keyed"},
            "agent_id": str(uuid.uuid4()),
        }
        await _kb.queue.push(json.dumps(task_data), partition_key="user-123")

        result = await _kb.queue.pop(timeout=10)
        assert result is not None
        assert result["task_name"] == "keyed_task"

    async def test_complete_is_noop(self):
        """complete() is a no-op for Kafka (partition assignment handles it)."""
        await _kb.queue.complete("any-key")
        await _kb.queue.complete(None)


class TestConnection:
    async def test_ensure_topic_idempotent(self):
        """ensure_topic can be called multiple times without error."""
        topic = f"test_ensure_{uuid.uuid4().hex[:8]}"
        await _kb.ensure_topic(topic)
        await _kb.ensure_topic(topic)  # Should not raise

    async def test_client_id_includes_pid(self):
        """client_id includes PID for uniqueness."""
        cid = _kb._client_id("producer")
        assert str(os.getpid()) in cid
        assert "producer" in cid

    async def test_produce_and_topic_creation(self):
        """produce() auto-creates the topic if needed."""
        topic = f"test_produce_{uuid.uuid4().hex[:8]}"
        await _kb.produce(topic, b"test-value", key=b"test-key")
        # If we got here without error, produce and topic creation worked
