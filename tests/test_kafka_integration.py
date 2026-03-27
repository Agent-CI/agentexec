"""Kafka backend integration tests.

These tests run against a real Kafka broker. They are skipped if the
``aiokafka`` package is not installed or ``KAFKA_BOOTSTRAP_SERVERS`` is
not set.

Run locally:

    docker compose -f docker-compose.kafka.yml up -d

    AGENTEXEC_STATE_BACKEND=agentexec.state.kafka_backend \\
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

# ---------------------------------------------------------------------------
# Skip entire module if prerequisites not met
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Imports that require Kafka (after skip check)
# ---------------------------------------------------------------------------

from agentexec.state import backend  # noqa: E402
from agentexec.state.kafka_backend.backend import KafkaBackend  # noqa: E402

# Convenience aliases to keep test code concise
_kb: KafkaBackend = backend  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Test models
# ---------------------------------------------------------------------------


class SampleResult(BaseModel):
    status: str
    value: int


class TaskContext(BaseModel):
    query: str


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest.fixture(autouse=True)
async def kafka_cleanup():
    """Ensure caches are clean before/after each test."""
    await _kb.state.clear()
    _kb._activity_cache.clear()

    yield

    await _kb.state.clear()
    _kb._activity_cache.clear()


@pytest.fixture(autouse=True, scope="module")
async def close_connections():
    """Close all Kafka connections once after the module completes."""
    yield
    await _kb.close()


# ---------------------------------------------------------------------------
# State: KV store
# ---------------------------------------------------------------------------


class TestKVStore:
    async def test_store_set_and_get(self):
        """Values written via store_set are readable from the cache."""
        key = f"test:kv:{uuid.uuid4()}"
        await _kb.state.set(key, b"hello-world")
        result = await _kb.state.get(key)
        assert result == b"hello-world"

    async def test_store_get_missing_key(self):
        """Reading a non-existent key returns None."""
        result = await _kb.state.get(f"test:missing:{uuid.uuid4()}")
        assert result is None

    async def test_store_delete(self):
        """Deleting a key removes it from the cache."""
        key = f"test:kv:{uuid.uuid4()}"
        await _kb.state.set(key, b"to-delete")
        assert await _kb.state.get(key) == b"to-delete"

        await _kb.state.delete(key)
        assert await _kb.state.get(key) is None

    async def test_store_set_overwrites(self):
        """A second store_set for the same key overwrites the value."""
        key = f"test:kv:{uuid.uuid4()}"
        await _kb.state.set(key, b"v1")
        await _kb.state.set(key, b"v2")
        assert await _kb.state.get(key) == b"v2"


# ---------------------------------------------------------------------------
# State: Counters
# ---------------------------------------------------------------------------


class TestCounters:
    async def test_incr_from_zero(self):
        """Incrementing a non-existent counter starts at 1."""
        key = f"test:counter:{uuid.uuid4()}"
        result = await _kb.state.counter_incr(key)
        assert result == 1

    async def test_incr_multiple(self):
        """Multiple increments accumulate."""
        key = f"test:counter:{uuid.uuid4()}"
        await _kb.state.counter_incr(key)
        await _kb.state.counter_incr(key)
        result = await _kb.state.counter_incr(key)
        assert result == 3

    async def test_decr(self):
        """Decrement reduces the counter."""
        key = f"test:counter:{uuid.uuid4()}"
        await _kb.state.counter_incr(key)
        await _kb.state.counter_incr(key)
        result = await _kb.state.counter_decr(key)
        assert result == 1


# ---------------------------------------------------------------------------
# State: Sorted index
# ---------------------------------------------------------------------------


class TestSortedIndex:
    async def test_index_add_and_range(self):
        """Members added with scores can be queried by score range."""
        key = f"test:index:{uuid.uuid4()}"
        await _kb.state.index_add(key, {"task_a": 100.0, "task_b": 200.0, "task_c": 300.0})

        result = await _kb.state.index_range(key, 0.0, 250.0)
        names = [item.decode() for item in result]
        assert "task_a" in names
        assert "task_b" in names
        assert "task_c" not in names

    async def test_index_remove(self):
        """Removed members no longer appear in range queries."""
        key = f"test:index:{uuid.uuid4()}"
        await _kb.state.index_add(key, {"task_a": 100.0, "task_b": 200.0})
        await _kb.state.index_remove(key, "task_a")

        result = await _kb.state.index_range(key, 0.0, 999.0)
        names = [item.decode() for item in result]
        assert "task_a" not in names
        assert "task_b" in names


# ---------------------------------------------------------------------------
# State: Serialization
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Queue: push / pop / commit
# ---------------------------------------------------------------------------


class TestQueue:
    async def test_push_and_pop(self):
        """A pushed task can be popped from the queue."""
        # Use a unique queue name per test to avoid cross-test interference
        q = f"kafka_test_{uuid.uuid4().hex[:8]}"
        import json

        task_data = {
            "task_name": "test_task",
            "context": {"query": "hello"},
            "agent_id": str(uuid.uuid4()),
        }
        await _kb.queue.push(q, json.dumps(task_data))

        result = await _kb.queue.pop(q, timeout=10)
        assert result is not None
        assert result["task_name"] == "test_task"
        assert result["context"]["query"] == "hello"

    async def test_pop_empty_queue_returns_none(self):
        """Popping an empty queue returns None after timeout."""
        q = f"kafka_empty_{uuid.uuid4().hex[:8]}"
        result = await _kb.queue.pop(q, timeout=1)
        assert result is None

    async def test_push_with_partition_key(self):
        """Tasks with partition_key are routed deterministically."""
        q = f"kafka_pk_{uuid.uuid4().hex[:8]}"
        import json

        task_data = {
            "task_name": "keyed_task",
            "context": {"query": "keyed"},
            "agent_id": str(uuid.uuid4()),
        }
        await _kb.queue.push(q, json.dumps(task_data), partition_key="user-123")

        result = await _kb.queue.pop(q, timeout=10)
        assert result is not None
        assert result["task_name"] == "keyed_task"

    async def test_multiple_push_pop_ordering(self):
        """Tasks with the same partition key are consumed in order."""
        q = f"kafka_order_{uuid.uuid4().hex[:8]}"
        import json

        ids = [str(uuid.uuid4()) for _ in range(3)]
        for agent_id in ids:
            await _kb.queue.push(q, json.dumps({
                "task_name": "order_test",
                "context": {"query": "test"},
                "agent_id": agent_id,
            }), partition_key="same-key")

        received = []
        for _ in range(3):
            result = await _kb.queue.pop(q, timeout=10)
            assert result is not None
            received.append(result["agent_id"])
    
        assert received == ids


# ---------------------------------------------------------------------------
# Activity tracking
# ---------------------------------------------------------------------------


class TestActivity:
    async def test_create_and_get(self):
        """Creating an activity makes it retrievable."""
        agent_id = uuid.uuid4()
        await _kb.activity.create(agent_id, "test_task", "Agent queued", None)

        record = await _kb.activity.get(agent_id)
        assert record is not None
        assert record["agent_id"] == str(agent_id)
        assert record["agent_type"] == "test_task"
        assert len(record["logs"]) == 1
        assert record["logs"][0]["status"] == "queued"
        assert record["logs"][0]["message"] == "Agent queued"

    async def test_append_log(self):
        """Appending a log entry adds to the record."""
        agent_id = uuid.uuid4()
        await _kb.activity.create(agent_id, "test_task", "Queued", None)
        await _kb.activity.append_log(agent_id, "Processing", "running", 50)

        record = await _kb.activity.get(agent_id)
        assert len(record["logs"]) == 2
        assert record["logs"][1]["status"] == "running"
        assert record["logs"][1]["message"] == "Processing"
        assert record["logs"][1]["percentage"] == 50

    async def test_activity_lifecycle(self):
        """Full lifecycle: create → update → complete."""
        agent_id = uuid.uuid4()
        await _kb.activity.create(agent_id, "lifecycle_task", "Queued", None)
        await _kb.activity.append_log(agent_id, "Started", "running", 0)
        await _kb.activity.append_log(agent_id, "Halfway", "running", 50)
        await _kb.activity.append_log(agent_id, "Done", "complete", 100)

        record = await _kb.activity.get(agent_id)
        assert len(record["logs"]) == 4
        assert record["logs"][-1]["status"] == "complete"
        assert record["logs"][-1]["percentage"] == 100

    async def test_activity_list_pagination(self):
        """activity_list returns paginated results."""
        for i in range(5):
            await _kb.activity.create(uuid.uuid4(), f"task_{i}", "Queued", None)

        rows, total = await _kb.activity.list(page=1, page_size=3)
        assert total == 5
        assert len(rows) == 3

        rows2, total2 = await _kb.activity.list(page=2, page_size=3)
        assert total2 == 5
        assert len(rows2) == 2

    async def test_activity_count_active(self):
        """count_active returns queued + running activities."""
        a1 = uuid.uuid4()
        a2 = uuid.uuid4()
        a3 = uuid.uuid4()

        await _kb.activity.create(a1, "task", "Queued", None)
        await _kb.activity.create(a2, "task", "Queued", None)
        await _kb.activity.create(a3, "task", "Queued", None)

        # Mark one as running, one as complete
        await _kb.activity.append_log(a2, "Running", "running", 10)
        await _kb.activity.append_log(a3, "Done", "complete", 100)

        count = await _kb.activity.count_active()
        assert count == 2  # a1 (queued) + a2 (running)

    async def test_activity_get_pending_ids(self):
        """get_pending_ids returns agent_ids for queued/running activities."""
        a1 = uuid.uuid4()
        a2 = uuid.uuid4()
        a3 = uuid.uuid4()

        await _kb.activity.create(a1, "task", "Queued", None)
        await _kb.activity.create(a2, "task", "Queued", None)
        await _kb.activity.create(a3, "task", "Queued", None)

        await _kb.activity.append_log(a3, "Done", "complete", 100)

        pending = await _kb.activity.get_pending_ids()
        pending_set = {str(p) for p in pending}
        assert str(a1) in pending_set
        assert str(a2) in pending_set
        assert str(a3) not in pending_set

    async def test_activity_with_metadata(self):
        """Metadata is stored and filterable."""
        agent_id = uuid.uuid4()
        await _kb.activity.create(
            agent_id, "task", "Queued",
            metadata={"org_id": "org-123", "env": "test"},
        )

        # Retrieve without filter
        record = await _kb.activity.get(agent_id)
        assert record["metadata"] == {"org_id": "org-123", "env": "test"}

        # Filter match
        record = await _kb.activity.get(agent_id, metadata_filter={"org_id": "org-123"})
        assert record is not None

        # Filter mismatch
        record = await _kb.activity.get(agent_id, metadata_filter={"org_id": "org-999"})
        assert record is None

    async def test_activity_get_nonexistent(self):
        """Getting a non-existent activity returns None."""
        result = await _kb.activity.get(uuid.uuid4())
        assert result is None


# ---------------------------------------------------------------------------
# Pub/sub (log streaming)
# ---------------------------------------------------------------------------


class TestLogPubSub:
    async def test_publish_and_subscribe(self):
        """Published log messages arrive via subscribe."""
        channel = _kb.format_key("agentexec", "logs")
        received = []

        async def subscriber():
            async for msg in _kb.state.log_subscribe(channel):
                received.append(msg)
                if len(received) >= 2:
                    break

        # Start subscriber in background
        sub_task = asyncio.create_task(subscriber())

        # Give the consumer time to join
        await asyncio.sleep(2)

        # Publish messages
        await _kb.state.log_publish(channel, '{"level":"info","msg":"hello"}')
        await _kb.state.log_publish(channel, '{"level":"info","msg":"world"}')

        # Wait for messages to arrive (with timeout)
        try:
            await asyncio.wait_for(sub_task, timeout=10)
        except asyncio.TimeoutError:
            sub_task.cancel()
            try:
                await sub_task
            except asyncio.CancelledError:
                pass

        assert len(received) >= 2
        assert '{"level":"info","msg":"hello"}' in received
        assert '{"level":"info","msg":"world"}' in received


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------


class TestConnection:
    async def test_ensure_topic_idempotent(self):
        """ensure_topic can be called multiple times without error."""
        topic = f"test_ensure_{uuid.uuid4().hex[:8]}"
        await _kb.ensure_topic(topic)
        await _kb.ensure_topic(topic)  # Should not raise

    async def test_client_id_includes_worker_id(self):
        """client_id includes worker_id when configured."""
        _kb.configure(worker_id="42")
        cid = _kb._client_id("producer")
        assert "42" in cid
        assert "producer" in cid

        # Reset
        _kb._worker_id = None

    async def test_produce_and_topic_creation(self):
        """produce() auto-creates the topic if needed."""
        topic = f"test_produce_{uuid.uuid4().hex[:8]}"
        await _kb.produce(topic, b"test-value", key=b"test-key")
        # If we got here without error, produce and topic creation worked
