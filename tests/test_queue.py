"""Test task queue operations."""

import json
import uuid

import pytest
from fakeredis import aioredis as fake_aioredis
from pydantic import BaseModel

import agentexec as ax
from agentexec.core.queue import Priority, dequeue, enqueue


class SampleContext(BaseModel):
    """Sample context for queue tests."""

    message: str
    value: int = 0


@pytest.fixture
def fake_redis(monkeypatch):
    """Setup fake async redis."""
    fake_redis = fake_aioredis.FakeRedis(decode_responses=False)

    def get_fake_redis():
        return fake_redis

    monkeypatch.setattr("agentexec.core.queue.get_redis", get_fake_redis)

    yield fake_redis


@pytest.fixture
def mock_activity_create(monkeypatch):
    """Mock activity.create to avoid database dependency."""

    def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)


def test_priority_enum_values():
    """Test Priority enum has expected values."""
    assert Priority.HIGH.value == "high"
    assert Priority.LOW.value == "low"


async def test_enqueue_creates_task(fake_redis, mock_activity_create) -> None:
    """Test that enqueue creates and returns a task."""
    ctx = SampleContext(message="test", value=42)

    task = await enqueue("test_task", ctx)

    assert task is not None
    assert task.task_name == "test_task"
    assert isinstance(task.agent_id, uuid.UUID)
    assert task.context.message == "test"
    assert task.context.value == 42


async def test_enqueue_pushes_to_redis(fake_redis, mock_activity_create) -> None:
    """Test that enqueue pushes task data to Redis."""
    ctx = SampleContext(message="queued")

    task = await enqueue("test_task", ctx)

    # Check Redis has the task
    task_json = await fake_redis.rpop(ax.CONF.queue_name)
    assert task_json is not None

    task_data = json.loads(task_json)
    assert task_data["task_name"] == "test_task"
    assert task_data["context"]["message"] == "queued"
    assert task_data["agent_id"] == str(task.agent_id)


async def test_enqueue_low_priority_lpush(fake_redis, mock_activity_create) -> None:
    """Test that low priority tasks use LPUSH (back of queue)."""
    ctx = SampleContext(message="low")

    await enqueue("low_task", ctx, priority=Priority.LOW)

    # LPUSH adds to left, RPOP takes from right
    # So we should use LPOP to see it
    task_json = await fake_redis.lpop(ax.CONF.queue_name)
    assert task_json is not None


async def test_enqueue_high_priority_rpush(fake_redis, mock_activity_create) -> None:
    """Test that high priority tasks use RPUSH (front of queue)."""
    # First add a low priority task
    await enqueue("low_task", SampleContext(message="low"), priority=Priority.LOW)

    # Then add a high priority task
    await enqueue("high_task", SampleContext(message="high"), priority=Priority.HIGH)

    # High priority should be at the front (RPOP side)
    task_json = await fake_redis.rpop(ax.CONF.queue_name)
    task_data = json.loads(task_json)
    assert task_data["task_name"] == "high_task"


async def test_enqueue_custom_queue_name(fake_redis, mock_activity_create) -> None:
    """Test enqueue with custom queue name."""
    ctx = SampleContext(message="custom")

    await enqueue("test_task", ctx, queue_name="custom_queue")

    # Check custom queue
    task_json = await fake_redis.rpop("custom_queue")
    assert task_json is not None


async def test_dequeue_returns_task_data(fake_redis) -> None:
    """Test that dequeue returns parsed task data."""
    # Manually push a task to Redis
    task_data = {
        "task_name": "test_task",
        "context": {"message": "dequeued", "value": 100},
        "agent_id": str(uuid.uuid4()),
    }
    await fake_redis.lpush(ax.CONF.queue_name, json.dumps(task_data).encode())

    # Dequeue
    result = await dequeue(timeout=1)

    assert result is not None
    assert result["task_name"] == "test_task"
    assert result["context"]["message"] == "dequeued"
    assert result["context"]["value"] == 100


async def test_dequeue_returns_none_on_empty_queue(fake_redis) -> None:
    """Test that dequeue returns None when queue is empty."""
    # timeout=1 because timeout=0 means block indefinitely in Redis BRPOP
    result = await dequeue(timeout=1)

    assert result is None


async def test_dequeue_custom_queue_name(fake_redis) -> None:
    """Test dequeue with custom queue name."""
    task_data = {
        "task_name": "custom_task",
        "context": {"message": "test"},
        "agent_id": str(uuid.uuid4()),
    }
    await fake_redis.lpush("custom_queue", json.dumps(task_data).encode())

    result = await dequeue(queue_name="custom_queue", timeout=1)

    assert result is not None
    assert result["task_name"] == "custom_task"


async def test_dequeue_brpop_behavior(fake_redis) -> None:
    """Test that dequeue uses BRPOP (right side of list)."""
    # Push two tasks - first one goes to left
    task1 = {"task_name": "first", "context": {}, "agent_id": str(uuid.uuid4())}
    task2 = {"task_name": "second", "context": {}, "agent_id": str(uuid.uuid4())}

    await fake_redis.lpush(ax.CONF.queue_name, json.dumps(task1).encode())
    await fake_redis.lpush(ax.CONF.queue_name, json.dumps(task2).encode())

    # BRPOP should get the first task (oldest) from the right
    result = await dequeue(timeout=1)
    assert result["task_name"] == "first"


async def test_enqueue_dequeue_roundtrip(fake_redis, mock_activity_create) -> None:
    """Test complete enqueue -> dequeue cycle."""
    ctx = SampleContext(message="roundtrip", value=999)

    # Enqueue
    task = await enqueue("roundtrip_task", ctx)

    # Dequeue
    result = await dequeue(timeout=1)

    assert result is not None
    assert result["task_name"] == "roundtrip_task"
    assert result["context"]["message"] == "roundtrip"
    assert result["context"]["value"] == 999
    assert result["agent_id"] == str(task.agent_id)


async def test_multiple_enqueue_fifo_order(fake_redis, mock_activity_create) -> None:
    """Test that multiple low priority tasks maintain FIFO order."""
    contexts = [SampleContext(message=f"msg{i}") for i in range(3)]

    # Enqueue in order
    for i, ctx in enumerate(contexts):
        await enqueue(f"task_{i}", ctx, priority=Priority.LOW)

    # Dequeue should be in FIFO order
    for i in range(3):
        result = await dequeue(timeout=1)
        assert result["task_name"] == f"task_{i}"
