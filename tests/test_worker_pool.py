"""Test WorkerPool implementation."""

import json
import uuid

import pytest
from sqlalchemy import create_engine

from agentexec import Priority, Task, WorkerPool, enqueue
from agentexec.core.redis_client import close_redis


@pytest.fixture(autouse=True)
def mock_redis(monkeypatch) -> None:
    """Use fakeredis for testing."""
    import fakeredis

    fake_redis = fakeredis.FakeRedis(decode_responses=True)

    def get_fake_redis():
        return fake_redis

    # Patch get_redis where it's used
    monkeypatch.setattr("agentexec.core.queue.get_redis", get_fake_redis)

    yield fake_redis

    # Cleanup
    fake_redis.flushdb()
    close_redis()


def test_enqueue_task(mock_redis, monkeypatch) -> None:
    """Test that tasks can be enqueued to Redis."""
    # Mock activity.create to avoid database dependency
    def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)

    # Enqueue a task
    from agentexec import CONF
    task = enqueue("test_task", {"message": "Hello World"})

    # Verify task was returned with agent_id
    assert task is not None
    assert isinstance(task.agent_id, uuid.UUID)
    assert task.task_name == "test_task"

    # Verify task was pushed to Redis using configured queue name
    task_json = mock_redis.rpop(CONF.queue_name)
    assert task_json is not None

    task_data = json.loads(task_json)
    assert task_data["task_name"] == "test_task"
    assert task_data["payload"]["message"] == "Hello World"
    assert task_data["agent_id"] == str(task.agent_id)


def test_enqueue_high_priority_task(mock_redis, monkeypatch) -> None:
    """Test that high priority tasks are enqueued to the front."""
    # Mock activity.create to avoid database dependency
    def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)

    # Enqueue low priority task
    from agentexec import CONF
    task1 = enqueue("low_task", {"value": 1}, priority=Priority.LOW)

    # Enqueue high priority task
    task2 = enqueue("high_task", {"value": 2}, priority=Priority.HIGH)

    # High priority task should be at the end (RPUSH) so it's processed first (BRPOP)
    task_json = mock_redis.rpop(CONF.queue_name)
    task_data = json.loads(task_json)
    assert task_data["agent_id"] == str(task2.agent_id)


def test_worker_pool_task_registration() -> None:
    """Test that task handlers are registered correctly."""
    engine = create_engine("sqlite:///:memory:")
    pool = WorkerPool(engine=engine)

    @pool.task("test_task")
    async def test_handler(agent_id: uuid.UUID, payload: dict) -> str:
        return f"Processed: {payload.get('message')}"

    # Verify handler was registered
    assert "test_task" in pool._handlers
    assert pool._handlers["test_task"] == test_handler


def test_worker_pool_initialization() -> None:
    """Test that WorkerPool initializes correctly."""
    engine = create_engine("sqlite:///:memory:")
    pool = WorkerPool(engine=engine)

    assert pool._handlers == {}
    assert pool._processes == []
    assert pool._shutdown_event is not None
