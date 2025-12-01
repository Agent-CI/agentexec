"""Test WorkerPool implementation."""

import json
import uuid

import pytest
from fakeredis import aioredis as fake_aioredis
from pydantic import BaseModel
from sqlalchemy import create_engine

import agentexec as ax


class SampleContext(BaseModel):
    """Sample context for worker pool tests."""

    message: str
    value: int = 0


@pytest.fixture
def fake_redis(monkeypatch):
    """Setup fake async redis."""
    fake_redis = fake_aioredis.FakeRedis(decode_responses=True)

    def get_fake_redis():
        return fake_redis

    monkeypatch.setattr("agentexec.core.queue.get_redis", get_fake_redis)

    yield fake_redis


@pytest.fixture
def pool():
    """Create a WorkerPool for testing."""
    from sqlalchemy import create_engine

    engine = create_engine("sqlite:///:memory:")
    return ax.WorkerPool(engine=engine)


async def test_enqueue_task(fake_redis, pool, monkeypatch) -> None:
    """Test that tasks can be enqueued to Redis."""
    # Mock activity.create to avoid database dependency
    def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)

    # Register the task with pool
    @pool.task("test_task")
    async def handler(agent_id: uuid.UUID, context: SampleContext) -> None:
        pass

    # Enqueue a task with BaseModel context
    ctx = SampleContext(message="Hello World")
    task = await ax.enqueue("test_task", ctx)

    # Verify task was returned with typed context
    assert task is not None
    assert isinstance(task.agent_id, uuid.UUID)
    assert task.task_name == "test_task"
    assert isinstance(task.context, SampleContext)
    assert task.context.message == "Hello World"

    # Verify task was pushed to Redis
    task_json = await fake_redis.rpop(ax.CONF.queue_name)
    assert task_json is not None

    task_data = json.loads(task_json)
    assert task_data["task_name"] == "test_task"
    assert task_data["context"]["message"] == "Hello World"
    assert task_data["agent_id"] == str(task.agent_id)


async def test_enqueue_high_priority_task(fake_redis, pool, monkeypatch) -> None:
    """Test that high priority tasks are enqueued to the front."""
    def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)

    # Register tasks with pool
    @pool.task("low_task")
    async def low_handler(agent_id: uuid.UUID, context: SampleContext) -> None:
        pass

    @pool.task("high_task")
    async def high_handler(agent_id: uuid.UUID, context: SampleContext) -> None:
        pass

    # Enqueue low priority task
    ctx1 = SampleContext(message="low", value=1)
    task1 = await ax.enqueue("low_task", ctx1, priority=ax.Priority.LOW)

    # Enqueue high priority task
    ctx2 = SampleContext(message="high", value=2)
    task2 = await ax.enqueue("high_task", ctx2, priority=ax.Priority.HIGH)

    # High priority task should be at the end (RPUSH) so it's processed first (BRPOP)
    task_json = await fake_redis.rpop(ax.CONF.queue_name)
    task_data = json.loads(task_json)
    assert task_data["agent_id"] == str(task2.agent_id)


def test_task_registration(pool) -> None:
    """Test that @pool.task() registers tasks with the pool."""
    @pool.task("test_task")
    async def handler(agent_id: uuid.UUID, context: SampleContext) -> str:
        return f"Processed: {context.message}"

    # Verify task definition was registered
    assert "test_task" in pool._context.tasks
    task_def = pool._context.tasks["test_task"]
    assert task_def.handler == handler
    assert task_def.context_class == SampleContext


def test_worker_pool_initialization() -> None:
    """Test that WorkerPool initializes correctly."""
    engine = create_engine("sqlite:///:memory:")
    pool = ax.WorkerPool(engine=engine)

    assert pool._processes == []
    assert pool._context.tasks == {}
    assert pool._context.shutdown_event is not None


def test_task_registration_requires_typed_context(pool) -> None:
    """Test that task registration fails without typed context parameter."""
    with pytest.raises(TypeError, match="must have a 'context' parameter"):

        @pool.task("bad_task")
        async def handler_without_context(agent_id: uuid.UUID) -> None:
            pass


def test_task_registration_requires_basemodel_context(pool) -> None:
    """Test that task registration fails with non-BaseModel context type."""
    with pytest.raises(TypeError, match="must be a BaseModel subclass"):

        @pool.task("bad_task")
        async def handler_with_dict_context(agent_id: uuid.UUID, context: dict) -> None:
            pass
