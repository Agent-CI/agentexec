"""Test WorkerPool implementation."""

import json
import uuid

import pytest
from pydantic import BaseModel

import agentexec as ax


class SampleContext(BaseModel):
    """Sample context for worker pool tests."""

    message: str
    value: int = 0


class TaskResult(BaseModel):
    """Sample result for worker pool tests."""

    status: str = "success"


@pytest.fixture
def mock_state_backend(monkeypatch):
    """Mock the state backend for queue operations."""
    queue_data = []

    def mock_lpush(key, value):
        queue_data.insert(0, value)
        return len(queue_data)

    def mock_rpush(key, value):
        queue_data.append(value)
        return len(queue_data)

    def pop_right():
        return queue_data.pop() if queue_data else None

    monkeypatch.setattr("agentexec.state.backend.lpush", mock_lpush)
    monkeypatch.setattr("agentexec.state.backend.rpush", mock_rpush)

    return {"queue": queue_data, "pop": pop_right}


@pytest.fixture
def pool():
    """Create a WorkerPool for testing."""
    from sqlalchemy import create_engine

    engine = create_engine("sqlite:///:memory:")
    return ax.WorkerPool(engine=engine)


async def test_enqueue_task(mock_state_backend, pool, monkeypatch) -> None:
    """Test that tasks can be enqueued."""
    # Mock activity.create to avoid database dependency
    def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)

    # Register the task with pool
    @pool.task("test_task")
    async def handler(agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
        return TaskResult()

    # Enqueue a task with BaseModel context
    ctx = SampleContext(message="Hello World")
    task = await ax.enqueue("test_task", ctx)

    # Verify task was returned with typed context
    assert task is not None
    assert isinstance(task.agent_id, uuid.UUID)
    assert task.task_name == "test_task"
    assert isinstance(task.context, SampleContext)
    assert task.context.message == "Hello World"

    # Verify task was pushed to queue
    task_json = mock_state_backend["pop"]()
    assert task_json is not None

    task_data = json.loads(task_json)
    assert task_data["task_name"] == "test_task"
    assert task_data["context"]["message"] == "Hello World"
    assert task_data["agent_id"] == str(task.agent_id)


async def test_enqueue_high_priority_task(mock_state_backend, pool, monkeypatch) -> None:
    """Test that high priority tasks are enqueued to the front."""
    def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)

    # Register tasks with pool
    @pool.task("low_task")
    async def low_handler(agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
        return TaskResult()

    @pool.task("high_task")
    async def high_handler(agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
        return TaskResult()

    # Enqueue low priority task
    ctx1 = SampleContext(message="low", value=1)
    task1 = await ax.enqueue("low_task", ctx1, priority=ax.Priority.LOW)

    # Enqueue high priority task
    ctx2 = SampleContext(message="high", value=2)
    task2 = await ax.enqueue("high_task", ctx2, priority=ax.Priority.HIGH)

    # High priority task should be at the end (RPUSH) so it's processed first (BRPOP)
    task_json = mock_state_backend["pop"]()
    task_data = json.loads(task_json)
    assert task_data["agent_id"] == str(task2.agent_id)


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


def test_worker_pool_requires_engine_or_database_url() -> None:
    """Test that WorkerPool requires either engine or database_url."""
    with pytest.raises(ValueError, match="Either engine or database_url must be provided"):
        ax.WorkerPool()


def test_worker_pool_with_database_url() -> None:
    """Test that WorkerPool can be created with database_url."""
    pool = ax.WorkerPool(database_url="sqlite:///:memory:")

    assert pool._context.database_url == "sqlite:///:memory:"
    assert pool._processes == []


def test_worker_pool_with_custom_queue_name() -> None:
    """Test that WorkerPool can use a custom queue name."""
    pool = ax.WorkerPool(
        database_url="sqlite:///:memory:",
        queue_name="custom_queue",
    )

    assert pool._context.queue_name == "custom_queue"


async def test_worker_dequeue_task(pool, monkeypatch) -> None:
    """Test Worker._dequeue_task method."""
    from agentexec.worker.pool import Worker, WorkerContext
    from agentexec.worker.event import StateEvent

    @pool.task("test_task")
    async def handler(agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
        return TaskResult()

    context = WorkerContext(
        database_url="sqlite:///:memory:",
        shutdown_event=StateEvent("shutdown", "test-worker"),
        tasks=pool._context.tasks,
        queue_name="test_queue",
    )

    worker = Worker(worker_id=0, context=context)

    # Mock dequeue to return task data
    agent_id = uuid.uuid4()
    task_data = {
        "task_name": "test_task",
        "context": {"message": "test", "value": 42},
        "agent_id": str(agent_id),
    }

    async def mock_dequeue(**kwargs):
        return task_data

    monkeypatch.setattr("agentexec.worker.pool.dequeue", mock_dequeue)

    task = await worker._dequeue_task()

    assert task is not None
    assert task.task_name == "test_task"
    assert isinstance(task.context, SampleContext)
    assert task.context.message == "test"
    assert task.agent_id == agent_id


async def test_worker_dequeue_task_returns_none_on_empty_queue(pool, monkeypatch) -> None:
    """Test Worker._dequeue_task returns None when queue is empty."""
    from agentexec.worker.pool import Worker, WorkerContext
    from agentexec.worker.event import StateEvent

    context = WorkerContext(
        database_url="sqlite:///:memory:",
        shutdown_event=StateEvent("shutdown", "test-worker"),
        tasks=pool._context.tasks,
        queue_name="test_queue",
    )

    worker = Worker(worker_id=0, context=context)

    async def mock_dequeue(**kwargs):
        return None

    monkeypatch.setattr("agentexec.worker.pool.dequeue", mock_dequeue)

    task = await worker._dequeue_task()

    assert task is None


def test_worker_pool_shutdown_with_no_processes(pool, monkeypatch) -> None:
    """Test shutdown when no processes have been started."""
    # Mock the shutdown event to avoid Redis dependency
    from unittest.mock import MagicMock

    pool._context.shutdown_event = MagicMock()

    # Should not raise even with empty process list
    pool.shutdown(timeout=1)

    assert pool._processes == []
    pool._context.shutdown_event.set.assert_called_once()


def test_get_pool_id() -> None:
    """Test _get_pool_id generates unique IDs."""
    from agentexec.worker.pool import _get_pool_id

    id1 = _get_pool_id()
    id2 = _get_pool_id()

    assert id1 != id2
