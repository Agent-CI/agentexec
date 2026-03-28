import json
import uuid
from unittest.mock import AsyncMock

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
    """Mock the queue ops for push operations."""
    queue_data = []

    async def mock_queue_push(queue_name, value, *, high_priority=False, partition_key=None):
        if high_priority:
            queue_data.append(value)
        else:
            queue_data.insert(0, value)

    def pop_right():
        return queue_data.pop() if queue_data else None

    monkeypatch.setattr("agentexec.state.backend.queue.push", mock_queue_push)

    return {"queue": queue_data, "pop": pop_right}


@pytest.fixture
def pool():
    """Create a Pool for testing."""
    from sqlalchemy import create_engine

    engine = create_engine("sqlite:///:memory:")
    return ax.Pool(engine=engine)


async def test_enqueue_task(mock_state_backend, pool, monkeypatch) -> None:
    """Test that tasks can be enqueued."""
    async def mock_create(*args, **kwargs):
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
    assert task.context["message"] == "Hello World"

    # Verify task was pushed to queue
    task_json = mock_state_backend["pop"]()
    assert task_json is not None

    task_data = json.loads(task_json)
    assert task_data["task_name"] == "test_task"
    assert task_data["context"]["message"] == "Hello World"
    assert task_data["agent_id"] == str(task.agent_id)


async def test_enqueue_high_priority_task(mock_state_backend, pool, monkeypatch) -> None:
    """Test that high priority tasks are enqueued to the front."""
    async def mock_create(*args, **kwargs):
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

    # High priority task should be at the end (popped first)
    task_json = mock_state_backend["pop"]()
    task_data = json.loads(task_json)
    assert task_data["agent_id"] == str(task2.agent_id)


async def test_add_task_registers_handler(mock_state_backend, pool, monkeypatch) -> None:
    """Test that pool.add_task() registers a task handler."""
    async def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)

    # Register the task with add_task instead of decorator
    async def handler(*, agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
        return TaskResult()

    pool.add_task("added_task", handler)

    # Verify task was registered
    assert "added_task" in pool._context.tasks

    # Enqueue and verify it works
    ctx = SampleContext(message="Added via add_task")
    task = await ax.enqueue("added_task", ctx)

    assert task is not None
    assert task.task_name == "added_task"
    assert task.context["message"] == "Added via add_task"


def test_add_task_duplicate_raises(pool) -> None:
    """Test that add_task raises ValueError for duplicate task names."""
    async def handler(*, agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
        return TaskResult()

    pool.add_task("duplicate_task", handler)

    with pytest.raises(ValueError, match="already registered"):
        pool.add_task("duplicate_task", handler)


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


def test_pool_requires_engine_or_database_url() -> None:
    """Test that Pool requires either engine or database_url."""
    with pytest.raises(ValueError, match="Either engine or database_url must be provided"):
        ax.Pool()


def test_pool_with_database_url() -> None:
    """Test that Pool can be created with database_url."""
    pool = ax.Pool(database_url="sqlite:///:memory:")

    assert pool._context.database_url == "sqlite:///:memory:"
    assert pool._processes == []


def test_pool_with_custom_queue_name() -> None:
    """Test that Pool can use a custom queue name."""
    pool = ax.Pool(
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

    # Mock queue_pop to return task data
    agent_id = uuid.uuid4()
    task_data = {
        "task_name": "test_task",
        "context": {"message": "test", "value": 42},
        "agent_id": str(agent_id),
    }

    async def mock_queue_pop(*args, **kwargs):
        return task_data

    monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_queue_pop)

    from agentexec.core.queue import dequeue
    task = await dequeue(queue_name="test_queue", timeout=1)

    assert task is not None
    assert task.task_name == "test_task"
    assert task.context == {"message": "test", "value": 42}
    assert task.agent_id == agent_id


async def test_dequeue_returns_none_on_empty_queue(pool, monkeypatch) -> None:
    """Test dequeue returns None when queue is empty."""

    async def mock_queue_pop(*args, **kwargs):
        return None

    monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_queue_pop)

    from agentexec.core.queue import dequeue
    task = await dequeue(queue_name="test_queue", timeout=1)

    assert task is None


async def test_worker_pool_shutdown_with_no_processes(pool) -> None:
    """Test shutdown when no processes have been started."""
    from unittest.mock import AsyncMock

    pool._context.shutdown_event = AsyncMock()

    # Should not raise even with empty process list
    await pool.shutdown(timeout=1)

    assert pool._processes == []
    pool._context.shutdown_event.set.assert_called_once()


def test_get_pool_id() -> None:
    """Test _get_pool_id generates unique IDs."""
    from agentexec.worker.pool import _get_pool_id

    id1 = _get_pool_id()
    id2 = _get_pool_id()

    assert id1 != id2
