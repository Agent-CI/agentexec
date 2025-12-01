"""Test Task data structure and serialization."""

import json
import uuid

import pytest
from pydantic import BaseModel

import agentexec as ax


class SampleContext(BaseModel):
    """Sample context for task tests."""

    message: str
    value: int = 0


class NestedContext(BaseModel):
    """Sample context with nested data."""

    message: str
    nested: dict


@pytest.fixture
def pool():
    """Create a WorkerPool for testing."""
    from sqlalchemy import create_engine

    engine = create_engine("sqlite:///:memory:")
    return ax.WorkerPool(engine=engine)


def test_task_creation_with_basemodel() -> None:
    """Test that tasks can be created with BaseModel context."""
    ctx = SampleContext(message="hello", value=42)
    task = ax.Task(task_name="test_task", context=ctx, agent_id=uuid.uuid4())

    assert task.task_name == "test_task"
    assert task.context == ctx
    assert task.context.message == "hello"
    assert task.context.value == 42


def test_task_with_custom_agent_id() -> None:
    """Test that tasks can be created with a custom agent_id."""
    custom_id = uuid.uuid4()
    ctx = SampleContext(message="hello")
    task = ax.Task(
        task_name="test_task",
        context=ctx,
        agent_id=custom_id,
    )

    assert task.agent_id == custom_id


def test_task_serialization() -> None:
    """Test that tasks can be serialized to JSON."""
    agent_id = uuid.uuid4()
    ctx = SampleContext(message="hello", value=42)
    task = ax.Task(
        task_name="test_task",
        context=ctx,
        agent_id=agent_id,
    )

    task_json = task.model_dump_json()
    task_data = json.loads(task_json)

    assert task_data["task_name"] == "test_task"
    assert task_data["context"]["message"] == "hello"
    assert task_data["context"]["value"] == 42
    assert task_data["agent_id"] == str(agent_id)


def test_task_deserialization(pool) -> None:
    """Test that tasks can be deserialized using context_class."""
    # Register a task to get a TaskDefinition
    @pool.task("test_task")
    async def handler(agent_id: uuid.UUID, context: SampleContext) -> None:
        pass

    task_def = pool._context.tasks["test_task"]

    agent_id = uuid.uuid4()
    data = {
        "task_name": "test_task",
        "context": {"message": "hello", "value": 42},
        "agent_id": str(agent_id),
    }

    task = ax.Task(
        task_name=data["task_name"],
        context=task_def.context_class.model_validate(data["context"]),
        agent_id=data["agent_id"],
    )

    assert task.task_name == "test_task"
    assert isinstance(task.context, SampleContext)
    assert task.context.message == "hello"
    assert task.context.value == 42
    assert task.agent_id == agent_id


def test_task_round_trip(pool) -> None:
    """Test that tasks can be serialized and deserialized."""
    # Register task for deserialization
    @pool.task("test_task")
    async def handler(agent_id: uuid.UUID, context: NestedContext) -> None:
        pass

    task_def = pool._context.tasks["test_task"]

    original_ctx = NestedContext(message="hello", nested={"key": "value"})
    original = ax.Task(
        task_name="test_task",
        context=original_ctx,
        agent_id=uuid.uuid4(),
    )

    # Serialize → JSON → Deserialize
    serialized = original.model_dump_json()
    data = json.loads(serialized)
    deserialized = ax.Task(
        task_name=data["task_name"],
        context=task_def.context_class.model_validate(data["context"]),
        agent_id=data["agent_id"],
    )

    assert deserialized.task_name == original.task_name
    assert deserialized.context.message == original.context.message
    assert deserialized.context.nested == original.context.nested
    assert deserialized.agent_id == original.agent_id


def test_task_create_with_basemodel(monkeypatch) -> None:
    """Test Task.create() with a BaseModel context."""
    # Mock activity.create to avoid database dependency
    def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)

    ctx = SampleContext(message="hello", value=42)
    task = ax.Task.create("test_task", ctx)

    assert task.task_name == "test_task"
    # Context is the typed object
    assert isinstance(task.context, SampleContext)
    assert task.context.message == "hello"
    assert task.context.value == 42


def test_task_create_preserves_nested(monkeypatch) -> None:
    """Test Task.create() preserves nested Pydantic models."""
    # Mock activity.create to avoid database dependency
    def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)

    ctx = NestedContext(message="hello", nested={"key": "value"})
    task = ax.Task.create("test_task", ctx)

    assert isinstance(task.context, NestedContext)
    assert task.context.message == "hello"
    assert task.context.nested == {"key": "value"}


def test_task_from_serialized(pool) -> None:
    """Test Task.from_serialized creates a task with typed context."""
    from agentexec.core.task import TaskDefinition

    @pool.task("test_task")
    async def handler(agent_id: uuid.UUID, context: SampleContext) -> str:
        return f"Result: {context.message}"

    task_def = pool._context.tasks["test_task"]
    agent_id = uuid.uuid4()

    data = {
        "task_name": "test_task",
        "context": {"message": "hello", "value": 42},
        "agent_id": str(agent_id),
    }

    task = ax.Task.from_serialized(task_def, data)

    assert task.task_name == "test_task"
    assert isinstance(task.context, SampleContext)
    assert task.context.message == "hello"
    assert task.context.value == 42
    assert task.agent_id == agent_id
    assert task._definition is task_def


async def test_task_execute_async_handler(pool, monkeypatch) -> None:
    """Test Task.execute with an async handler."""
    from unittest.mock import AsyncMock, MagicMock
    import pickle

    # Track activity updates
    activity_updates = []

    def mock_update(**kwargs):
        activity_updates.append(kwargs)

    # Mock Redis
    mock_redis = MagicMock()
    mock_redis.set = AsyncMock()

    monkeypatch.setattr("agentexec.core.task.activity.update", mock_update)
    monkeypatch.setattr("agentexec.core.task.get_redis", lambda: mock_redis)

    execution_result = {"status": "success"}

    @pool.task("async_task")
    async def async_handler(agent_id: uuid.UUID, context: SampleContext) -> dict:
        return execution_result

    task_def = pool._context.tasks["async_task"]
    agent_id = uuid.uuid4()

    task = ax.Task.from_serialized(
        task_def,
        {
            "task_name": "async_task",
            "context": {"message": "test"},
            "agent_id": str(agent_id),
        },
    )

    result = await task.execute()

    assert result == execution_result
    # Verify activity was updated (started and completed)
    assert len(activity_updates) == 2
    # First update marks task as started
    assert activity_updates[0]["completion_percentage"] == 0
    # Second update marks task as completed
    assert activity_updates[1]["completion_percentage"] == 100

    # Verify result was stored in Redis
    mock_redis.set.assert_called_once()
    call_args = mock_redis.set.call_args
    assert call_args[0][0] == f"result:{agent_id}"
    assert pickle.loads(call_args[0][1]) == execution_result


async def test_task_execute_sync_handler(pool, monkeypatch) -> None:
    """Test Task.execute with a sync handler."""
    from unittest.mock import AsyncMock, MagicMock

    activity_updates = []

    def mock_update(**kwargs):
        activity_updates.append(kwargs)

    mock_redis = MagicMock()
    mock_redis.set = AsyncMock()

    monkeypatch.setattr("agentexec.core.task.activity.update", mock_update)
    monkeypatch.setattr("agentexec.core.task.get_redis", lambda: mock_redis)

    @pool.task("sync_task")
    def sync_handler(agent_id: uuid.UUID, context: SampleContext) -> str:
        return f"Sync result: {context.message}"

    task_def = pool._context.tasks["sync_task"]
    agent_id = uuid.uuid4()

    task = ax.Task.from_serialized(
        task_def,
        {
            "task_name": "sync_task",
            "context": {"message": "test"},
            "agent_id": str(agent_id),
        },
    )

    result = await task.execute()

    assert result == "Sync result: test"
    assert len(activity_updates) == 2


async def test_task_execute_without_definition_raises() -> None:
    """Test Task.execute raises RuntimeError if not bound to definition."""
    task = ax.Task(
        task_name="test_task",
        context=SampleContext(message="test"),
        agent_id=uuid.uuid4(),
    )

    with pytest.raises(RuntimeError, match="must be bound to a definition"):
        await task.execute()


async def test_task_execute_error_marks_activity_errored(pool, monkeypatch) -> None:
    """Test Task.execute marks activity as errored on exception."""
    from unittest.mock import AsyncMock, MagicMock
    from agentexec.activity.models import Status

    activity_updates = []

    def mock_update(**kwargs):
        activity_updates.append(kwargs)

    mock_redis = MagicMock()
    mock_redis.set = AsyncMock()

    monkeypatch.setattr("agentexec.core.task.activity.update", mock_update)
    monkeypatch.setattr("agentexec.core.task.get_redis", lambda: mock_redis)

    @pool.task("failing_task")
    async def failing_handler(agent_id: uuid.UUID, context: SampleContext) -> None:
        raise ValueError("Task failed!")

    task_def = pool._context.tasks["failing_task"]
    agent_id = uuid.uuid4()

    task = ax.Task.from_serialized(
        task_def,
        {
            "task_name": "failing_task",
            "context": {"message": "test"},
            "agent_id": str(agent_id),
        },
    )

    # execute() catches the exception and marks activity as errored, returns None
    result = await task.execute()

    assert result is None  # Handler exception results in None return
    # First update marks started, second marks errored
    assert len(activity_updates) == 2
    assert activity_updates[1]["status"] == Status.ERROR
    assert "Task failed!" in activity_updates[1]["message"]
