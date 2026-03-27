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


class TaskResult(BaseModel):
    """Sample result model for task tests."""

    status: str


@pytest.fixture
def pool():
    """Create a Pool for testing."""
    from sqlalchemy import create_engine

    engine = create_engine("sqlite:///:memory:")
    return ax.Pool(engine=engine)


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
    """Test that tasks can be deserialized using Task.from_serialized."""
    # Register a task to get a TaskDefinition
    @pool.task("test_task")
    async def handler(agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
        return TaskResult(status="success")

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


def test_task_round_trip(pool) -> None:
    """Test that tasks can be serialized and deserialized."""
    # Register task for deserialization
    @pool.task("round_trip_task")
    async def handler(agent_id: uuid.UUID, context: NestedContext) -> TaskResult:
        return TaskResult(status="success")

    task_def = pool._context.tasks["round_trip_task"]

    original_ctx = NestedContext(message="hello", nested={"key": "value"})
    original = ax.Task(
        task_name="round_trip_task",
        context=original_ctx,
        agent_id=uuid.uuid4(),
    )

    # Serialize → JSON → Deserialize
    serialized = original.model_dump_json()
    data = json.loads(serialized)
    deserialized = ax.Task.from_serialized(task_def, data)

    assert deserialized.task_name == original.task_name
    # Cast to access typed attributes (Task.context is typed as BaseModel)
    assert isinstance(deserialized.context, NestedContext)
    assert isinstance(original.context, NestedContext)
    assert deserialized.context.message == original.context.message
    assert deserialized.context.nested == original.context.nested
    assert deserialized.agent_id == original.agent_id


async def test_task_create_with_basemodel(monkeypatch) -> None:
    """Test Task.create() with a BaseModel context."""
    # Mock activity.create to avoid database dependency
    async def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)

    ctx = SampleContext(message="hello", value=42)
    task = await ax.Task.create("test_task", ctx)

    assert task.task_name == "test_task"
    # Context is the typed object
    assert isinstance(task.context, SampleContext)
    assert task.context.message == "hello"
    assert task.context.value == 42


async def test_task_create_preserves_nested(monkeypatch) -> None:
    """Test Task.create() preserves nested Pydantic models."""
    # Mock activity.create to avoid database dependency
    async def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)

    ctx = NestedContext(message="hello", nested={"key": "value"})
    task = await ax.Task.create("test_task", ctx)

    assert isinstance(task.context, NestedContext)
    assert task.context.message == "hello"
    assert task.context.nested == {"key": "value"}


def test_task_from_serialized(pool) -> None:
    """Test Task.from_serialized creates a task with typed context."""
    from agentexec.core.task import TaskDefinition

    @pool.task("test_task")
    async def handler(agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
        return TaskResult(status=f"Result: {context.message}")

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
    from unittest.mock import AsyncMock

    # Track activity updates
    activity_updates = []

    async def mock_update(**kwargs):
        activity_updates.append(kwargs)

    # Mock backend.state.set
    set_result_calls = []

    async def mock_state_set(key, value, ttl_seconds=None):
        set_result_calls.append((key, value, ttl_seconds))

    monkeypatch.setattr("agentexec.core.task.activity.update", mock_update)
    monkeypatch.setattr("agentexec.core.task.backend.state.set", mock_state_set)

    execution_result = TaskResult(status="success")

    @pool.task("async_task")
    async def async_handler(agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
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
    assert activity_updates[0]["percentage"] == 0
    # Second update marks task as completed
    assert activity_updates[1]["percentage"] == 100

    # Verify result was stored
    assert len(set_result_calls) == 1
    assert str(agent_id) in set_result_calls[0][0]  # Key contains agent_id
    assert set_result_calls[0][1] is not None  # Serialized result


async def test_task_execute_sync_handler(pool, monkeypatch) -> None:
    """Test Task.execute with a sync handler."""
    activity_updates = []

    async def mock_update(**kwargs):
        activity_updates.append(kwargs)

    async def mock_state_set(key, value, ttl_seconds=None):
        pass

    monkeypatch.setattr("agentexec.core.task.activity.update", mock_update)
    monkeypatch.setattr("agentexec.core.task.backend.state.set", mock_state_set)

    @pool.task("sync_task")
    def sync_handler(agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
        return TaskResult(status=f"Sync result: {context.message}")

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

    assert result is not None
    assert isinstance(result, TaskResult)
    assert result.status == "Sync result: test"
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
    from agentexec.activity.models import Status

    activity_updates = []

    async def mock_update(**kwargs):
        activity_updates.append(kwargs)

    async def mock_state_set(key, value, ttl_seconds=None):
        pass

    monkeypatch.setattr("agentexec.core.task.activity.update", mock_update)
    monkeypatch.setattr("agentexec.core.task.backend.state.set", mock_state_set)

    @pool.task("failing_task")
    async def failing_handler(agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
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
