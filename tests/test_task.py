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
