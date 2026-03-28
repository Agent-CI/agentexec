import json
import uuid

import pytest
from pydantic import BaseModel

import agentexec as ax
from agentexec.core.task import TaskDefinition


class SampleContext(BaseModel):
    message: str
    value: int = 0


class NestedContext(BaseModel):
    message: str
    nested: dict


class TaskResult(BaseModel):
    status: str


@pytest.fixture
def pool():
    from sqlalchemy import create_engine
    engine = create_engine("sqlite:///:memory:")
    return ax.Pool(engine=engine)


def test_task_serialization() -> None:
    """Task serializes to JSON with context as a dict."""
    agent_id = uuid.uuid4()
    task = ax.Task(
        task_name="test_task",
        context={"message": "hello", "value": 42},
        agent_id=agent_id,
    )

    task_json = task.model_dump_json()
    task_data = json.loads(task_json)

    assert task_data["task_name"] == "test_task"
    assert task_data["context"]["message"] == "hello"
    assert task_data["context"]["value"] == 42
    assert task_data["agent_id"] == str(agent_id)


def test_task_deserialization() -> None:
    """Task deserializes from raw queue data."""
    agent_id = uuid.uuid4()
    data = {
        "task_name": "test_task",
        "context": {"message": "hello", "value": 42},
        "agent_id": str(agent_id),
    }

    task = ax.Task.model_validate(data)

    assert task.task_name == "test_task"
    assert task.context == {"message": "hello", "value": 42}
    assert task.agent_id == agent_id


def test_task_round_trip() -> None:
    """Task survives serialize → JSON → deserialize."""
    original = ax.Task(
        task_name="round_trip_task",
        context={"message": "hello", "nested": {"key": "value"}},
        agent_id=uuid.uuid4(),
    )

    serialized = original.model_dump_json()
    deserialized = ax.Task.model_validate_json(serialized)

    assert deserialized.task_name == original.task_name
    assert deserialized.context == original.context
    assert deserialized.agent_id == original.agent_id


async def test_task_create_with_basemodel(monkeypatch) -> None:
    """Task.create() serializes context to dict."""
    async def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)

    ctx = SampleContext(message="hello", value=42)
    task = await ax.Task.create("test_task", ctx)

    assert task.task_name == "test_task"
    assert task.context == {"message": "hello", "value": 42}


async def test_task_create_preserves_nested(monkeypatch) -> None:
    """Task.create() preserves nested structures in the dict."""
    async def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)

    ctx = NestedContext(message="hello", nested={"key": "value"})
    task = await ax.Task.create("test_task", ctx)

    assert task.context == {"message": "hello", "nested": {"key": "value"}}


def test_definition_hydrates_context(pool) -> None:
    """TaskDefinition.hydrate_context validates dict into typed model."""
    @pool.task("test_task")
    async def handler(agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
        return TaskResult(status="success")

    definition = pool._context.tasks["test_task"]
    typed = definition.hydrate_context({"message": "hello", "value": 42})

    assert isinstance(typed, SampleContext)
    assert typed.message == "hello"
    assert typed.value == 42


async def test_definition_execute_async(pool, monkeypatch) -> None:
    """TaskDefinition.execute() runs async handler and tracks activity."""
    activity_updates = []

    async def mock_update(**kwargs):
        activity_updates.append(kwargs)

    async def mock_state_set(key, value, ttl_seconds=None):
        pass

    monkeypatch.setattr("agentexec.core.task.activity.update", mock_update)
    monkeypatch.setattr("agentexec.core.task.backend.state.set", mock_state_set)

    execution_result = TaskResult(status="success")

    @pool.task("async_task")
    async def async_handler(agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
        return execution_result

    definition = pool._context.tasks["async_task"]
    task = ax.Task(
        task_name="async_task",
        context={"message": "test"},
        agent_id=uuid.uuid4(),
    )

    result = await definition.execute(task)

    assert result == execution_result
    assert len(activity_updates) == 2
    assert activity_updates[0]["percentage"] == 0
    assert activity_updates[1]["percentage"] == 100


async def test_definition_execute_sync(pool, monkeypatch) -> None:
    """TaskDefinition.execute() runs sync handler."""
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

    definition = pool._context.tasks["sync_task"]
    task = ax.Task(
        task_name="sync_task",
        context={"message": "test"},
        agent_id=uuid.uuid4(),
    )

    result = await definition.execute(task)

    assert result is not None
    assert isinstance(result, TaskResult)
    assert result.status == "Sync result: test"
    assert len(activity_updates) == 2


async def test_definition_execute_error(pool, monkeypatch) -> None:
    """TaskDefinition.execute() marks activity as errored on exception."""
    from agentexec.activity.status import Status

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

    definition = pool._context.tasks["failing_task"]
    task = ax.Task(
        task_name="failing_task",
        context={"message": "test"},
        agent_id=uuid.uuid4(),
    )

    with pytest.raises(ValueError, match="Task failed!"):
        await definition.execute(task)

    assert len(activity_updates) == 2
    assert activity_updates[1]["status"] == Status.ERROR
    assert "Task failed!" in activity_updates[1]["message"]
