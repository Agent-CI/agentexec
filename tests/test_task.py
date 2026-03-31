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


async def test_definition_execute_none_result_not_stored(pool, monkeypatch) -> None:
    """Handler returning None does not write to result storage."""
    state_set_calls = []

    async def mock_update(**kwargs):
        pass

    async def mock_state_set(key, value, ttl_seconds=None):
        state_set_calls.append(key)

    monkeypatch.setattr("agentexec.core.task.activity.update", mock_update)
    monkeypatch.setattr("agentexec.core.task.backend.state.set", mock_state_set)

    @pool.task("void_task")
    async def void_handler(agent_id: uuid.UUID, context: SampleContext) -> None:
        pass

    definition = pool._context.tasks["void_task"]
    task = ax.Task(
        task_name="void_task",
        context={"message": "test"},
        agent_id=uuid.uuid4(),
    )

    result = await definition.execute(task)
    assert result is None
    assert len(state_set_calls) == 0


async def test_definition_execute_stores_result_with_ttl(pool, monkeypatch) -> None:
    """Handler result is stored in state with the configured TTL."""
    from agentexec.config import CONF

    state_set_calls = []

    async def mock_update(**kwargs):
        pass

    async def mock_state_set(key, value, ttl_seconds=None):
        state_set_calls.append({"key": key, "ttl_seconds": ttl_seconds})

    monkeypatch.setattr("agentexec.core.task.activity.update", mock_update)
    monkeypatch.setattr("agentexec.core.task.backend.state.set", mock_state_set)

    @pool.task("result_task")
    async def result_handler(agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
        return TaskResult(status="done")

    definition = pool._context.tasks["result_task"]
    task = ax.Task(
        task_name="result_task",
        context={"message": "test"},
        agent_id=uuid.uuid4(),
    )

    await definition.execute(task)
    assert len(state_set_calls) == 1
    assert state_set_calls[0]["ttl_seconds"] == CONF.result_ttl
    assert str(task.agent_id) in state_set_calls[0]["key"]


async def test_definition_execute_hydrates_context(pool, monkeypatch) -> None:
    """execute() passes a typed context model to the handler, not a raw dict."""
    received_context = []

    async def mock_update(**kwargs):
        pass

    monkeypatch.setattr("agentexec.core.task.activity.update", mock_update)

    @pool.task("typed_task")
    async def typed_handler(agent_id: uuid.UUID, context: SampleContext) -> None:
        received_context.append(context)

    definition = pool._context.tasks["typed_task"]
    task = ax.Task(
        task_name="typed_task",
        context={"message": "typed", "value": 7},
        agent_id=uuid.uuid4(),
    )

    await definition.execute(task)
    assert len(received_context) == 1
    assert isinstance(received_context[0], SampleContext)
    assert received_context[0].message == "typed"
    assert received_context[0].value == 7


async def test_definition_execute_passes_agent_id(pool, monkeypatch) -> None:
    """execute() passes the task's agent_id to the handler."""
    received_ids = []

    async def mock_update(**kwargs):
        pass

    monkeypatch.setattr("agentexec.core.task.activity.update", mock_update)

    @pool.task("id_task")
    async def id_handler(agent_id: uuid.UUID, context: SampleContext) -> None:
        received_ids.append(agent_id)

    definition = pool._context.tasks["id_task"]
    expected_id = uuid.uuid4()
    task = ax.Task(
        task_name="id_task",
        context={"message": "test"},
        agent_id=expected_id,
    )

    await definition.execute(task)
    assert received_ids == [expected_id]


async def test_definition_execute_error_reraises(pool, monkeypatch) -> None:
    """execute() re-raises the original exception after marking activity as errored."""
    async def mock_update(**kwargs):
        pass

    monkeypatch.setattr("agentexec.core.task.activity.update", mock_update)

    @pool.task("reraise_task")
    async def bad_handler(agent_id: uuid.UUID, context: SampleContext):
        raise RuntimeError("original error")

    definition = pool._context.tasks["reraise_task"]
    task = ax.Task(
        task_name="reraise_task",
        context={"message": "test"},
        agent_id=uuid.uuid4(),
    )

    with pytest.raises(RuntimeError, match="original error"):
        await definition.execute(task)


async def test_definition_execute_bad_context_raises(pool, monkeypatch) -> None:
    """execute() raises ValidationError when context doesn't match the registered type."""
    async def mock_update(**kwargs):
        pass

    monkeypatch.setattr("agentexec.core.task.activity.update", mock_update)

    @pool.task("strict_task")
    async def strict_handler(agent_id: uuid.UUID, context: SampleContext):
        pass

    definition = pool._context.tasks["strict_task"]
    task = ax.Task(
        task_name="strict_task",
        context={"wrong_field": "oops"},  # missing required 'message'
        agent_id=uuid.uuid4(),
    )

    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        await definition.execute(task)
