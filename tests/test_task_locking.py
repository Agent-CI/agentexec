import uuid

import pytest
from fakeredis import aioredis as fake_aioredis
from pydantic import BaseModel

import agentexec as ax
from agentexec.config import CONF
from agentexec.state import backend
from agentexec.core.task import TaskDefinition


class UserContext(BaseModel):
    """Context with user_id for lock key tests."""

    user_id: str
    message: str = ""


class TaskResult(BaseModel):
    status: str


@pytest.fixture
def pool():
    """Create a Pool for testing."""
    from sqlalchemy import create_engine

    engine = create_engine("sqlite:///:memory:")
    return ax.Pool(engine=engine)


@pytest.fixture
def fake_redis(monkeypatch):
    """Setup fake redis for state backend."""
    fake = fake_aioredis.FakeRedis(decode_responses=False)
    monkeypatch.setattr(backend, "_client", fake)
    yield fake


def test_task_definition_lock_key_default():
    """TaskDefinition.lock_key defaults to None."""

    async def handler(agent_id: uuid.UUID, context: UserContext) -> TaskResult:
        return TaskResult(status="ok")

    defn = TaskDefinition(name="test", handler=handler)
    assert defn.lock_key is None


def test_task_definition_lock_key_set():
    """TaskDefinition stores lock_key when provided."""

    async def handler(agent_id: uuid.UUID, context: UserContext) -> TaskResult:
        return TaskResult(status="ok")

    defn = TaskDefinition(name="test", handler=handler, lock_key="user:{user_id}")
    assert defn.lock_key == "user:{user_id}"


def test_pool_task_decorator_with_lock_key(pool):
    """@pool.task() passes lock_key to TaskDefinition."""

    @pool.task("locked_task", lock_key="user:{user_id}")
    async def handler(agent_id: uuid.UUID, context: UserContext) -> TaskResult:
        return TaskResult(status="ok")

    defn = pool._context.tasks["locked_task"]
    assert defn.lock_key == "user:{user_id}"


def test_pool_task_decorator_without_lock_key(pool):
    """@pool.task() without lock_key leaves it as None."""

    @pool.task("unlocked_task")
    async def handler(agent_id: uuid.UUID, context: UserContext) -> TaskResult:
        return TaskResult(status="ok")

    defn = pool._context.tasks["unlocked_task"]
    assert defn.lock_key is None


def test_pool_add_task_with_lock_key(pool):
    """pool.add_task() passes lock_key to TaskDefinition."""

    async def handler(agent_id: uuid.UUID, context: UserContext) -> TaskResult:
        return TaskResult(status="ok")

    pool.add_task("locked_task", handler, lock_key="user:{user_id}")

    defn = pool._context.tasks["locked_task"]
    assert defn.lock_key == "user:{user_id}"


def test_get_lock_key_evaluates_template(pool):
    """definition.get_lock_key() evaluates template against context."""

    @pool.task("locked_task", lock_key="user:{user_id}")
    async def handler(agent_id: uuid.UUID, context: UserContext) -> TaskResult:
        return TaskResult(status="ok")

    definition = pool._context.tasks["locked_task"]
    assert definition.get_lock_key({"user_id": "42", "message": "hello"}) == "user:42"


def test_get_lock_key_returns_none_when_no_lock(pool):
    """definition.get_lock_key() returns None when no lock_key configured."""

    @pool.task("unlocked_task")
    async def handler(agent_id: uuid.UUID, context: UserContext) -> TaskResult:
        return TaskResult(status="ok")

    definition = pool._context.tasks["unlocked_task"]
    assert definition.get_lock_key({"user_id": "42"}) is None


def test_get_lock_key_raises_on_missing_field(pool):
    """definition.get_lock_key() raises KeyError if template references missing field."""

    @pool.task("bad_template", lock_key="org:{organization_id}")
    async def handler(agent_id: uuid.UUID, context: UserContext) -> TaskResult:
        return TaskResult(status="ok")

    definition = pool._context.tasks["bad_template"]
    with pytest.raises(KeyError):
        definition.get_lock_key({"user_id": "42"})


async def test_acquire_lock_success(fake_redis):
    """acquire_lock returns True when lock is free."""
    result = await backend.state.acquire_lock("user:42", uuid.UUID(int=1))
    assert result is True


async def test_acquire_lock_already_held(fake_redis):
    """acquire_lock returns False when lock is already held."""
    await backend.state.acquire_lock("user:42", uuid.UUID(int=1))
    result = await backend.state.acquire_lock("user:42", uuid.UUID(int=2))
    assert result is False


async def test_release_lock(fake_redis):
    """release_lock frees the lock so it can be re-acquired."""
    await backend.state.acquire_lock("user:42", uuid.UUID(int=1))
    await backend.state.release_lock("user:42")

    result = await backend.state.acquire_lock("user:42", uuid.UUID(int=2))
    assert result is True


async def test_release_lock_nonexistent(fake_redis):
    """release_lock on a non-existent key returns 0."""
    result = await backend.state.release_lock("nonexistent")
    assert result == 0


async def test_lock_key_uses_prefix(fake_redis):
    """Lock keys are prefixed with agentexec:lock:."""
    await backend.state.acquire_lock("user:42", uuid.UUID(int=1))

    value = await fake_redis.get("agentexec:lock:user:42")
    assert value is not None


async def test_requeue_pushes_to_back(fake_redis, monkeypatch):
    """requeue() pushes task to the back of the queue (lpush)."""

    async def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)

    # Enqueue a normal task first
    task1 = await ax.enqueue("task_1", UserContext(user_id="1", message="first"))

    # Push a second task directly (simulating a requeue)
    task2 = ax.Task(
        task_name="task_2",
        context={"user_id": "2", "message": "requeued"},
        agent_id=uuid.uuid4(),
    )
    await backend.queue.push(ax.CONF.queue_prefix, task2.model_dump_json())

    result1 = await backend.queue.pop(ax.CONF.queue_prefix, timeout=1)
    assert result1 is not None
    assert result1["task_name"] == "task_1"

    result2 = await backend.queue.pop(ax.CONF.queue_prefix, timeout=1)
    assert result2 is not None
    assert result2["task_name"] == "task_2"
