import json
import multiprocessing as mp
import uuid
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

import agentexec as ax
from agentexec.state import backend


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

    async def mock_queue_push(value, *, high_priority=False, partition_key=None):
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

    assert pool._processes == []
    assert pool._processes == []



async def test_worker_dequeue_task(pool, monkeypatch) -> None:
    """Test Worker._dequeue_task method."""
    from agentexec.worker.pool import Worker, WorkerContext
    from agentexec.worker.event import StateEvent

    @pool.task("test_task")
    async def handler(agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
        return TaskResult()

    context = WorkerContext(
        shutdown_event=StateEvent("shutdown", "test-worker"),
        tasks=pool._context.tasks,
        tx=mp.Queue(),
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

    data = await backend.queue.pop(timeout=1)
    assert data is not None

    task = ax.Task.model_validate(data)
    assert task.task_name == "test_task"
    assert task.context == {"message": "test", "value": 42}
    assert task.agent_id == agent_id


async def test_dequeue_returns_none_on_empty_queue(pool, monkeypatch) -> None:
    """Test pop returns None when queue is empty."""

    async def mock_queue_pop(*args, **kwargs):
        return None

    monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_queue_pop)

    data = await backend.queue.pop(timeout=1)
    assert data is None


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


class TestTaskFailed:
    def test_from_exception(self):
        """TaskFailed.from_exception captures the error string."""
        from agentexec.worker.pool import TaskFailed

        task = ax.Task(
            task_name="test_task",
            context={"message": "hello"},
            agent_id=uuid.uuid4(),
        )
        exc = RuntimeError("something broke")
        msg = TaskFailed.from_exception(task, exc)

        assert msg.task == task
        assert msg.error == "something broke"

    def test_preserves_retry_count(self):
        """TaskFailed preserves the task's current retry_count."""
        from agentexec.worker.pool import TaskFailed

        task = ax.Task(
            task_name="test_task",
            context={"message": "hello"},
            agent_id=uuid.uuid4(),
            retry_count=2,
        )
        msg = TaskFailed.from_exception(task, ValueError("bad"))
        assert msg.task.retry_count == 2


class TestWorkerFailurePath:
    """Test that Worker._run sends TaskFailed on handler exception."""

    async def test_exception_sends_task_failed(self, pool, monkeypatch):
        """Handler exception → TaskFailed sent via IPC queue."""
        from agentexec.worker.pool import Worker, WorkerContext, TaskFailed

        @pool.task("failing_task")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            raise RuntimeError("handler exploded")

        tx = mp.Queue()
        call_count = 0
        shutdown = AsyncMock()

        async def is_set():
            nonlocal call_count
            return call_count > 1

        shutdown.is_set = is_set

        context = WorkerContext(
            shutdown_event=shutdown,
            tasks=pool._context.tasks,
            tx=tx,
        )

        async def mock_pop(*, timeout=1):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "task_name": "failing_task",
                    "context": {"message": "boom"},
                    "agent_id": str(uuid.uuid4()),
                }
            return None

        import agentexec.activity as activity_mod
        monkeypatch.setattr(activity_mod, "update", AsyncMock())
        monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_pop)
        monkeypatch.setattr("agentexec.state.backend.queue.complete", AsyncMock())

        worker = Worker(0, context)

        # Capture _send calls directly to avoid mp.Queue reliability issues
        sent_messages = []
        original_send = worker._send
        def capture_send(message):
            sent_messages.append(message)
            original_send(message)
        monkeypatch.setattr(worker, "_send", capture_send)

        await worker._run()

        failed = [m for m in sent_messages if isinstance(m, TaskFailed)]
        assert len(failed) == 1
        assert failed[0].error == "handler exploded"
        assert failed[0].task.task_name == "failing_task"

    async def test_complete_called_after_failure(self, pool, monkeypatch):
        """queue.complete is called even when the handler throws."""
        from agentexec.worker.pool import Worker, WorkerContext

        @pool.task("locked_fail")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            raise ValueError("oops")

        pool._context.tasks["locked_fail"].lock_key = "msg:{message}"

        tx = mp.Queue()
        call_count = 0
        shutdown = AsyncMock()

        async def is_set():
            nonlocal call_count
            return call_count > 1

        shutdown.is_set = is_set

        context = WorkerContext(
            shutdown_event=shutdown,
            tasks=pool._context.tasks,
            tx=tx,
        )

        async def mock_pop(*, timeout=1):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "task_name": "locked_fail",
                    "context": {"message": "test"},
                    "agent_id": str(uuid.uuid4()),
                }
            return None

        completed_keys = []

        async def mock_complete(partition_key):
            completed_keys.append(partition_key)

        import agentexec.activity as activity_mod
        monkeypatch.setattr(activity_mod, "update", AsyncMock())
        monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_pop)
        monkeypatch.setattr("agentexec.state.backend.queue.complete", mock_complete)

        worker = Worker(0, context)
        await worker._run()

        assert completed_keys == ["msg:test"]


class TestPoolRetryLogic:
    """Test that Pool._process_worker_events handles TaskFailed correctly."""

    async def test_requeues_with_incremented_retry(self, pool, monkeypatch):
        """Failed task with retries remaining is requeued as high priority."""
        from agentexec.worker.pool import TaskFailed

        @pool.task("retry_task")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            pass

        task = ax.Task(
            task_name="retry_task",
            context={"message": "test"},
            agent_id=uuid.uuid4(),
            retry_count=0,
        )

        pushed = []

        async def mock_push(value, *, high_priority=False, partition_key=None):
            pushed.append({"value": value, "high_priority": high_priority, "partition_key": partition_key})

        monkeypatch.setattr("agentexec.state.backend.queue.push", mock_push)
        monkeypatch.setattr(ax.CONF, "max_task_retries", 3)

        # Put a TaskFailed message in the worker queue
        pool._worker_queue.put_nowait(TaskFailed(task=task, error="boom"))

        # Simulate one iteration of _process_worker_events
        # We need a fake process that reports alive once then dead
        class FakeProcess:
            def __init__(self):
                self._calls = 0

            def is_alive(self):
                self._calls += 1
                return self._calls <= 2  # alive for first check, dead on second

        pool._processes = [FakeProcess()]
        pool._log_handler = __import__("logging").StreamHandler()

        await pool._process_worker_events()

        assert len(pushed) == 1
        requeued = json.loads(pushed[0]["value"])
        assert requeued["retry_count"] == 1
        assert pushed[0]["high_priority"] is True

    async def test_gives_up_after_max_retries(self, pool, monkeypatch, capsys):
        """Failed task at max retries is not requeued."""
        from agentexec.worker.pool import TaskFailed

        @pool.task("doomed_task")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            pass

        task = ax.Task(
            task_name="doomed_task",
            context={"message": "test"},
            agent_id=uuid.uuid4(),
            retry_count=3,
        )

        pushed = []

        async def mock_push(value, *, high_priority=False, partition_key=None):
            pushed.append(value)

        monkeypatch.setattr("agentexec.state.backend.queue.push", mock_push)
        monkeypatch.setattr(ax.CONF, "max_task_retries", 3)

        pool._worker_queue.put_nowait(TaskFailed(task=task, error="fatal"))

        class FakeProcess:
            def __init__(self):
                self._calls = 0

            def is_alive(self):
                self._calls += 1
                return self._calls <= 2

        pool._processes = [FakeProcess()]
        pool._log_handler = __import__("logging").StreamHandler()

        await pool._process_worker_events()

        # Should NOT have requeued
        assert len(pushed) == 0

        # Should have printed the give-up message
        captured = capsys.readouterr()
        assert "doomed_task" in captured.out
        assert "4 attempts" in captured.out
        assert "fatal" in captured.out

    async def test_retry_preserves_partition_key(self, pool, monkeypatch):
        """Requeued task uses the correct partition key from its definition."""
        from agentexec.worker.pool import TaskFailed

        @pool.task("partitioned_task")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            pass

        pool._context.tasks["partitioned_task"].lock_key = "msg:{message}"

        task = ax.Task(
            task_name="partitioned_task",
            context={"message": "hello"},
            agent_id=uuid.uuid4(),
            retry_count=0,
        )

        pushed = []

        async def mock_push(value, *, high_priority=False, partition_key=None):
            pushed.append({"partition_key": partition_key})

        monkeypatch.setattr("agentexec.state.backend.queue.push", mock_push)
        monkeypatch.setattr(ax.CONF, "max_task_retries", 3)

        pool._worker_queue.put_nowait(TaskFailed(task=task, error="transient"))

        class FakeProcess:
            def __init__(self):
                self._calls = 0

            def is_alive(self):
                self._calls += 1
                return self._calls <= 2

        pool._processes = [FakeProcess()]
        pool._log_handler = __import__("logging").StreamHandler()

        await pool._process_worker_events()

        assert pushed[0]["partition_key"] == "msg:hello"
