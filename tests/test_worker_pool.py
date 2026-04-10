import json
import multiprocessing as mp
import uuid
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

import agentexec as ax
from agentexec.core.queue import Priority
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

    async def mock_queue_push(value, *, priority=None, partition_key=None):
        from agentexec.core.queue import Priority
        if priority is Priority.HIGH:
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
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
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
    pool = ax.Pool(database_url="sqlite+aiosqlite:///:memory:")

    assert pool._processes == []
    assert pool._processes == []



async def test_worker_dequeue_task(pool, monkeypatch) -> None:
    """Test Worker._dequeue_task method."""
    from agentexec.worker.pool import Worker, WorkerContext

    @pool.task("test_task")
    async def handler(agent_id: uuid.UUID, context: SampleContext) -> TaskResult:
        return TaskResult()

    context = WorkerContext(
        shutdown_event=mp.Event(),
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
    # Should not raise even with empty process list
    await pool.shutdown(timeout=1)

    assert pool._processes == []
    assert pool._context.shutdown_event.is_set()




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
        shutdown = mp.Event()

        context = WorkerContext(
            shutdown_event=shutdown,
            tasks=pool._context.tasks,
            tx=tx,
        )

        call_count = 0

        async def mock_pop(*, timeout=1):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "task_name": "failing_task",
                    "context": {"message": "boom"},
                    "agent_id": str(uuid.uuid4()),
                }
            shutdown.set()
            return None

        import agentexec.activity as activity_mod
        monkeypatch.setattr(activity_mod, "update", AsyncMock())
        monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_pop)
        monkeypatch.setattr("agentexec.state.backend.queue.complete", AsyncMock())

        worker = Worker(0, context)

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
        shutdown = mp.Event()

        context = WorkerContext(
            shutdown_event=shutdown,
            tasks=pool._context.tasks,
            tx=tx,
        )

        call_count = 0

        async def mock_pop(*, timeout=1):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "task_name": "locked_fail",
                    "context": {"message": "test"},
                    "agent_id": str(uuid.uuid4()),
                }
            shutdown.set()
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
    """Test that _EventHandler handles TaskFailed correctly."""

    def _make_handler(self, pool):
        import queue
        from agentexec.worker.pool import _EventHandler
        self._test_queue = queue.Queue()
        return _EventHandler(
            shutdown_event=pool._context.shutdown_event,
            queue=self._test_queue,
            tasks=pool._context.tasks,
        )

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

        async def mock_push(value, *, priority=None, partition_key=None):
            pushed.append({"value": value, "priority": priority, "partition_key": partition_key})

        monkeypatch.setattr("agentexec.state.backend.queue.push", mock_push)
        monkeypatch.setattr(ax.CONF, "max_task_retries", 3)

        eh = self._make_handler(pool)
        self._test_queue.put(TaskFailed(task=task, error="boom"))
        await eh._handle()

        assert len(pushed) == 1
        requeued = json.loads(pushed[0]["value"])
        assert requeued["retry_count"] == 1
        assert pushed[0]["priority"] is Priority.HIGH

    async def test_gives_up_after_max_retries(self, pool, monkeypatch, caplog):
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

        async def mock_push(value, *, priority=None, partition_key=None):
            pushed.append(value)

        monkeypatch.setattr("agentexec.state.backend.queue.push", mock_push)
        monkeypatch.setattr(ax.CONF, "max_task_retries", 3)

        eh = self._make_handler(pool)
        self._test_queue.put(TaskFailed(task=task, error="fatal"))
        with caplog.at_level("INFO", logger="agentexec.worker.pool"):
            await eh._handle()

        assert len(pushed) == 0
        assert "doomed_task" in caplog.text
        assert "4 attempts" in caplog.text
        assert "fatal" in caplog.text

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

        async def mock_push(value, *, priority=None, partition_key=None):
            pushed.append({"partition_key": partition_key})

        monkeypatch.setattr("agentexec.state.backend.queue.push", mock_push)
        monkeypatch.setattr(ax.CONF, "max_task_retries", 3)

        eh = self._make_handler(pool)
        self._test_queue.put(TaskFailed(task=task, error="transient"))
        await eh._handle()

        assert pushed[0]["partition_key"] == "msg:hello"
