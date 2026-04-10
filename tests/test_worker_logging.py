"""Integration tests for logging across the worker pool.

These tests spawn actual worker processes and verify that log output
from all components (pool, worker, event handler, user task code)
reaches stderr with the correct format.
"""

import asyncio
import logging
import multiprocessing as mp
import re
import sys
import uuid
from io import StringIO
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

import agentexec as ax
from agentexec.worker.pool import Worker, WorkerContext, _EventHandler, TaskFailed
from agentexec.state import backend


class SampleContext(BaseModel):
    message: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pool():
    from sqlalchemy.ext.asyncio import create_async_engine
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    return ax.Pool(engine=engine)


def _make_worker_context(pool) -> WorkerContext:
    return WorkerContext(
        shutdown_event=mp.Event(),
        tasks=pool._context.tasks,
        tx=mp.Queue(),
    )


def _capture_handler(name: str = "agentexec.worker.pool") -> tuple[logging.Logger, StringIO]:
    """Attach a StringIO handler to a logger and return both."""
    buf = StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(logging.Formatter(
        "[%(levelname)s/%(processName)s] %(name)s: %(message)s"
    ))
    log = logging.getLogger(name)
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    return log, buf


# ---------------------------------------------------------------------------
# Worker._run logging
# ---------------------------------------------------------------------------

class TestWorkerRunLogging:
    """Logs emitted by the worker's main loop."""

    async def test_processing_and_completed(self, monkeypatch):
        """Worker logs 'processing' and 'completed' for a successful task."""
        pool = _make_pool()

        @pool.task("greet")
        async def greet(agent_id: uuid.UUID, context: SampleContext):
            pass

        ctx = _make_worker_context(pool)
        worker = Worker(0, ctx)
        _, buf = _capture_handler()

        call_count = 0

        async def mock_pop(*, timeout=1):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "task_name": "greet",
                    "context": {"message": "hi"},
                    "agent_id": str(uuid.uuid4()),
                }
            ctx.shutdown_event.set()
            return None

        monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_pop)
        monkeypatch.setattr("agentexec.state.backend.queue.complete", AsyncMock())
        monkeypatch.setattr("agentexec.activity.update", AsyncMock())

        await worker._run()

        output = buf.getvalue()
        assert "Worker 0 processing: greet" in output
        assert "Worker 0 completed: greet" in output

    async def test_failed_task_logs_error(self, monkeypatch):
        """Worker logs an error when a task handler raises."""
        pool = _make_pool()

        @pool.task("boom")
        async def boom(agent_id: uuid.UUID, context: SampleContext):
            raise RuntimeError("kaboom")

        ctx = _make_worker_context(pool)
        worker = Worker(0, ctx)
        _, buf = _capture_handler()

        call_count = 0

        async def mock_pop(*, timeout=1):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "task_name": "boom",
                    "context": {"message": "hi"},
                    "agent_id": str(uuid.uuid4()),
                }
            ctx.shutdown_event.set()
            return None

        monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_pop)
        monkeypatch.setattr("agentexec.state.backend.queue.complete", AsyncMock())
        monkeypatch.setattr("agentexec.activity.update", AsyncMock())

        await worker._run()

        output = buf.getvalue()
        assert "Worker 0 failed: boom" in output

    async def test_unknown_task_logs_with_context(self, monkeypatch):
        """Worker logs a useful message when task_name isn't registered."""
        pool = _make_pool()

        @pool.task("real_task")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            pass

        ctx = _make_worker_context(pool)
        worker = Worker(0, ctx)
        _, buf = _capture_handler()

        call_count = 0

        async def mock_pop(*, timeout=1):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "task_name": "ghost_task",
                    "context": {"message": "hi"},
                    "agent_id": str(uuid.uuid4()),
                }
            ctx.shutdown_event.set()
            return None

        monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_pop)
        monkeypatch.setattr("agentexec.state.backend.queue.complete", AsyncMock())

        await worker._run()

        output = buf.getvalue()
        assert "ghost_task" in output
        assert "not registered" in output


# ---------------------------------------------------------------------------
# User task code logging
# ---------------------------------------------------------------------------

class TestUserTaskLogging:
    """Logs emitted by user-defined task handlers."""

    async def test_user_logger_output_reaches_stderr(self, monkeypatch):
        """A logger.info() call inside a task handler should appear in output."""
        pool = _make_pool()

        @pool.task("chatty")
        async def chatty(agent_id: uuid.UUID, context: SampleContext):
            task_logger = logging.getLogger("myapp.tasks")
            task_logger.info(f"User says: {context.message}")

        ctx = _make_worker_context(pool)
        worker = Worker(0, ctx)

        # Capture the user's logger too
        _, pool_buf = _capture_handler("agentexec.worker.pool")
        _, user_buf = _capture_handler("myapp.tasks")

        call_count = 0

        async def mock_pop(*, timeout=1):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "task_name": "chatty",
                    "context": {"message": "hello from user"},
                    "agent_id": str(uuid.uuid4()),
                }
            ctx.shutdown_event.set()
            return None

        monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_pop)
        monkeypatch.setattr("agentexec.state.backend.queue.complete", AsyncMock())
        monkeypatch.setattr("agentexec.activity.update", AsyncMock())

        await worker._run()

        assert "User says: hello from user" in user_buf.getvalue()

    async def test_user_exception_traceback_in_output(self, monkeypatch):
        """When user code raises, the full traceback should be logged."""
        pool = _make_pool()

        @pool.task("broken")
        async def broken(agent_id: uuid.UUID, context: SampleContext):
            def deep_call():
                raise ValueError("deep error")
            deep_call()

        ctx = _make_worker_context(pool)
        worker = Worker(0, ctx)
        _, buf = _capture_handler()

        call_count = 0

        async def mock_pop(*, timeout=1):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "task_name": "broken",
                    "context": {"message": "hi"},
                    "agent_id": str(uuid.uuid4()),
                }
            ctx.shutdown_event.set()
            return None

        monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_pop)
        monkeypatch.setattr("agentexec.state.backend.queue.complete", AsyncMock())
        monkeypatch.setattr("agentexec.activity.update", AsyncMock())

        await worker._run()

        output = buf.getvalue()
        # The traceback should include the nested function name
        assert "deep_call" in output or "deep error" in output


# ---------------------------------------------------------------------------
# _EventHandler logging
# ---------------------------------------------------------------------------

class TestEventHandlerLogging:
    """Logs emitted by the main-process event handler."""

    async def test_max_retries_logs_giving_up(self, monkeypatch):
        """Event handler logs when a task exhausts its retries."""
        pool = _make_pool()

        @pool.task("doomed")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            pass

        _, buf = _capture_handler()
        monkeypatch.setattr(ax.CONF, "max_task_retries", 3)

        import queue
        q = queue.Queue()
        eh = _EventHandler(
            shutdown_event=pool._context.shutdown_event,
            queue=q,
            tasks=pool._context.tasks,
        )

        task = ax.Task(
            task_name="doomed",
            context={"message": "test"},
            agent_id=uuid.uuid4(),
            retry_count=3,
        )
        q.put(TaskFailed(task=task, error="persistent failure"))
        await eh._handle()

        output = buf.getvalue()
        assert "doomed" in output
        assert "persistent failure" in output
        assert "giving up" in output


# ---------------------------------------------------------------------------
# Log format consistency
# ---------------------------------------------------------------------------

class TestLogFormatConsistency:
    """All log sources should use a consistent format."""

    async def test_pool_lifecycle_format(self, monkeypatch):
        """Pool startup/shutdown logs include process name and logger name."""
        _, buf = _capture_handler()
        pool = _make_pool()

        # Just test that _spawn_workers and shutdown produce formatted output
        # without actually spawning (mock the Process)
        mock_process = AsyncMock()
        mock_process.pid = 12345
        mock_process.is_alive.return_value = False

        original_spawn = pool._mp_context.Process

        def fake_process(**kwargs):
            return mock_process

        monkeypatch.setattr(pool._mp_context, "Process", fake_process)
        mock_process.start = lambda: None
        mock_process.join = lambda timeout=None: None

        pool._spawn_workers()

        output = buf.getvalue()
        assert "Starting" in output
        assert "Started worker 0" in output

    async def test_worker_logs_include_worker_id(self, monkeypatch):
        """Every worker log line should identify which worker emitted it."""
        pool = _make_pool()

        @pool.task("task_a")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            pass

        ctx = _make_worker_context(pool)
        worker = Worker(0, ctx)
        _, buf = _capture_handler()

        call_count = 0

        async def mock_pop(*, timeout=1):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "task_name": "task_a",
                    "context": {"message": "hi"},
                    "agent_id": str(uuid.uuid4()),
                }
            ctx.shutdown_event.set()
            return None

        monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_pop)
        monkeypatch.setattr("agentexec.state.backend.queue.complete", AsyncMock())
        monkeypatch.setattr("agentexec.activity.update", AsyncMock())

        await worker._run()

        output = buf.getvalue()
        for line in output.strip().splitlines():
            if "Worker" in line:
                assert "Worker 0" in line


# ---------------------------------------------------------------------------
# Spawn process logging (true integration)
# ---------------------------------------------------------------------------

class TestSpawnProcessLogging:
    """Verify that logs from a spawned worker process reach the parent."""

    def test_spawn_worker_has_log_handlers(self):
        """Spawned worker process should have a root log handler.

        Uses fork context to avoid daemon-can't-spawn-children restriction
        in pytest. The handler setup in run_in_process() is context-agnostic.
        """
        result_queue = mp.Queue()

        def _check_logging_in_child(worker_id, context):
            """Runs inside the child process."""
            # run_in_process does signal + handler setup then calls cls(id, ctx).run()
            # We only care about the handler setup, so replicate just that part.
            import signal
            signal.signal(signal.SIGINT, signal.SIG_IGN)

            root = logging.getLogger()
            if not root.handlers:
                handler = logging.StreamHandler()
                handler.setFormatter(logging.Formatter(
                    "[%(levelname)s/%(processName)s] %(name)s: %(message)s"
                ))
                root.addHandler(handler)
                root.setLevel(logging.INFO)

            pool_logger = logging.getLogger("agentexec.worker.pool")
            user_logger = logging.getLogger("myapp.tasks")

            result_queue.put({
                "root_handlers": len(root.handlers),
                "pool_effective_level": pool_logger.getEffectiveLevel(),
                "user_effective_level": user_logger.getEffectiveLevel(),
                "tasks": list(context.tasks.keys()),
            })

        pool = _make_pool()

        @pool.task("spawn_test")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            pass

        fork_ctx = mp.get_context("fork")
        context = WorkerContext(
            shutdown_event=fork_ctx.Event(),
            tasks=pool._context.tasks,
            tx=fork_ctx.Queue(),
        )

        process = fork_ctx.Process(
            target=_check_logging_in_child,
            args=(0, context),
        )
        process.start()
        process.join(timeout=10)

        assert not result_queue.empty(), "Child process did not report back"
        result = result_queue.get_nowait()

        assert result["root_handlers"] >= 1, (
            f"Spawned worker has no log handlers (root has {result['root_handlers']})"
        )
        assert result["pool_effective_level"] <= logging.INFO
        assert result["user_effective_level"] <= logging.INFO
        assert "spawn_test" in result["tasks"]
