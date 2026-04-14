"""Tests for worker process resilience.

These tests verify that the worker survives various pathological failure
modes in user task code without taking down the worker process.
"""

import asyncio
import multiprocessing as mp
import sys
import uuid
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

import agentexec as ax
from agentexec.worker.pool import Worker, WorkerContext


class SampleContext(BaseModel):
    message: str


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


def _drive_one_task(monkeypatch, ctx, task_name: str):
    """Set up mocks so the worker pops one task then shuts down."""
    call_count = 0

    async def mock_pop(*, timeout=1):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {
                "task_name": task_name,
                "context": {"message": "test"},
                "agent_id": str(uuid.uuid4()),
            }
        ctx.shutdown_event.set()
        return None

    monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_pop)
    monkeypatch.setattr("agentexec.state.backend.queue.complete", AsyncMock())
    monkeypatch.setattr("agentexec.activity.update", AsyncMock())


class TestWorkerSurvivesUserCodeFailures:
    """User code should never take down the worker process."""

    async def test_import_error_in_handler(self, monkeypatch):
        """ImportError raised inside a handler should be caught."""
        pool = _make_pool()

        @pool.task("bad_import")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            import this_module_does_not_exist  # type: ignore[unresolved-import]  # noqa: F401

        ctx = _make_worker_context(pool)
        worker = Worker(0, ctx)
        _drive_one_task(monkeypatch, ctx, "bad_import")

        # Should not raise — the loop should continue and exit cleanly
        await worker._run()

    async def test_system_exit_in_handler(self, monkeypatch):
        """sys.exit() inside a handler should not kill the worker."""
        pool = _make_pool()

        @pool.task("exiter")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            sys.exit(1)

        ctx = _make_worker_context(pool)
        worker = Worker(0, ctx)
        _drive_one_task(monkeypatch, ctx, "exiter")

        # SystemExit is BaseException, not Exception — does the worker survive?
        await worker._run()

    async def test_keyboard_interrupt_in_handler(self, monkeypatch):
        """KeyboardInterrupt inside a handler should not crash the worker."""
        pool = _make_pool()

        @pool.task("interrupter")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            raise KeyboardInterrupt("user pressed ctrl-c inside handler")

        ctx = _make_worker_context(pool)
        worker = Worker(0, ctx)
        _drive_one_task(monkeypatch, ctx, "interrupter")

        await worker._run()

    async def test_base_exception_in_handler(self, monkeypatch):
        """A raw BaseException should not crash the worker."""
        pool = _make_pool()

        @pool.task("bex")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            raise BaseException("rude")

        ctx = _make_worker_context(pool)
        worker = Worker(0, ctx)
        _drive_one_task(monkeypatch, ctx, "bex")

        await worker._run()

    async def test_recursion_error_in_handler(self, monkeypatch):
        """RecursionError should be caught (it's an Exception subclass)."""
        pool = _make_pool()

        @pool.task("recurse")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            def boom():
                return boom()
            boom()

        ctx = _make_worker_context(pool)
        worker = Worker(0, ctx)
        _drive_one_task(monkeypatch, ctx, "recurse")

        await worker._run()


class TestWorkerSurvivesQueueFailures:
    """Backend failures (Redis hiccup, malformed payload) should not kill the worker."""

    async def test_malformed_task_payload(self, monkeypatch):
        """A payload that fails Task.model_validate should not crash the worker."""
        pool = _make_pool()

        @pool.task("real")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            pass

        ctx = _make_worker_context(pool)
        worker = Worker(0, ctx)

        call_count = 0

        async def mock_pop(*, timeout=1):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"not_a_task": "garbage"}  # missing required fields
            ctx.shutdown_event.set()
            return None

        monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_pop)
        monkeypatch.setattr("agentexec.state.backend.queue.complete", AsyncMock())

        await worker._run()

    async def test_complete_raises(self, monkeypatch):
        """If queue.complete raises in the finally, the worker should keep going."""
        pool = _make_pool()

        @pool.task("ok")
        async def handler(agent_id: uuid.UUID, context: SampleContext):
            pass

        ctx = _make_worker_context(pool)
        worker = Worker(0, ctx)

        call_count = 0

        async def mock_pop(*, timeout=1):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "task_name": "ok",
                    "context": {"message": "test"},
                    "agent_id": str(uuid.uuid4()),
                }
            ctx.shutdown_event.set()
            return None

        async def broken_complete(*args, **kwargs):
            raise RuntimeError("redis went away")

        monkeypatch.setattr("agentexec.state.backend.queue.pop", mock_pop)
        monkeypatch.setattr("agentexec.state.backend.queue.complete", broken_complete)
        monkeypatch.setattr("agentexec.activity.update", AsyncMock())

        await worker._run()
