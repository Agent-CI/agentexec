"""Worker pool for background task execution.

Tasks are registered via @WorkerPool.task() decorator before starting the pool.
Workers use the class-level registry to deserialize and execute tasks.
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
from dataclasses import dataclass
from typing import Callable
from uuid import uuid4

from sqlalchemy import Engine, create_engine

from agentexec import state
from agentexec.config import CONF
from agentexec.core.db import remove_global_session, set_global_session
from agentexec.core.queue import Priority, dequeue, enqueue
from agentexec.core.task import Task, TaskDefinition, TaskHandler
from agentexec.worker.event import StateEvent
from agentexec.worker.logging import (
    DEFAULT_FORMAT,
    LogMessage,
    get_worker_logger,
)

__all__ = [
    "TaskWrapper",
    "Worker",
    "WorkerPool",
    "WorkerContext",
]


from pydantic import BaseModel


class TaskWrapper:
    """Wrapper for task handlers that provides .enqueue() and .run() methods.

    This is returned by the @pool.task() decorator instead of the raw function.
    The wrapper is callable, so existing code that calls the function directly
    still works.

    Example:
        @pool.task("research")
        async def research(agent_id: UUID, context: ResearchContext) -> ResearchResult:
            ...

        # Enqueue to worker pool (non-blocking)
        task = await research.enqueue(ResearchContext(company="Acme"))

        # Run inline (blocking)
        result = await research.run(ResearchContext(company="Acme"))
    """

    def __init__(self, name: str, handler: TaskHandler, definition: TaskDefinition):
        self._name = name
        self._handler = handler
        self._definition = definition
        # Copy function attributes for introspection
        self.__name__ = handler.__name__
        self.__doc__ = handler.__doc__
        self.__wrapped__ = handler

    def __call__(self, **kwargs):
        """Direct call delegates to the original handler."""
        return self._handler(**kwargs)

    async def enqueue(
        self,
        context: BaseModel,
        priority: Priority = Priority.LOW,
    ) -> Task:
        """Enqueue the task to run on a worker.

        Args:
            context: Task context (must match handler's context type)
            priority: Task priority (HIGH or LOW)

        Returns:
            Task instance with agent_id for tracking
        """
        return await enqueue(self._name, context, priority=priority)

    async def run(self, context: BaseModel) -> BaseModel:
        """Execute the task inline (not on a worker).

        Creates an activity record and runs the handler directly.
        Useful for testing or when already running on a worker.

        Args:
            context: Task context (must match handler's context type)

        Returns:
            Handler result
        """
        task = Task.create(self._name, context)
        task._definition = self._definition
        result = await task.execute()
        if result is None:
            raise RuntimeError(f"Task {self._name} failed")
        return result


def _get_pool_id() -> str:
    """Get a unique pool ID for shutdown event keys."""
    return uuid4().hex[:8]


@dataclass
class WorkerContext:
    """Shared context passed from WorkerPool to Worker processes."""

    database_url: str
    shutdown_event: StateEvent
    tasks: dict[str, TaskDefinition]
    queue_name: str


class Worker:
    """Individual worker process with isolated state.

    Each worker configures the scoped Session factory on startup.
    Task handlers can use get_global_session() to get the process-local session.
    """

    _worker_id: int
    _context: WorkerContext
    _logger: logging.Logger

    def __init__(self, worker_id: int, context: WorkerContext):
        """Initialize worker with isolated state.

        Args:
            worker_id: Unique identifier for this worker
            context: Shared context from WorkerPool
        """
        self._worker_id = worker_id
        self._context = context
        self._logger = get_worker_logger(__name__)

    @classmethod
    def run_in_process(cls, worker_id: int, context: WorkerContext) -> None:
        """Entry point for running a worker in a new process.

        Args:
            worker_id: Unique identifier for this worker
            context: Shared context from WorkerPool
        """
        instance = cls(worker_id, context)
        instance.run()

    def run(self) -> None:
        """Main worker entry point - sets up async loop and runs."""
        self._logger.info(f"Worker {self._worker_id} starting")

        engine = create_engine(self._context.database_url)
        set_global_session(engine)

        try:
            asyncio.run(self._run())
        except Exception as e:
            self._logger.exception(f"Worker {self._worker_id} fatal error: {e}")
            raise

    async def _run(self) -> None:
        """Async main loop - polls queue and processes tasks."""
        try:
            # No sleep needed - dequeue() uses brpop which blocks waiting for tasks
            while not await self._context.shutdown_event.is_set():
                if (task := await self._dequeue_task()) is not None:
                    self._logger.info(f"Worker {self._worker_id} processing: {task.task_name}")
                    await task.execute()
                    self._logger.info(f"Worker {self._worker_id} completed: {task.task_name}")
        except Exception as e:
            self._logger.exception(f"Worker {self._worker_id} error: {e}")
            # Continue processing other tasks
            # TODO allow configurable behavior here (retry, backoff, fail)
            # TODO all of the actual logic is handled in task.execute(), so I don't know why we ever end up here.
        finally:
            await state.backend.close()
            remove_global_session()
            self._logger.info(f"Worker {self._worker_id} shutting down")

    async def _dequeue_task(self) -> Task | None:
        """Dequeue and hydrate a task from the Redis queue.

        Reconstructs the typed context using the TaskDefinition
        and binds the definition to the task.

        Returns:
            Hydrated Task instance if available, else None.
        """
        if (data := await dequeue(queue_name=self._context.queue_name)) is not None:
            return Task.from_serialized(
                definition=self._context.tasks[data["task_name"]],
                data=data,
            )

        return None


class WorkerPool:
    """Manages a pool of worker processes for background task execution.

    Tasks are registered via @pool.task() decorator. Workers process tasks
    from the Redis queue using the pool's task registry.

    Example:
        import agentexec as ax
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///agents.db")
        pool = ax.WorkerPool(engine=engine)

        @pool.task("research_company")
        async def research(agent_id: UUID, context: ResearchContext):
            ...

        pool.start()
    """

    _context: WorkerContext
    _processes: list[mp.Process]
    _log_handler: logging.Handler | None

    def __init__(
        self,
        engine: Engine | None = None,
        database_url: str | None = None,
        queue_name: str | None = None,
    ) -> None:
        """Initialize the worker pool.

        Args:
            engine: SQLAlchemy engine (URL will be extracted for workers).
            database_url: Database URL string. Alternative to passing engine.
            queue_name: Redis queue name. Defaults to CONF.queue_name.

        Raises:
            ValueError: If neither engine nor database_url is provided.
        """

        if not engine and not database_url:
            raise ValueError("Either engine or database_url must be provided")

        engine = engine or create_engine(database_url)
        set_global_session(engine)

        self._context = WorkerContext(
            database_url=database_url or str(engine.url),
            shutdown_event=StateEvent("shutdown", _get_pool_id()),
            tasks={},
            queue_name=queue_name or CONF.queue_name,
        )
        self._processes = []
        self._log_handler = None

    def task(self, name: str) -> Callable[[TaskHandler], TaskWrapper]:
        """Decorator to register a task handler with this pool.

        Creates a TaskDefinition that captures the handler and its context class
        from type annotations. Returns a TaskWrapper with .enqueue() and .run()
        methods.

        Args:
            name: Task name used when enqueueing and for worker routing.

        Returns:
            Decorator function that returns a TaskWrapper.

        Example:
            @pool.task("research_company")
            async def research(agent_id: UUID, context: ResearchContext) -> ResearchResult:
                ...

            # Enqueue to worker
            task = await research.enqueue(ResearchContext(company="Acme"))

            # Run inline
            result = await research.run(ResearchContext(company="Acme"))
        """

        def decorator(func: TaskHandler) -> TaskWrapper:
            wrapper = self._add_task(name, func)
            # TODO wrapper is breaking pikcling for the forked process.
            return func

        return decorator

    def _add_task(
        self,
        name: str,
        func: TaskHandler,
        *,
        context_class: type[BaseModel] | None = None,
        result_class: type[BaseModel] | None = None,
    ) -> TaskWrapper:
        """Add a task to the pool (internal use)."""
        definition = TaskDefinition(
            name=name,
            handler=func,
            context_class=context_class,
            result_class=result_class,
        )
        self._context.tasks[name] = definition
        return func

        # return TaskWrapper(name, func, definition)

    def start(self) -> None:
        """Start worker processes (non-blocking).

        Spawns N worker processes that poll the Redis queue and execute
        tasks from this pool's registry. Returns immediately.

        Workers log to Redis pubsub. Use run() if you want the main
        process to collect and display those logs.
        """
        # Clear any stale shutdown signal
        self._context.shutdown_event.clear()

        # Spawn workers BEFORE setting up log handler to avoid pickling issues
        # (StreamHandler has a lock that can't be pickled)
        self._spawn_workers()

        # Set up log handler for receiving worker logs
        # TODO make this configurable
        self._log_handler = logging.StreamHandler()
        self._log_handler.setFormatter(logging.Formatter(DEFAULT_FORMAT))

    def run(self) -> None:
        """Start workers and run log collector until interrupted.

        Spawns worker processes and runs an async event loop in the main
        process that collects logs from workers via Redis pubsub.
        Blocks until all workers exit or KeyboardInterrupt, then shuts
        down gracefully.
        """

        async def _loop():
            try:
                await self._collect_logs()
            except asyncio.CancelledError:
                pass
            finally:
                self.shutdown()
                await state.backend.close()

        try:
            self.start()
            asyncio.run(_loop())
        except KeyboardInterrupt:
            pass

    def _spawn_workers(self) -> None:
        """Spawn worker processes."""
        print(f"Starting {CONF.num_workers} worker processes")

        for worker_id in range(CONF.num_workers):
            process = mp.Process(
                target=Worker.run_in_process,
                args=(worker_id, self._context),
                daemon=False,
            )
            process.start()
            self._processes.append(process)
            print(f"Started worker {worker_id} (PID: {process.pid})")

    async def _collect_logs(self) -> None:
        """Listen for log messages from workers via state backend pubsub."""
        assert self._log_handler, "Log handler not initialized"

        # Create task to subscribe to logs
        log_task = asyncio.create_task(self._process_log_stream())

        try:
            # Poll worker processes
            while any(p.is_alive() for p in self._processes):
                await asyncio.sleep(0.1)
        finally:
            log_task.cancel()
            try:
                await log_task
            except asyncio.CancelledError:
                pass

    async def _process_log_stream(self) -> None:
        """Process log messages from the state backend."""
        assert self._log_handler, "Log handler not initialized"

        async for message in state.subscribe_logs():
            log_message = LogMessage.model_validate_json(message)
            self._log_handler.emit(log_message.to_log_record())

    def shutdown(self, timeout: int | None = None) -> None:
        """Gracefully shutdown all worker processes.

        For use with start(). If using run(), shutdown is handled automatically.

        Args:
            timeout: Max seconds to wait per worker. Defaults to CONF.graceful_shutdown_timeout.
        """
        if timeout is None:
            timeout = CONF.graceful_shutdown_timeout

        print("Shutting down worker pool")
        self._context.shutdown_event.set()

        for process in self._processes:
            process.join(timeout=timeout)
            if process.is_alive():
                print(f"Worker {process.pid} did not stop, terminating")
                process.terminate()
                process.join(timeout=5)

        self._processes.clear()
        print("Worker pool shutdown complete")
