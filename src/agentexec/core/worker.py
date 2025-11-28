"""Worker pool for background task execution.

Tasks are registered via @WorkerPool.task() decorator before starting the pool.
Workers use the class-level registry to deserialize and execute tasks.
"""

from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing as mp
from dataclasses import dataclass
from multiprocessing.synchronize import Event as EventClass
from typing import Callable

from sqlalchemy import Engine

from agentexec.config import CONF
from agentexec.core.db import remove_global_session, set_global_session
from agentexec.core.queue import dequeue
from agentexec.core.redis_client import close_redis
from agentexec.core.task import Task, TaskDefinition, TaskHandler

logger = logging.getLogger(__name__)


__all__ = [
    "Worker",
    "WorkerPool",
    "WorkerContext",
]


@dataclass
class WorkerContext:
    """Shared context passed from WorkerPool to Worker processes."""

    engine: Engine
    shutdown_event: EventClass
    tasks: dict[str, TaskDefinition]
    queue_name: str


class Worker:
    """Individual worker process with isolated state.

    Each worker configures the scoped Session factory on startup.
    Task handlers can use get_global_session() to get the process-local session.
    """

    _worker_id: int
    _context: WorkerContext

    def __init__(self, worker_id: int, context: WorkerContext):
        """Initialize worker with isolated state.

        Args:
            worker_id: Unique identifier for this worker
            context: Shared context from WorkerPool
        """
        self._worker_id = worker_id
        self._context = context

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
        set_global_session(self._context.engine)
        logger.info(f"Worker {self._worker_id} starting")

        try:
            asyncio.run(self._run())
        except Exception as e:
            logger.exception(f"Worker {self._worker_id} fatal error: {e}")
            raise

    async def _run(self) -> None:
        """Async main loop - polls queue and processes tasks."""
        try:
            # No sleep needed - dequeue() uses brpop which blocks waiting for tasks
            while not self._context.shutdown_event.is_set():
                if (task := await self._dequeue_task()) is not None:
                    logger.info(f"Worker {self._worker_id} processing: {task.task_name}")
                    await task.execute()
        finally:
            await close_redis()
            remove_global_session()
            logger.info(f"Worker {self._worker_id} shutting down")

    async def _dequeue_task(self) -> Task | None:
        """Dequeue and hydrate a task from the Redis queue.

        Parses the JSON, reconstructs the typed context using the TaskDefinition,
        and binds the definition to the task.

        Returns:
            Hydrated Task instance if available, else None.
        """
        task_json = await dequeue(queue_name=self._context.queue_name)
        if task_json is None:
            return None

        data = json.loads(task_json)
        task_def = self._context.tasks[data["task_name"]]
        return Task.from_serialized(task_def, data)


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

    def __init__(self, engine: Engine, queue_name: str | None = None) -> None:
        """Initialize the worker pool.

        Args:
            engine: SQLAlchemy engine for database sessions.
            queue_name: Redis queue name. Defaults to CONF.queue_name.
        """
        self._context = WorkerContext(
            engine=engine,
            shutdown_event=mp.Event(),
            tasks={},
            queue_name=queue_name or CONF.queue_name,
        )
        self._processes = []

    def task(self, name: str) -> Callable[[TaskHandler], TaskHandler]:
        """Decorator to register a task handler with this pool.

        Creates a TaskDefinition that captures the handler and its context class
        from type annotations.

        Args:
            name: Task name used when enqueueing and for worker routing.

        Returns:
            Decorator function that registers the handler.

        Example:
            @pool.task("research_company")
            async def research(agent_id: UUID, context: ResearchContext):
                company = context.company_name  # Typed!
                ...
        """

        def decorator(func: TaskHandler) -> TaskHandler:
            self._context.tasks[name] = TaskDefinition(
                name=name,
                handler=func,
            )
            return func

        return decorator

    def start(self) -> None:
        """Start the worker processes.

        Spawns N worker processes that poll the Redis queue and execute
        tasks from this pool's registry.
        """
        logger.info(f"Starting {CONF.num_workers} worker processes")

        for worker_id in range(CONF.num_workers):
            process = mp.Process(
                target=Worker.run_in_process,
                args=(worker_id, self._context),
                daemon=False,
            )
            process.start()
            self._processes.append(process)
            logger.info(f"Started worker {worker_id} (PID: {process.pid})")

    def shutdown(self, timeout: int | None = None) -> None:
        """Gracefully shutdown all worker processes.

        Args:
            timeout: Max seconds to wait per worker. Defaults to CONF.graceful_shutdown_timeout.
        """
        if timeout is None:
            timeout = CONF.graceful_shutdown_timeout

        logger.info("Shutting down worker pool")
        self._context.shutdown_event.set()

        for process in self._processes:
            process.join(timeout=timeout)
            if process.is_alive():
                logger.warning(f"Worker {process.pid} did not stop, terminating")
                process.terminate()
                process.join(timeout=5)

        self._processes.clear()
        logger.info("Worker pool shutdown complete")
