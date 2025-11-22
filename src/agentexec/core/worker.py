from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
from collections.abc import Callable
from multiprocessing.synchronize import Event as EventClass
from typing import Any, ClassVar, Generic, TypeVar, cast

from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from agentexec.config import CONF
from agentexec.core.task import Task, TaskHandler

logger = logging.getLogger(__name__)


__all__ = [
    "Worker",
    "WorkerPool",
    "get_worker_session",
]

T = TypeVar("T")


class classproperty(Generic[T]):
    """Decorator for class-level properties.

    Generic decorator that preserves the return type of the decorated method.
    """

    def __init__(self, func: Callable[[Any], T]) -> None:
        self.func = func

    def __get__(self, obj: Any, owner: type) -> T:
        return self.func(owner)


def _build_session(engine: Engine) -> Session:
    """Helper to build a new SQLAlchemy session from engine."""
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    return cast(Session, SessionLocal())


def get_worker_session() -> Session:
    """Get the current worker's database session.

    Returns:
        Session from the current worker

    Raises:
        RuntimeError: If called outside of a worker process
    """
    return Worker.session


class Worker:
    """Individual worker process with isolated state.

    Each worker maintains its own database session, event loop, and processes
    tasks from the queue independently. Workers run in separate processes for
    true parallelism.
    """

    # Class-level reference to current worker in this process
    current: ClassVar[Worker | None] = None

    # Instance attributes
    _worker_id: int
    _handlers: dict[str, TaskHandler]
    _shutdown_event: EventClass
    _session: Session
    _loop: asyncio.AbstractEventLoop

    def __init__(
        self,
        worker_id: int,
        engine: Engine,
        handlers: dict[str, TaskHandler],
        shutdown_event: EventClass,
    ):
        """Initialize worker with isolated state.

        Args:
            worker_id: Unique identifier for this worker
            engine: SQLAlchemy engine to create session from
            handlers: Task handler registry (shared read-only)
            shutdown_event: Multiprocessing event for coordinated shutdown
        """
        self._worker_id = worker_id
        self._handlers = handlers
        self._shutdown_event = shutdown_event

        # Create worker-owned resources
        self._session = _build_session(engine)
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

    @classmethod
    def run_in_process(
        cls,
        worker_id: int,
        engine: Engine,
        handlers: dict[str, TaskHandler],
        shutdown_event: EventClass,
    ) -> None:
        """Entry point for running a worker in a new process.

        Creates a Worker instance and runs it. This is called by multiprocessing
        to start a worker in a new process.

        Args:
            worker_id: Unique identifier for this worker
            engine: SQLAlchemy engine to create session from
            handlers: Task handler registry
            shutdown_event: Multiprocessing event for coordinated shutdown
        """
        instance = cls(worker_id, engine, handlers, shutdown_event)
        Worker.current = instance
        instance.run()

    @classproperty
    def session(cls) -> Session:
        """Get the current worker's database session.

        Returns:
            Session from the current worker

        Raises:
            RuntimeError: If called outside of a worker process
        """
        if cls.current is None:
            raise RuntimeError("Worker.session called outside of worker context")

        return cls.current._session

    def run(self) -> None:
        """Main worker loop - polls queue and processes tasks."""
        from agentexec.core.queue import dequeue

        logger.info(f"Worker {self._worker_id} starting")

        try:
            while not self._shutdown_event.is_set():
                # Dequeue task (blocks with timeout to check shutdown event)
                if (task := dequeue()) is not None:
                    self._process_task(task)
        except Exception as e:
            logger.exception(f"Worker {self._worker_id} fatal error: {e}")
            raise
        finally:
            self._cleanup()

    def _process_task(self, task: Task) -> None:
        """Process a single task from the queue.

        Args:
            task: Task to process
        """
        logger.info(f"Worker {self._worker_id} processing task: {task.task_name}")

        try:
            handler = self._handlers[task.task_name]
            task.started()

            if asyncio.iscoroutinefunction(handler):
                self._loop.run_until_complete(handler(**task.handler_kwargs))
            else:
                handler(**task.handler_kwargs)

            task.completed()
            logger.info(f"Worker {self._worker_id} completed task: {task.task_name}")

        except KeyError:
            logger.error(f"No handler registered for task: {task.task_name}")
            raise RuntimeError(f"No handler for task: {task.task_name}")
        except Exception as e:
            task.errored(e)
            logger.exception(f"Worker {self._worker_id} error executing task {task.task_name}: {e}")

    def _cleanup(self) -> None:
        """Clean up worker resources on shutdown."""
        self._session.close()
        logger.debug(f"Worker {self._worker_id} closed database session")

        self._loop.close()
        logger.info(f"Worker {self._worker_id} shutting down")


class WorkerPool:
    """Manages a pool of worker processes for background task execution.

    The WorkerPool coordinates multiple worker processes, handles task handler
    registration, and manages graceful shutdown. Each worker runs in a separate
    process with its own isolated state (session, event loop).

    This allows multiple pools to coexist if needed, each managing their own
    set of workers and handlers.

    Example:
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///agents.db")
        pool = WorkerPool(engine=engine)

        @pool.task("research_company")
        async def research_company(payload: dict, agent_id: str):
            company_name = payload["company_name"]
            # Task implementation
            pass

        pool.start()
    """

    _engine: Engine
    _handlers: dict[str, TaskHandler]
    _processes: list[mp.Process]
    _shutdown_event: EventClass

    def __init__(self, engine: Engine) -> None:
        """Initialize the worker pool.

        Args:
            engine: SQLAlchemy engine for activity tracking.
                   Each worker will create its own session from this engine.

        Configuration is loaded from agentexec.CONF:
        - num_workers: Number of worker processes to spawn
        - queue_name: Name of the Redis list to use as task queue
        - redis_url: Redis connection URL (via get_redis())
        """
        self._engine = engine
        self._handlers: dict[str, TaskHandler] = {}
        self._processes: list[mp.Process] = []
        self._shutdown_event: EventClass = mp.Event()

    def task(self, name: str) -> Callable[[TaskHandler], TaskHandler]:
        """Decorator to register a task handler.

        Args:
            name: Task type name that will be used when enqueueing.

        Returns:
            Decorator function that registers the handler.

        Example:
            @pool.task("research_company")
            async def research_company(payload: dict, agent_id: str):
                company_name = payload["company_name"]
                # Implementation
                pass
        """

        def decorator(func: TaskHandler) -> TaskHandler:
            self._handlers[name] = func
            logger.info(f"Registered task handler: {name}")
            return func

        return decorator

    def start(self) -> None:
        """Start the worker processes.

        Spawns N worker processes (configured via num_workers) that will poll
        the Redis queue and execute registered task handlers.

        Each worker runs independently with its own session and event loop.
        """
        logger.info(f"Starting {CONF.num_workers} worker processes")

        for worker_id in range(CONF.num_workers):
            process = mp.Process(
                target=Worker.run_in_process,
                args=(worker_id, self._engine, self._handlers, self._shutdown_event),
                daemon=False,
            )
            process.start()
            self._processes.append(process)
            logger.info(f"Started worker process {worker_id} (PID: {process.pid})")

    def shutdown(self, timeout: int | None = None) -> None:
        """Gracefully shutdown all worker processes.

        Args:
            timeout: Maximum seconds to wait for workers to finish current task.
                    Defaults to CONF.graceful_shutdown_timeout.
        """
        if timeout is None:
            timeout = CONF.graceful_shutdown_timeout

        logger.info("Initiating graceful shutdown of worker pool")
        self._shutdown_event.set()

        for process in self._processes:
            process.join(timeout=timeout)
            if process.is_alive():
                logger.warning(f"Worker process {process.pid} did not stop gracefully, terminating")
                process.terminate()
                process.join(timeout=5)

        self._processes.clear()
        logger.info("Worker pool shutdown complete")
