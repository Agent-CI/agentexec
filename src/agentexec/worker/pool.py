from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
from dataclasses import dataclass
from typing import Any, Callable
from uuid import uuid4

from pydantic import BaseModel
from sqlalchemy import Engine, create_engine

from agentexec.config import CONF
from agentexec.state import backend
import queue as stdlib_queue

from agentexec.core.db import remove_global_session, set_global_session
from agentexec.core.queue import dequeue, enqueue
from agentexec.core.task import Task, TaskDefinition, TaskHandler
from agentexec import schedule
from agentexec.worker.event import StateEvent
from agentexec.worker.logging import (
    DEFAULT_FORMAT,
    LogMessage,
    get_worker_logger,
)

__all__ = [
    "Worker",
    "Pool",
]


class Message(BaseModel):
    """Base event sent from a worker to the pool."""
    pass


class TaskCompleted(Message):
    task: Task


class TaskFailed(Message):
    task: Task
    error: str

    @classmethod
    def from_exception(cls, task: Task, exception: Exception) -> TaskFailed:
        return cls(task=task, error=str(exception))


class LogEntry(Message):
    record: LogMessage


class _EmptyContext(BaseModel):
    """Default context for scheduled tasks that don't need one."""

    pass


def _get_pool_id() -> str:
    """Get a unique pool ID for shutdown event keys."""
    return str(uuid4())


@dataclass
class WorkerContext:
    """Shared context passed from Pool to Worker processes."""

    database_url: str
    shutdown_event: StateEvent
    tasks: dict[str, TaskDefinition]
    tx: mp.Queue | None = None  # worker → pool message queue


class Worker:
    """Individual worker process with isolated state.

    Each worker configures the scoped Session factory on startup.
    Task handlers can use get_global_session() to get the process-local session.
    """

    _worker_id: int
    _context: WorkerContext
    logger: logging.Logger

    def __init__(self, worker_id: int, context: WorkerContext):
        """Initialize worker with isolated state.

        Args:
            worker_id: Unique identifier for this worker
            context: Shared context from Pool
        """
        self._worker_id = worker_id
        self._context = context
        self.logger = get_worker_logger(__name__, tx=context.tx)

    @classmethod
    def run_in_process(cls, worker_id: int, context: WorkerContext) -> None:
        """Entry point for running a worker in a new process.

        Args:
            worker_id: Unique identifier for this worker
            context: Shared context from Pool
        """
        instance = cls(worker_id, context)
        instance.run()

    def run(self) -> None:
        """Main worker entry point - sets up async loop and runs."""
        self.logger.info(f"Worker {self._worker_id} starting")

        backend.configure(worker_id=str(self._worker_id))

        # TODO: Make postgres session conditional on backend — not all backends
        # need it (e.g. Kafka). An empty/unset DATABASE_URL could skip this.
        engine = create_engine(self._context.database_url)
        set_global_session(engine)

        try:
            asyncio.run(self._run())
        except Exception as e:
            self.logger.exception(f"Worker {self._worker_id} fatal error: {e}")
            raise
        finally:
            asyncio.run(backend.close())  # TODO: avoid second asyncio.run — maybe fold into _run's finally
            remove_global_session()
            self.logger.info(f"Worker {self._worker_id} shutting down")

    def _send(self, message: Message) -> None:
        """Send a message to the pool via the multiprocessing queue."""
        if self._context.tx is not None:
            self._context.tx.put_nowait(message)

    async def _run(self) -> None:
        """Async main loop - polls queue and executes tasks.

        All events are sent to the pool via _send. The worker never
        manipulates the queue or writes to Postgres directly.
        Locking is handled by the queue backend during pop.
        """
        while not await self._context.shutdown_event.is_set():
            task = await dequeue()
            if task is None:
                continue

            definition = self._context.tasks[task.task_name]

            try:
                self.logger.info(f"Worker {self._worker_id} processing: {task.task_name}")
                await definition.execute(task)
                self.logger.info(f"Worker {self._worker_id} completed: {task.task_name}")
                self._send(TaskCompleted(task=task))
            except Exception as e:
                self._send(TaskFailed.from_exception(task, e))



class Pool:
    """Manages a pool of worker processes for background task execution.

    Tasks are registered via @pool.task() decorator. Workers process tasks
    from the Redis queue using the pool's task registry.

    Example:
        import agentexec as ax
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///agents.db")
        pool = ax.Pool(engine=engine)

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
    ) -> None:
        """Initialize the worker pool.

        Args:
            engine: SQLAlchemy engine (URL will be extracted for workers).
            database_url: Database URL string. Alternative to passing engine.

        Raises:
            ValueError: If neither engine nor database_url is provided.
        """

        if not engine and not database_url:
            raise ValueError("Either engine or database_url must be provided")

        engine = engine or create_engine(database_url)  # type: ignore[arg-type]
        set_global_session(engine)

        self._worker_queue: mp.Queue = mp.Queue()
        self._context = WorkerContext(
            database_url=database_url or engine.url.render_as_string(hide_password=False),
            shutdown_event=StateEvent("shutdown", _get_pool_id()),
            tasks={},
            tx=self._worker_queue,
        )
        self._processes = []
        self._log_handler = None
        self._pending_schedules: list[dict[str, Any]] = []

    def task(
        self,
        name: str,
        *,
        lock_key: str | None = None,
    ) -> Callable[[TaskHandler], TaskHandler]:
        """Decorator to register a task handler with this pool.

        Creates a TaskDefinition that captures the handler and its context class
        from type annotations.

        Args:
            name: Task name used when enqueueing and for worker routing.
            lock_key: Optional string template for distributed locking. Evaluated
                against context fields (e.g., "user:{user_id}"). When set, only
                one task with the same evaluated lock key can run at a time.

        Returns:
            Decorator function that returns the handler.

        Example:
            @pool.task("research_company")
            async def research(agent_id: UUID, context: ResearchContext) -> ResearchResult:
                ...

            @pool.task("associate_observation", lock_key="user:{user_id}")
            async def associate(agent_id: UUID, context: ObservationContext):
                ...
        """

        def decorator(func: TaskHandler) -> TaskHandler:
            self.add_task(name, func, lock_key=lock_key)
            return func

        return decorator

    def add_task(
        self,
        name: str,
        func: TaskHandler,
        *,
        context_type: type[BaseModel] | None = None,
        result_type: type[BaseModel] | None = None,
        lock_key: str | None = None,
    ) -> None:
        """Register a task handler with this pool.

        Alternative to the @pool.task() decorator for programmatic registration.

        Args:
            name: Task name used when enqueueing and for worker routing.
            func: Task handler function (sync or async).
            context_type: Optional explicit context type (inferred from annotations if not provided).
            result_type: Optional explicit result type (inferred from annotations if not provided).
            lock_key: Optional string template for distributed locking. Evaluated
                against context fields (e.g., "user:{user_id}"). When set, only
                one task with the same evaluated lock key can run at a time.

        Raises:
            ValueError: If a task with the same name is already registered.

        Example:
            pool.add_task("research_company", research_handler)
            pool.add_task("associate_observation", handler, lock_key="user:{user_id}")
        """
        if name in self._context.tasks:
            raise ValueError(f"Task '{name}' is already registered in this pool")

        definition = TaskDefinition(
            name=name,
            handler=func,
            context_type=context_type,
            result_type=result_type,
            lock_key=lock_key,
        )
        self._context.tasks[name] = definition

    def schedule(
        self,
        name: str,
        every: str,
        *,
        context: BaseModel | None = None,
        repeat: int = -1,
        lock_key: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Callable[[TaskHandler], TaskHandler]:
        """Decorator to register and schedule a task in one step.

        Combines ``@pool.task()`` and ``pool.add_schedule()`` — registers the
        handler as a task and schedules it to run on the given interval.

        Args:
            name: Task name used when enqueueing and for worker routing.
            every: Schedule expression (cron syntax: min hour dom mon dow).
            context: Pydantic context payload passed to the handler each time.
                Defaults to an empty BaseModel if not provided.
            repeat: How many additional executions after the first.
                -1 = forever (default), 0 = one-shot, N = N more times.
            lock_key: Optional string template for distributed locking.
            metadata: Optional metadata dict (e.g. for multi-tenancy).

        Returns:
            Decorator function that returns the handler.

        Example:
            @pool.schedule("refresh_cache", "*/5 * * * *")
            async def refresh(agent_id: UUID, context: RefreshContext):
                ...

            @pool.schedule("sync_users", "0 * * * *", context=SyncContext(full=True), repeat=3)
            async def sync(agent_id: UUID, context: SyncContext):
                ...
        """

        def decorator(func: TaskHandler) -> TaskHandler:
            self.add_task(name, func, lock_key=lock_key)
            self.add_schedule(
                name, every, context or _EmptyContext(),
                repeat=repeat, metadata=metadata,
            )
            return func

        return decorator

    def add_schedule(
        self,
        task_name: str,
        every: str,
        context: BaseModel,
        *,
        repeat: int = -1,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Schedule a registered task to run on a recurring interval.

        The task must already be registered via ``@pool.task()`` or
        ``pool.add_task()``.  The scheduler loop runs automatically
        inside ``pool.run()`` — no extra setup needed.

        Schedules are stored and registered with the backend when
        ``start()`` is called.

        Args:
            task_name: Name of a registered task.
            every: Schedule expression (cron syntax: min hour dom mon dow).
            context: Pydantic context payload passed to the handler each time.
            repeat: How many additional executions after the first.
                -1 = forever (default), 0 = one-shot, N = N more times.
            metadata: Optional metadata dict (e.g. for multi-tenancy).

        Raises:
            ValueError: If the task name is not registered with this pool.

        Example:
            pool.add_schedule("refresh_cache", "*/5 * * * *", RefreshContext(scope="all"))
            pool.add_schedule("refresh_cache", "0 * * * *", RefreshContext(scope="users"), repeat=3)
        """
        if task_name not in self._context.tasks:
            raise ValueError(
                f"Task '{task_name}' is not registered. "
                f"Use @pool.task() or pool.add_task() first."
            )

        self._pending_schedules.append(dict(
            task_name=task_name,
            every=every,
            context=context,
            repeat=repeat,
            metadata=metadata,
        ))

    async def start(self) -> None:
        """Start workers and run until they exit.

        Spawns worker processes, forwards logs, and processes scheduled
        tasks. This is the foreground entry point — it blocks until all
        workers finish. Use ``run()`` for a daemonized version that
        handles KeyboardInterrupt and cleanup.
        """
        await self._context.shutdown_event.clear()

        # Spawn workers before log handler to avoid pickling issues
        self._spawn_workers()

        # TODO make this configurable
        self._log_handler = logging.StreamHandler()
        self._log_handler.setFormatter(logging.Formatter(DEFAULT_FORMAT))

        from agentexec.activity.consumer import process_activity_stream

        await asyncio.gather(
            self._process_worker_events(),
            self._process_scheduled_tasks(),
            process_activity_stream(),
        )

    def run(self) -> None:
        """Start workers in a managed event loop with graceful shutdown.

        Calls ``start()`` inside ``asyncio.run()`` and handles
        KeyboardInterrupt, shutdown, and connection cleanup.
        """

        async def _loop() -> None:
            try:
                await self.start()
            except asyncio.CancelledError:
                pass
            finally:
                await self.shutdown()

        try:
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

    async def _process_scheduled_tasks(self) -> None:
        """Register pending schedules, then poll for due tasks and enqueue them."""
        for _schedule in self._pending_schedules:
            await schedule.register(**_schedule)
        self._pending_schedules.clear()

        while any(p.is_alive() for p in self._processes):
            await asyncio.sleep(CONF.scheduler_poll_interval)

            for scheduled_task in await backend.schedule.get_due():
                await enqueue(
                    scheduled_task.task_name,
                    context=backend.deserialize(scheduled_task.context),
                    metadata=scheduled_task.metadata,
                )

                if scheduled_task.repeat == 0:
                    await backend.schedule.remove(scheduled_task.task_name)
                else:
                    scheduled_task.advance()
                    await backend.schedule.register(scheduled_task)

    def _partition_key_for(self, task: Task) -> str | None:
        """Derive the partition/lock key for a task from its definition."""
        return self._context.tasks[task.task_name].get_lock_key(task.context)

    async def _process_worker_events(self) -> None:
        """Handle all events from worker processes via multiprocessing queue."""
        assert self._log_handler, "Log handler not initialized"

        while any(p.is_alive() for p in self._processes):
            try:
                message = self._worker_queue.get_nowait()
            except stdlib_queue.Empty:
                await asyncio.sleep(0.05)
                continue

            match message:
                case LogEntry(record=record):
                    self._log_handler.emit(record.to_log_record())

                case TaskCompleted(task=task):
                    partition_key = self._partition_key_for(task)
                    if partition_key:
                        await backend.queue.release_lock(CONF.queue_prefix, partition_key)

                case TaskFailed(task=task, error=error):
                    partition_key = self._partition_key_for(task)
                    if task.retry_count < CONF.max_task_retries:
                        task.retry_count += 1
                        await backend.queue.push(
                            CONF.queue_prefix,
                            task.model_dump_json(),
                            partition_key=partition_key,
                            high_priority=True,
                        )
                    else:
                        # TODO incorporate this messaging into the ax.activity stream.
                        print(
                            f"Task {task.task_name} failed "
                            f"after {task.retry_count + 1} attempts, giving up: {error}"
                        )
                    if partition_key:
                        await backend.queue.release_lock(CONF.queue_prefix, partition_key)

    async def shutdown(self, timeout: int | None = None) -> None:
        """Gracefully shutdown all worker processes.

        For use with start(). If using run(), shutdown is handled automatically.

        Args:
            timeout: Max seconds to wait per worker. Defaults to CONF.graceful_shutdown_timeout.
        """
        if timeout is None:
            timeout = CONF.graceful_shutdown_timeout

        print("Shutting down worker pool")
        await self._context.shutdown_event.set()

        for process in self._processes:
            process.join(timeout=timeout)
            if process.is_alive():
                print(f"Worker {process.pid} did not stop, terminating")
                process.terminate()
                process.join(timeout=5)

        self._processes.clear()
        await backend.close()
        print("Worker pool shutdown complete")
