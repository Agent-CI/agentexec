from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
from dataclasses import dataclass
from multiprocessing.synchronize import Event as MPEvent
from typing import Any, Callable

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from agentexec.config import CONF
from agentexec.state import backend
import queue as stdlib_queue

from agentexec import activity
from agentexec.activity.events import ActivityEvent
from agentexec.activity.handlers import IPCHandler
from agentexec.core.db import configure_engine, dispose_engine
from agentexec.core.queue import enqueue
from agentexec.core.task import Task, TaskDefinition, TaskHandler
from agentexec import schedule

logger = logging.getLogger(__name__)

__all__ = [
    "Worker",
    "Pool",
]


class Message(BaseModel):
    """Base event sent from a worker to the pool."""

    pass


class TaskFailed(Message):
    task: Task
    error: str

    @classmethod
    def from_exception(cls, task: Task, exception: BaseException) -> TaskFailed:
        return cls(task=task, error=str(exception))


class _EmptyContext(BaseModel):
    """Default context for scheduled tasks that don't need one."""

    pass


@dataclass
class WorkerContext:
    """Shared context passed from Pool to Worker processes."""

    shutdown_event: MPEvent
    tasks: dict[str, TaskDefinition]
    tx: mp.Queue


class Worker:
    """Individual worker process with isolated state.

    Each worker configures the scoped Session factory on startup.
    Workers don't have database access — all persistence goes through the pool.
    """

    _worker_id: int
    _context: WorkerContext

    def __init__(self, worker_id: int, context: WorkerContext):
        """Initialize worker with isolated state.

        Args:
            worker_id: Unique identifier for this worker
            context: Shared context from Pool
        """
        self._worker_id = worker_id
        self._context = context

        activity.handler = IPCHandler(context.tx)

    @classmethod
    def run_in_process(cls, worker_id: int, context: WorkerContext) -> None:
        """Entry point for running a worker in a new process.

        Args:
            worker_id: Unique identifier for this worker
            context: Shared context from Pool
        """
        import signal

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        context.tx.cancel_join_thread()

        # Spawn doesn't inherit log handlers; bootstrap stderr for this process.
        root = logging.getLogger()
        if not root.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                "[%(levelname)s/%(processName)s] %(name)s: %(message)s"
            ))
            root.addHandler(handler)
            root.setLevel(logging.INFO)

        instance = cls(worker_id, context)
        instance.run()

    def run(self) -> None:
        """Main worker entry point - sets up async loop and runs."""
        logger.info(f"Worker {self._worker_id} starting")

        try:
            asyncio.run(self._run())
        except Exception as e:
            logger.exception(f"Worker {self._worker_id} fatal error: {e}")
            raise

    def _send(self, message: Message) -> None:
        """Send a message to the pool via the multiprocessing queue."""
        self._context.tx.put_nowait(message)

    async def _run(self) -> None:
        """Async main loop - dequeue, execute, complete."""
        try:
            while not self._context.shutdown_event.is_set():
                try:
                    if data := await backend.queue.pop(timeout=1):
                        task = Task.model_validate(data)
                        try:
                            definition = self._context.tasks[task.task_name]
                        except KeyError:
                            logger.error(
                                f"Worker {self._worker_id}: task '{task.task_name}' is not registered"
                            )
                            continue
                        partition_key = definition.get_lock_key(task.context)
                    else:
                        await asyncio.sleep(1)
                        continue

                    try:
                        logger.info(f"Worker {self._worker_id} processing: {task.task_name}")
                        await definition.execute(task)
                        logger.info(f"Worker {self._worker_id} completed: {task.task_name}")
                    except asyncio.CancelledError:
                        raise
                    except BaseException as e:
                        # Catch BaseException so SystemExit/KeyboardInterrupt in user code don't kill the worker.
                        logger.exception(f"Worker {self._worker_id} failed: {task.task_name}")
                        self._send(TaskFailed.from_exception(task, e))
                    finally:
                        await backend.queue.complete(partition_key)
                except Exception as e:
                    logger.exception(f"Worker {self._worker_id} error: {e}")
                    await asyncio.sleep(1)  # avoid tight loop when backend is unreachable
        finally:
            await backend.close()


class _EventHandler:
    shutdown_event: MPEvent
    queue: mp.Queue
    tasks: dict[str, TaskDefinition]

    def __init__(
        self,
        shutdown_event: MPEvent,
        queue: mp.Queue,
        tasks: dict[str, TaskDefinition],
    ) -> None:
        self.shutdown_event = shutdown_event
        self.queue = queue
        self.tasks = tasks

    async def __call__(self) -> None:
        """Process messages until a shutdown event is set."""
        while not self.shutdown_event.is_set():
            try:
                await self._handle()
            except stdlib_queue.Empty:
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.exception(f"Event handler error: {e}")

    async def cleanup(self) -> None:
        """Process messages until the queue is empty."""
        while not self.queue.empty():
            try:
                await self._handle()
            except stdlib_queue.Empty:
                break
            except Exception as e:
                logger.exception(f"Event handler cleanup error: {e}")

    def _partition_key_for(self, task: Task) -> str | None:
        """Derive the partition/lock key for a task from its definition."""
        return self.tasks[task.task_name].get_lock_key(task.context)

    async def _handle(self) -> None:
        """Handle a single worker event."""
        message = self.queue.get_nowait()
        match message:
            case TaskFailed(task=task, error=error):
                if task.retry_count < CONF.max_task_retries:
                    task.retry_count += 1
                    from agentexec.core.queue import Priority

                    await backend.queue.push(
                        task.model_dump_json(),
                        partition_key=self._partition_key_for(task),
                        priority=Priority.HIGH,
                    )
                else:
                    logger.info(
                        f"Task {task.task_name} failed "
                        f"after {task.retry_count + 1} attempts, giving up: {error}"
                    )

            case ActivityEvent():
                await activity.handler(message)


class _Scheduler:
    shutdown_event: MPEvent
    pending: list[dict[str, Any]]

    def __init__(self, shutdown_event: MPEvent, pending: list[dict[str, Any]]) -> None:
        self.shutdown_event = shutdown_event
        self.pending = pending

    async def __call__(self) -> None:
        await self._register_pending()

        while not self.shutdown_event.is_set():
            try:
                await self._process_due()
            except Exception as e:
                # TODO: exponential backoff on repeated failures.
                logger.exception(f"Scheduled task error: {e}")
            await asyncio.sleep(CONF.scheduler_poll_interval)

    async def _register_pending(self) -> None:
        """Register configured schedules."""
        for _schedule in self.pending:
            await schedule.register(**_schedule)
            logger.info(f"Scheduled {_schedule['task_name']}")
        self.pending.clear()

    async def _process_due(self) -> None:
        """Collect due tasks and enqueue them."""
        for scheduled_task in await backend.schedule.get_due():
            await enqueue(
                scheduled_task.task_name,
                context=backend.deserialize(scheduled_task.context),
                metadata=scheduled_task.metadata,
            )

            if scheduled_task.repeat == 0:
                await backend.schedule.remove(scheduled_task.key)
            else:
                scheduled_task.advance()
                await backend.schedule.register(scheduled_task)


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
    _processes: list[mp.process.BaseProcess]

    def __init__(
        self,
        engine: AsyncEngine | None = None,
        database_url: str | None = None,
    ) -> None:
        """Initialize the worker pool.

        Args:
            engine: Async SQLAlchemy engine.
            database_url: Async database URL string (e.g. ``"sqlite+aiosqlite:///..."``).
                Alternative to passing engine.

        Raises:
            ValueError: If neither engine nor database_url is provided.
        """

        if not engine and not database_url:
            raise ValueError("Either engine or database_url must be provided")

        self._engine = engine or create_async_engine(database_url)  # type: ignore[arg-type]
        configure_engine(self._engine)
        self._mp_context = mp.get_context("spawn")
        self._worker_queue: mp.Queue = self._mp_context.Queue()
        self._context = WorkerContext(
            shutdown_event=self._mp_context.Event(),
            tasks={},
            tx=self._worker_queue,
        )
        self._processes = []
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
                name,
                every,
                context or _EmptyContext(),
                repeat=repeat,
                metadata=metadata,
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
        when the pool starts — no extra setup needed.

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
                f"Task '{task_name}' is not registered. Use @pool.task() or pool.add_task() first."
            )

        self._pending_schedules.append(
            dict(
                task_name=task_name,
                every=every,
                context=context,
                repeat=repeat,
                metadata=metadata,
            )
        )

    def _spawn_workers(self) -> None:
        """Spawn worker processes using the 'spawn' start method.

        Workers start fresh with no inherited state — connections and
        event loops are created from scratch in each process.
        """
        logger.info(f"Starting {CONF.num_workers} worker processes")

        for worker_id in range(CONF.num_workers):
            process = self._mp_context.Process(
                target=Worker.run_in_process,
                args=(worker_id, self._context),
                daemon=True,
            )
            process.start()
            self._processes.append(process)
            logger.info(f"Started worker {worker_id} (PID: {process.pid})")

    async def start(self) -> None:
        """Start workers and run until they exit.

        Spawns worker processes, forwards logs, and processes scheduled
        tasks. Handles shutdown on cancellation (Ctrl+C). This is the
        async entry point — use ``run()`` for a blocking version.
        """
        self._context.shutdown_event.clear()

        event_handler = _EventHandler(
            shutdown_event=self._context.shutdown_event,
            queue=self._worker_queue,
            tasks=self._context.tasks,
        )
        scheduler = _Scheduler(
            shutdown_event=self._context.shutdown_event,
            pending=self._pending_schedules,
        )

        try:
            self._spawn_workers()
            await asyncio.gather(
                event_handler(),
                scheduler(),
            )
        except asyncio.CancelledError:
            await event_handler.cleanup()
            await self.shutdown()

    async def shutdown(self, timeout: int | None = None) -> None:
        """Gracefully shutdown all worker processes.

        For use with start(). If using run(), shutdown is handled automatically.

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
                logger.error(f"Worker {process.pid} did not stop, terminating")
                process.terminate()
                process.join(timeout=5)

        self._processes.clear()

        await backend.close()
        await dispose_engine()
        logger.info("Worker pool shutdown complete")
