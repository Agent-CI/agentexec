# Changelog

## v0.2.0rc2

Second release candidate for 0.2.0. Completes the async migration and
stabilizes the worker pool architecture.

### Breaking Changes

**Fully async database layer**
- `configure_engine()` and `get_session()` now require an async SQLAlchemy engine (`AsyncEngine`) and return `AsyncSession`
- Database URLs must use async drivers (e.g. `sqlite+aiosqlite://`, `postgresql+asyncpg://`)
- `sqlalchemy[asyncio]` is now a core dependency; `aiosqlite` added as dev dependency

**Async activity query API**
- `activity.list()`, `activity.detail()`, and `activity.count_active()` are now async and accept `AsyncSession`
- Activity handlers are now async (`async def __call__`)

**Removed `session` parameter from activity mutations**
- `activity.create()`, `activity.update()`, `activity.complete()`, and `activity.error()` no longer accept a `session` parameter — the handler owns its own session lifecycle

**Queue priority parameter**
- `BaseQueueBackend.push()` signature changed from `high_priority: bool` to `priority: Priority | None`
- Affects Redis, Kafka, and any custom queue backend implementations

### New Features

**CLI entrypoint**
- New `agentexec` CLI command via `[project.scripts]`

**Activity model `create()` classmethod**
- `Activity.create()` encapsulates record + initial log entry creation in one async call

**Async engine disposal**
- `dispose_engine()` ensures the async engine's background threads exit cleanly on shutdown

### Architecture Changes

**Worker pool refactor**
- Workers now use the `spawn` multiprocessing start method with explicit context — no inherited state
- `StateEvent` replaced with stdlib `multiprocessing.Event` — removes dependency on the state backend for shutdown coordination
- Event handling and scheduling extracted into `_EventHandler` and `_Scheduler` classes
- Worker processes are now daemonic with `SIGINT` ignored — clean shutdown driven by the event
- `pool.start()` handles `CancelledError` directly for Ctrl+C shutdown

**Logging overhaul**
- Removed `worker/logging.py` and `core/logging.py` — all modules use stdlib `logging.getLogger(__name__)`
- Spawned workers bootstrap a `StreamHandler` on the root logger so logs reach stderr
- Pool messages use `logger.info`/`logger.error` instead of `print()`

### Bug Fixes

- Fixed crash when worker receives a task for an unregistered task name (now logs error and skips)
- Failed tasks now log full tracebacks via `logger.exception` instead of `logger.error`
- Kafka consumer handles `None` message values without crashing
- `ActivityUpdated.status` is now `Status` enum instead of raw string
- `raise e` instead of bare `raise` in task execution for clearer tracebacks

### Improvements

- Dependency groups use PEP 735 `[dependency-groups]` instead of `[tool.uv]`
- Ruff line-length increased to 110
- Removed verbose docstring examples and redundant comments throughout activity models
- `selectinload` for eager loading of activity logs in `get_by_agent_id`
- Redis backend adds `type: ignore` annotations for redis-py async stubs

## v0.2.0rc1

First release candidate for 0.2.0. Major refactor of the backend, queue,
activity, and worker systems.

### Breaking Changes

**Backend module restructure**
- `agentexec.state.redis_backend` renamed to `agentexec.state.redis` — update `AGENTEXEC_STATE_BACKEND` if set explicitly
- `AGENTEXEC_QUEUE_NAME` renamed to `AGENTEXEC_QUEUE_PREFIX` (old name still accepted as alias)

**Async activity API**
- Activity functions are now async: `await ax.activity.create(...)`, `await ax.activity.update(...)`, etc.

**Task context serialization**
- `Task.context` is now `Mapping[str, Any]` (raw dict), not a typed BaseModel — hydration happens at execution time
- `Task.create()` is now async

**Removed APIs**
- `set_global_session`/`get_global_session`/`remove_global_session` — use `configure_engine`/`get_session`
- `state.backend.publish`/`subscribe` (pubsub), `index_add`/`index_range`/`index_remove`, `clear`, `configure`

### New Features

**Partitioned Redis queues**
- Tasks with `lock_key` route to dedicated partition queues with per-partition locking and SCAN-based fair dequeue

**Activity handler pattern**
- Pluggable persistence via `PostgresHandler` (default) and `IPCHandler` (worker processes)

**Task retry**
- Failed tasks requeue as high priority with `AGENTEXEC_MAX_TASK_RETRIES` (default 3)

**Kafka backend (experimental)**
- `pip install agentexec[kafka]` for queue and schedule via Kafka

**Typed worker IPC**
- `TaskFailed`, `LogEntry`, `ActivityUpdated` messages over `multiprocessing.Queue`

**Schedule composite keys**
- `{task_name}:{cron}:{context_hash}` for unique schedule identity

### Improvements

- Class-based backend architecture with ABCs (`BaseStateBackend`, `BaseQueueBackend`, `BaseScheduleBackend`)
- `Task` is pure data, `TaskDefinition` owns behavior
- Session management via `configure_engine`/`get_session` (Pool owns the engine)
- Status enum extracted to `activity/status.py` (no SQLAlchemy dependency)

## v0.1.7

### New Features

**Scheduled tasks with cron expressions**
- `@pool.schedule("task_name", "*/5 * * * *")` decorator registers and schedules a task in one step
- `pool.add_schedule()` for imperative scheduling of already-registered tasks
- Cron expressions evaluated in configurable timezone (`AGENTEXEC_SCHEDULER_TIMEZONE`, default UTC)
- Repeat budget: `-1` for forever (default), `0` for one-shot, `N` for N more executions
- Scheduler runs automatically inside `pool.run()` — no extra setup needed
- Idempotent registration: keyed by task name, so restarts and multiple pool instances overwrite instead of duplicating
- Clock-drift resilient: next run computed from intended anchor time, not wall clock
- Skips missed intervals after downtime instead of enqueuing a burst of catch-up tasks
- New `croniter` dependency for cron expression parsing

### Improvements

**State backend sorted set operations**
- Added `zadd()`, `zrangebyscore()`, `zrem()` to `StateBackend` protocol and Redis implementation
- Used internally by the scheduler for efficient due-task polling

## v0.1.6

### New Features

**Task-level distributed locking**
- New `lock_key` parameter on `@pool.task()` and `pool.add_task()` for sequential execution of tasks sharing state
- String template evaluated against context fields (e.g., `lock_key="user:{user_id}"`)
- Workers acquire a Redis lock before execution; tasks requeue automatically on contention
- Lock released in `finally` block on completion or error
- Configurable TTL via `AGENTEXEC_LOCK_TTL` (default 1800s) as safety net for worker process death
- Note: strict FIFO ordering is not guaranteed between tasks sharing the same lock key

**Activity metadata for multi-tenancy**
- Attach arbitrary metadata when creating activities (e.g., `metadata={"organization_id": "org-123"}`)
- Filter activities by metadata in `activity.list()` and `activity.detail()`
- Metadata accessible as attribute for programmatic use but excluded from API serialization by default to prevent accidental tenant info leakage

### Improvements

**Redis cleanup on shutdown**
- `state.clear_keys()` removes all agentexec-prefixed keys and the task queue on shutdown
- Prevents stale tasks from being picked up on restart

**State backend lock primitives**
- Added `acquire_lock()` and `release_lock()` to `StateBackend` protocol
- Redis implementation uses atomic `SET NX EX` / `DELETE`

### Testing

- Added `test_task_locking.py` with 16 tests covering lock acquisition, release, requeue, and template evaluation
- Fixed `ty` type checker errors in `test_activity_tracking.py` (added narrowing guards for `Activity | None`)

## v0.1.5

### New Features

**Public `Pool.add_task()` method**
- `Pool.add_task()` is now public (was `_add_task()`)
- Alternative to `@pool.task()` decorator for programmatic task registration
- Includes comprehensive docstring with usage examples

### Improvements

**Enhanced type safety for TaskHandler protocols**
- Added generic type parameters (`ContextT`, `ResultT`) to `_SyncTaskHandler` and `_AsyncTaskHandler`
- Better IDE autocomplete and type checking support
- Added comprehensive type checking tests in `test_task_types.py`

**Activity tracking improvements**
- Made `percentage` field optional (`int | None`) in activity schemas
- More flexible activity percentage tracking

**Configuration robustness**
- Added `extra="ignore"` to config model for better forward compatibility

### Bug Fixes

**Database URL handling**
- Fixed database URL rendering to properly handle password visibility
- Uses `engine.url.render_as_string(hide_password=False)` instead of `str(engine.url)`

**Tracker commit**
- Added missing `db.commit()` call in activity tracker

### Testing

**Type checking tests**
- Added `test_task_types.py` for validating TaskHandler protocol compatibility
- Covers sync/async functions and class methods

## v0.1.4

### Breaking Changes

**Renamed `WorkerPool` to `Pool`**
- `ax.WorkerPool` is now `ax.Pool` for cleaner API
- Update imports: `from agentexec import Pool`

**Activity percentage field renamed**
- `completion_%` renamed to `percentage` for cleaner field naming

### New Features

**Pipelines run on workers**
- Pipelines can now be executed on worker processes
- Register pipelines with the pool and enqueue them like tasks

**Tracker for stateful counters**
- New `Tracker` class for managing stateful counters across workers
- Useful for tracking progress, metrics, and distributed state

**Strict Pipeline type flow validation**
- All step parameters and return types must be `BaseModel` subclasses
- Type flow between consecutive steps is validated at runtime
- Tuple returns are unpacked and matched to next step's parameters
- Final step must return a single `BaseModel` (not a tuple)
- Empty pipelines raise `RuntimeError` at class definition time

### Internal Improvements

**Type checking with `ty`**
- Added `ty` type checker to development workflow
- Better Protocol definitions for step handlers
- Improved type hints throughout pipeline module

**Better Pipeline flow tests**
- Comprehensive test coverage for valid and invalid type flows
- Tests for tuple unpacking, subclass compatibility, count mismatches
- Tests for primitive type rejection and edge cases

## v0.1.3

### Breaking Changes

**Self-describing JSON serialization replaces pickle**
- Task results now use JSON serialization with embedded type information (similar to pickle)
- Automatically stores fully qualified class name with data for type reconstruction
- No longer requires `TaskDefinition` registry for result deserialization
- `ax.gather()` now works with tasks created via `ax.enqueue()` without pool context
- **Migration**: Clear Redis or wait for TTL expiry on old pickled results

**TaskHandler Protocol enforces BaseModel returns**
- Task handlers must return a Pydantic `BaseModel` instance (not `None` or arbitrary objects)
- Return type is automatically inferred and validated at registration time
- Enables type-safe result retrieval and automatic serialization

### New Features

**State backend abstraction**
- Introduced `StateBackend` Protocol for pluggable state storage implementations
- Current Redis implementation moved to `agentexec.state.redis_backend`
- Backend modules verified against protocol at import time via `cast()`
- Prepares foundation for alternative backends (in-memory, DynamoDB, etc.)

**Improved async patterns**
- `brpop()` is now a proper async function (was sync returning coroutine)
- Consistent async/await usage across state operations
- Better type hints and IDE support

**Enhanced type safety**
- `TaskHandler` Protocol with support for both sync and async handlers
- Proper type annotations for all state backend operations
- `serialize()` and `deserialize()` type-enforced for `BaseModel` only

### Documentation

**Comprehensive documentation added**
- API reference for core modules (activity, pipeline, runner, task)
- Conceptual guides (architecture, task lifecycle, worker pool)
- Deployment guides (Docker, production best practices)
- Usage guides (basic usage, pipelines, FastAPI integration, OpenAI runner)
- Getting started (installation, quickstart, configuration)
- Contributing guide

### UI & Tooling

**React frontend and component library**
- Added `agentexec-ui` npm package with reusable React components
- Pre-built UI for agent monitoring and activity tracking
- TanStack Query integration for real-time updates
- React Router for navigation between agent list and detail views

**Docker deployment**
- Docker worker image for containerized deployments
- GitHub Actions for automated Docker image publishing to GitHub Container Registry
- GitHub Actions for automated npm publishing of UI components

### Testing

**Comprehensive test coverage**
- Achieved 89% code coverage
- Added unit tests for all core modules:
  - State backend and serialization (`test_state.py`, `test_state_backend.py`)
  - Self-describing results (`test_self_describing_results.py`)
  - Activity tracking schemas (`test_activity_schemas.py`)
  - Pipeline orchestration (`test_pipeline.py`)
  - Task queue operations (`test_queue.py`)
  - Worker events and logging (`test_worker_event.py`, `test_worker_logging.py`)
  - Database operations (`test_db.py`)
  - Configuration (`test_config.py`)

### Internal Improvements

**Redis client refactoring**
- Removed `core/redis_client.py` in favor of state backend abstraction
- Lazy connection initialization for both async and sync Redis clients
- Proper connection cleanup in `backend.close()`

**Key formatting consistency**
- All state keys use consistent `agentexec:` prefix via `backend.format_key()`
- Results: `agentexec:result:{agent_id}`
- Events: `agentexec:event:{name}:{id}`
- Logs channel: `agentexec:logs`

**Standardized function signatures**
- `get_result()` and `gather()` return `BaseModel` directly (not JSON strings)
- Consistent parameter ordering across state module functions
- Better docstrings with type information

## v0.1.2

### New Features

**Pipelines**
- Multi-step workflow orchestration with `ax.Pipeline`
- Define steps with `@pipeline.step(order)` decorator
- Parallel task execution with `ax.gather()`
- Result retrieval with `ax.get_result()`

**Worker logging via Redis pubsub**
- Workers publish logs to Redis, collected by main process
- Use `pool.run()` to see worker logs in real-time

### Internal Improvements

**Reorganized worker module**
- Worker code moved to `agentexec.worker` subpackage
- `RedisEvent` for cross-process shutdown coordination
- `get_worker_logger()` configures logging and returns logger in one call

**Refactored Redis client usage**
- Added `get_redis_sync()` for synchronous Redis operations
- Sync/async Redis clients for different contexts

## v0.1.1

### Breaking Changes

**Async `enqueue()` function**
- `ax.enqueue()` is now async and must be awaited:
  ```python
  task = await ax.enqueue("task_name", MyContext(key="value"))
  ```

**Type-safe context with Pydantic BaseModel**
- Task context must be a Pydantic `BaseModel` instead of a raw `dict`
- Context class is automatically inferred from handler type hints:
  ```python
  class ResearchContext(BaseModel):
      company: str

  @pool.task("research")
  async def research(agent_id: UUID, context: ResearchContext):
      company = context.company  # Type-safe with IDE autocomplete
  ```

**Redis URL now required**
- `redis_url` defaults to `None` and must be explicitly configured via `REDIS_URL`
- Prevents accidental connections to wrong Redis instances

### New Features

**Configurable activity messages**
- Activity status messages are configurable via environment variables:
  ```bash
  AGENTEXEC_ACTIVITY_MESSAGE_CREATE="Waiting to start."
  AGENTEXEC_ACTIVITY_MESSAGE_STARTED="Task started."
  AGENTEXEC_ACTIVITY_MESSAGE_COMPLETE="Task completed successfully."
  AGENTEXEC_ACTIVITY_MESSAGE_ERROR="Task failed with error: {error}"
  ```

**Improved Task architecture**
- `Task` is now the primary execution object with `execute()` method
- `TaskDefinition` handles registration metadata and context class inference
- Full lifecycle management (QUEUED → RUNNING → COMPLETE/ERROR) encapsulated in `Task.execute()`

**Better SQLAlchemy session management**
- New `scoped_session` pattern for worker processes
- Proper session cleanup on worker shutdown

### Internal Improvements

- Switched to async Redis client (`redis.asyncio`)
- Consolidated cleanup code in worker `_run()` method
- Removed unused `debug` config option

## v0.1.0

Initial release.
