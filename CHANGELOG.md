# Changelog

## Unreleased

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
