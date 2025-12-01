# Changelog

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
