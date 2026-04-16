# Worker Pool

The worker pool is the execution engine of agentexec. It manages multiple
Python processes that dequeue and execute tasks from the configured state
backend (Redis by default).

## Overview

```
┌─────────────────────────────────────────────────────────┐
│                      Pool                                │
│  ┌─────────────────────────────────────────────────┐    │
│  │                Main Process                       │    │
│  │  • Spawns worker processes                        │    │
│  │  • Runs the scheduler loop                        │    │
│  │  • Receives IPC events (TaskFailed, activity)    │    │
│  │  • Handles graceful shutdown                      │    │
│  └─────────────────────────────────────────────────┘    │
│                         │                                │
│       ┌─────────────────┼─────────────────┐             │
│       ▼                 ▼                 ▼             │
│  ┌─────────┐       ┌─────────┐       ┌─────────┐       │
│  │Worker 0 │       │Worker 1 │       │Worker 2 │       │
│  │  pop    │       │  pop    │       │  pop    │       │
│  │ execute │       │ execute │       │ execute │       │
│  │complete │       │complete │       │complete │       │
│  └─────────┘       └─────────┘       └─────────┘       │
└─────────────────────────────────────────────────────────┘
```

Workers are spawned using Python's `spawn` start method — every worker is a
fresh interpreter that re-imports the user's worker module. This means
modules you load at import time must be free of side effects like
`asyncio.run()` or `Base.metadata.create_all()`.

## Creating a Worker Pool

```python
from sqlalchemy.ext.asyncio import create_async_engine
import agentexec as ax

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
pool = ax.Pool(engine=engine)
```

Pool construction calls `configure_engine()` as a side effect, so other
modules in the same process can call `ax.enqueue()` once the Pool exists.

### Constructor Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `engine` | `AsyncEngine \| None` | Async SQLAlchemy engine |
| `database_url` | `str \| None` | Async database URL (alternative to `engine`) |

At least one of `engine` or `database_url` must be provided.

## Registering Tasks

Tasks are registered using the `@pool.task()` decorator:

```python
from uuid import UUID
from pydantic import BaseModel

class MyContext(BaseModel):
    data: str
    count: int = 1

@pool.task("my_task_name")
async def my_task(agent_id: UUID, context: MyContext) -> str:
    """Task handler function.

    Args:
        agent_id: Unique identifier for this task instance.
        context: Typed context with task parameters.

    Returns:
        Optional Pydantic result (stored for pipelines / get_result).
    """
    return "result"
```

### Distributed Locking (Partition Keys)

Use `lock_key` to serialize tasks that share a logical resource:

```python
@pool.task("sync_user", lock_key="user:{user_id}")
async def sync_user(agent_id: UUID, context: UserContext):
    ...
```

Tasks whose evaluated `lock_key` matches go to a dedicated partition queue
and execute one at a time. Tasks with different keys run concurrently.

## Starting Workers

### Recommended: CLI

```bash
agentexec run myapp.worker:pool --create-tables
```

The CLI imports your pool, optionally creates database tables, and calls
`await pool.start()`. It handles SIGINT/SIGTERM cleanly.

### Programmatic

```python
import asyncio

async def main():
    async with engine.begin() as conn:
        await conn.run_sync(ax.Base.metadata.create_all)
    await pool.start()

if __name__ == "__main__":
    asyncio.run(main())
```

## Worker Process Lifecycle

Each worker process goes through this lifecycle:

```
1. Spawn (fresh Python interpreter)
      │
      ▼
2. Initialize
   • Import your worker module (re-registers tasks)
   • Install stderr log handler
   • Set IPC activity handler
      │
      ▼
3. Main Loop ◄──────────────────────────┐
   • await queue.pop()                  │
   • lookup task definition             │
   • execute handler                    │
   • await queue.complete()             │
   • send activity events via IPC       │
   └────────────────────────────────────┘
      │ (shutdown_event set)
      ▼
4. Cleanup
   • backend.close()
   • Exit process
```

Worker processes are daemons (`daemon=True`) — if the main process dies
unexpectedly, workers die with it instead of orphaning and polling Redis
with stale code.

## Worker Configuration

### Number of Workers

```bash
export AGENTEXEC_NUM_WORKERS=8
```

Or pass `-w 8` to the CLI.

**Guidelines:**
- CPU-bound tasks: set to the number of CPU cores.
- I/O-bound tasks (most LLM calls): can exceed CPU cores.
- Start with 4–8 and adjust based on monitoring.

### Graceful Shutdown Timeout

```bash
export AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT=300  # 5 minutes
```

Pass `--shutdown-timeout N` to override per-run.

## Graceful Shutdown

### Signal Handling

- **SIGINT** (Ctrl+C) and **SIGTERM** trigger graceful shutdown.
- Workers ignore SIGINT in their own process (the parent signals shutdown
  via a shared `multiprocessing.Event`).

### Shutdown Process

```
1. Main process receives SIGINT/SIGTERM
      │
      ▼
2. Set shared shutdown_event
      │
      ▼
3. Workers finish current task and exit loop
      │
      ▼
4. Main process joins each worker (up to timeout)
      │
      ▼
5. Any remaining workers are terminate()'d
      │
      ▼
6. backend.close() and engine.dispose()
```

## Database Access in Tasks

Task handlers that need DB access should use `get_session()` as an async
context manager:

```python
from agentexec.core.db import get_session

@pool.task("inspect_user")
async def inspect_user(agent_id: UUID, context: UserContext):
    async with get_session() as db:
        user = await db.get(User, context.user_id)
        ...
```

`Pool.__init__` calls `configure_engine(engine)` in every process (parent
and workers) so `get_session()` works without additional setup.

## Logging

All log output routes through the standard `logging` module. In spawned
workers, a stderr handler is installed automatically so every logger —
`agentexec.*` and your own `myapp.*` — writes to the worker's stderr with a
consistent format:

```
[INFO/SpawnProcess-1] agentexec.worker.pool: Worker 0 processing: research
[INFO/SpawnProcess-1] myapp.tasks: Researching Anthropic...
```

Configure your own logger in user code with `logging.getLogger(__name__)`.

## Error Handling

### Task Errors

When a task handler raises, the worker:

1. Logs the exception with a full traceback.
2. Sends a `TaskFailed` message to the main process via IPC.
3. Marks the activity record as `ERROR`.
4. Continues to the next task.

The main process retries the task up to `CONF.max_task_retries` (default
`3`), pushing it back with `HIGH` priority so retries don't sit behind new
work.

```python
@pool.task("risky_task")
async def risky_task(agent_id: UUID, context: MyContext):
    raise ValueError("Something went wrong")
    # Activity becomes ERROR, worker continues, task retried up to 3 times.
```

The worker catches *any* exception from user code — including `SystemExit`,
`KeyboardInterrupt`, and other `BaseException` subclasses — so that a
poorly-behaved task or library can't take down the worker.

### Stale Activities on Startup

To reconcile activities left in `QUEUED`/`RUNNING` state after a previous
crash:

```python
canceled = await ax.activity.cancel_pending()
```

Call this from your own startup code if you want that behavior.

## Multiple Pools

You typically run one pool per deployment. If you need separate pools for
isolation (e.g. different task families with different memory profiles),
configure them to run as separate processes with different
`AGENTEXEC_QUEUE_PREFIX` values so they don't share a queue namespace.

## Monitoring

### Queue Depth (Redis)

```python
from agentexec.state import backend

async def queue_length() -> int:
    # Direct Redis access
    client = backend.client  # type: ignore
    return await client.llen(backend.queue._default_key)
```

### Activity Metrics

```python
from agentexec.activity import Status
from agentexec.core.db import get_session

async def metrics():
    running = await ax.activity.count_active()
    recent = await ax.activity.list(page=1, page_size=50)
    completed_last_hour = sum(
        1 for a in recent.items if a.status == Status.COMPLETE
    )
    return {"running": running, "completed_last_hour": completed_last_hour}
```

## Best Practices

### 1. Use Type Hints

Always use type hints for the `context` parameter — the decorator infers
the context type from the annotation.

```python
# Good - context type is inferred
@pool.task("task")
async def task(agent_id: UUID, context: MyContext):
    ...

# Bad - raises TypeError at registration time
@pool.task("task")
async def task(agent_id, context):
    ...
```

### 2. Keep Worker Module Free of Side Effects

Because workers re-import your module via `spawn`, anything at module scope
runs in every worker process. Don't put `asyncio.run()`, `create_all()`, or
`await ax.enqueue(...)` calls at the top level — only class and function
definitions, the Pool instance, and the task decorators.

### 3. Keep Tasks Focused

```python
# Good - single responsibility
@pool.task("send_email")
async def send_email(agent_id: UUID, context: EmailContext):
    await send(context.to, context.subject, context.body)
```

### 4. Use Timeouts

```python
import asyncio

@pool.task("api_task")
async def api_task(agent_id: UUID, context: APIContext):
    try:
        return await asyncio.wait_for(call_external_api(), timeout=30)
    except asyncio.TimeoutError:
        await ax.activity.error(agent_id, "API call timed out")
        raise
```

## Next Steps

- [Task Lifecycle](task-lifecycle.md) — Understand task states
- [Activity Tracking](activity-tracking.md) — Monitor task progress
- [Production Guide](../deployment/production.md) — Production deployment
