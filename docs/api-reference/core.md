# Core API Reference

This document covers the core agentexec API for task queuing, configuration,
and database setup.

All database access is async. Pool construction requires an async SQLAlchemy
engine (`sqlalchemy.ext.asyncio.create_async_engine`) or an async database
URL (e.g. `sqlite+aiosqlite://`, `postgresql+asyncpg://`).

## Module: agentexec

The main module exports:

```python
import agentexec as ax

# Core
ax.enqueue()
ax.gather()
ax.get_result()
ax.Priority
ax.Task

# Worker
ax.Pool

# Activity
ax.activity

# Runner (optional, requires openai-agents)
ax.OpenAIRunner

# Pipeline
ax.Pipeline
ax.Tracker

# Database
ax.Base

# Configuration
ax.CONF
```

---

## enqueue()

Queue a task for background execution.

```python
async def enqueue(
    task_name: str,
    context: BaseModel,
    *,
    priority: Priority = Priority.LOW,
    metadata: dict[str, Any] | None = None,
) -> Task
```

`enqueue()` writes an activity record to the database, so
`configure_engine()` must have been called first — either directly or as a
side-effect of importing a module that instantiates `Pool`.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task_name` | `str` | required | Name of the registered task |
| `context` | `BaseModel` | required | Pydantic model with task parameters |
| `priority` | `Priority` | `Priority.LOW` | Task priority |
| `metadata` | `dict \| None` | `None` | Arbitrary metadata attached to the activity record |

### Returns

`Task` — the created task instance with `agent_id` for tracking.

### Example

```python
from pydantic import BaseModel
import agentexec as ax

class MyContext(BaseModel):
    data: str

task = await ax.enqueue("my_task", MyContext(data="value"))
print(task.agent_id)  # UUID for tracking
```

### With Priority

```python
# High priority (processed first)
task = await ax.enqueue(
    "urgent_task",
    UrgentContext(...),
    priority=ax.Priority.HIGH,
)

# Low priority (default)
task = await ax.enqueue(
    "background_task",
    BackgroundContext(...),
)
```

### With Metadata

```python
task = await ax.enqueue(
    "research",
    ResearchContext(company="Acme"),
    metadata={"organization_id": "org-123"},
)
```

---

## gather()

Wait for multiple tasks to complete and return their results.

```python
async def gather(
    *tasks: Task,
    timeout: int = 300,
) -> tuple[BaseModel, ...]
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `*tasks` | `Task` | required | Variable number of Task instances |
| `timeout` | `int` | `300` | Maximum seconds to wait per task |

### Returns

`tuple[BaseModel, ...]` — results from each task in the same order.

### Example

```python
task1 = await ax.enqueue("task_a", ContextA(...))
task2 = await ax.enqueue("task_b", ContextB(...))
task3 = await ax.enqueue("task_c", ContextC(...))

result_a, result_b, result_c = await ax.gather(task1, task2, task3)
```

---

## get_result()

Wait for a single task result.

```python
async def get_result(
    task: Task,
    timeout: int = 300,
) -> BaseModel
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task` | `Task` | required | The Task instance to wait for |
| `timeout` | `int` | `300` | Maximum seconds to wait |

### Returns

`BaseModel` — the task handler's Pydantic return value.

### Raises

- `TimeoutError` — if the task doesn't complete within `timeout` seconds.

### Example

```python
task = await ax.enqueue("my_task", MyContext(...))

# Wait up to 5 minutes for result
result = await ax.get_result(task, timeout=300)
```

---

## Priority

Enum for task priority levels.

```python
class Priority(str, Enum):
    HIGH = "high"
    LOW = "low"
```

| Value | Behavior | Use Case |
|-------|----------|----------|
| `HIGH` | Added to front of queue | Urgent, user-facing tasks |
| `LOW` | Added to back of queue | Background processing |

---

## Task

Represents a queued task instance.

```python
class Task(BaseModel):
    task_name: str
    context: dict
    agent_id: UUID
    retry_count: int = 0
    metadata: dict | None = None
```

### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `task_name` | `str` | Name of the registered task |
| `context` | `dict` | Serialized task input |
| `agent_id` | `UUID` | Unique identifier for tracking |
| `retry_count` | `int` | Number of times this task has been retried |
| `metadata` | `dict \| None` | Metadata attached at enqueue time |

Task instances are typically created via `ax.enqueue()`, not constructed
directly.

---

## Pool

Manages multi-process worker execution.

```python
class Pool:
    def __init__(
        self,
        engine: AsyncEngine | None = None,
        database_url: str | None = None,
    )
```

Construction requires either an async engine or an async database URL. The
constructor calls `configure_engine()` as a side effect so subsequent calls
to `ax.enqueue()` in the same process will succeed.

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `engine` | `AsyncEngine \| None` | `None` | Async SQLAlchemy engine |
| `database_url` | `str \| None` | `None` | Async database URL (alternative to `engine`) |

At least one of `engine` or `database_url` must be provided.

### Methods

#### `@pool.task()`

Register a task handler.

```python
def task(
    self,
    name: str,
    *,
    lock_key: str | None = None,
) -> Callable
```

**Parameters:**
- `name` — Unique task identifier.
- `lock_key` — Optional string template for distributed locking, evaluated
  against context fields (e.g. `"user:{user_id}"`). Serializes tasks that
  share the same evaluated key.

**Example:**
```python
@pool.task("my_task")
async def my_task(agent_id: UUID, context: MyContext) -> MyResult:
    return MyResult(...)

# With partition locking
@pool.task("sync_user", lock_key="user:{user_id}")
async def sync_user(agent_id: UUID, context: UserContext):
    ...
```

#### `pool.add_task()`

Programmatic alternative to the decorator.

```python
def add_task(
    self,
    name: str,
    func: TaskHandler,
    *,
    context_type: type[BaseModel] | None = None,
    result_type: type[BaseModel] | None = None,
    lock_key: str | None = None,
) -> None
```

#### `@pool.schedule()`

Register and schedule a task in one step.

```python
def schedule(
    self,
    name: str,
    every: str,
    *,
    context: BaseModel | None = None,
    repeat: int = -1,
    lock_key: str | None = None,
    metadata: dict | None = None,
) -> Callable
```

**Parameters:**
- `name` — Task identifier.
- `every` — Cron expression (`"min hour dom mon dow"`).
- `context` — Pydantic context passed to the handler each tick
  (defaults to an empty model).
- `repeat` — `-1` for forever (default), `0` for one-shot, `N` for `N`
  additional executions.
- `lock_key`, `metadata` — Same as `@pool.task()`.

**Example:**
```python
@pool.schedule("refresh_cache", "*/5 * * * *")
async def refresh(agent_id: UUID, context: EmptyContext):
    ...
```

#### `pool.add_schedule()`

Schedule a task that's already registered.

```python
def add_schedule(
    self,
    task_name: str,
    every: str,
    context: BaseModel,
    *,
    repeat: int = -1,
    metadata: dict | None = None,
) -> None
```

#### `await pool.start()`

Start workers and run until shutdown. Async entry point.

```python
async def start(self) -> None
```

This is the entry point called by `agentexec run module:pool`. Invoke it
directly from your own async code if you need more control than the CLI
provides.

#### `await pool.shutdown()`

Gracefully shut down workers. Called automatically when `start()` receives a
cancellation.

```python
async def shutdown(self, timeout: int | None = None) -> None
```

**Parameters:**
- `timeout` — Seconds to wait per worker. Defaults to
  `CONF.graceful_shutdown_timeout`.

### Starting a Pool

The recommended entry point is the CLI:

```bash
agentexec run myapp.worker:pool --create-tables
```

See [CLI reference](#cli-agentexec) for all flags.

To start a pool programmatically:

```python
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
import agentexec as ax

engine = create_async_engine("sqlite+aiosqlite:///agents.db")
pool = ax.Pool(engine=engine)

@pool.task("research")
async def research(agent_id: UUID, context: ResearchContext):
    ...

async def main():
    async with engine.begin() as conn:
        await conn.run_sync(ax.Base.metadata.create_all)
    await pool.start()

asyncio.run(main())
```

---

## Base

SQLAlchemy declarative base for agentexec models.

```python
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    pass
```

### Usage

Use `Base.metadata` with Alembic for production migrations, or with
`create_all()` for quick prototyping:

```python
from sqlalchemy.ext.asyncio import create_async_engine
import agentexec as ax

engine = create_async_engine("sqlite+aiosqlite:///agents.db")

async with engine.begin() as conn:
    await conn.run_sync(ax.Base.metadata.create_all)
```

### Tables Created

- `{prefix}activity` — Activity records
- `{prefix}activity_log` — Activity log entries

Where `{prefix}` is `CONF.table_prefix` (default: `agentexec_`).

---

## Database Session Management

Located in `agentexec.core.db`.

### configure_engine()

Set the process-wide async engine. Called automatically by `Pool.__init__`;
call it yourself in processes that don't create a Pool (e.g. an API server
that only enqueues tasks).

```python
def configure_engine(engine: AsyncEngine) -> None
```

### get_session()

Create a new `AsyncSession` from the configured engine. Use as an async
context manager.

```python
def get_session() -> AsyncSession
```

**Raises:** `RuntimeError` if `configure_engine()` hasn't been called.

**Example:**
```python
from agentexec.core.db import get_session

async with get_session() as db:
    result = await db.execute(...)
```

### dispose_engine()

Dispose the engine and clear the session factory. Called during pool
shutdown.

```python
async def dispose_engine() -> None
```

---

## CONF

Global configuration instance. See
[Configuration](../getting-started/configuration.md) for the full list of
settings and their environment variable bindings.

```python
import agentexec as ax

print(ax.CONF.redis_url)
print(ax.CONF.num_workers)
```

---

## CLI: `agentexec`

```
agentexec run MODULE:POOL [OPTIONS]
```

Imports a Pool instance and starts processing tasks.

| Option | Description |
|--------|-------------|
| `--workers N`, `-w N` | Number of worker processes (default: `CONF.num_workers`) |
| `--create-tables` | Run `Base.metadata.create_all` before starting |
| `--log-level LEVEL` | Log level (default: `INFO`) |
| `--max-retries N` | Max task retries before giving up |
| `--shutdown-timeout N` | Graceful shutdown timeout in seconds |
| `--scheduler-timezone TZ` | IANA timezone for cron schedules |

**Example:**
```bash
agentexec run myapp.worker:pool --create-tables -w 4
```
