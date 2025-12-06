# Core API Reference

This document covers the core agentexec API for task queuing, configuration, and database setup.

## Module: agentexec

The main module exports all public APIs:

```python
import agentexec as ax

# Core
ax.enqueue()
ax.gather()
ax.get_result()
ax.Priority

# Worker
ax.Pool

# Activity
ax.activity

# Runner
ax.OpenAIRunner

# Pipeline
ax.Pipeline

# Database
ax.Base

# Configuration
ax.CONF

# Redis
ax.close_redis()
```

---

## enqueue()

Queue a task for background execution.

```python
async def enqueue(
    task_name: str,
    context: BaseModel,
    priority: Priority = Priority.LOW,
    queue_name: str | None = None,
) -> Task
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task_name` | `str` | required | Name of the registered task |
| `context` | `BaseModel` | required | Pydantic model with task parameters |
| `priority` | `Priority` | `Priority.LOW` | Task priority |
| `queue_name` | `str \| None` | `None` | Queue name (uses `CONF.queue_name` if None) |

### Returns

`Task` - The created task instance with `agent_id` for tracking.

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
    priority=ax.Priority.HIGH
)

# Low priority (default)
task = await ax.enqueue(
    "background_task",
    BackgroundContext(...)
)
```

### With Custom Queue

```python
task = await ax.enqueue(
    "email_task",
    EmailContext(...),
    queue_name="email_queue"
)
```

---

## gather()

Wait for multiple tasks to complete and return their results.

```python
async def gather(*tasks: Task) -> tuple[Any, ...]
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `*tasks` | `Task` | Variable number of Task instances |

### Returns

`tuple[Any, ...]` - Results from each task in the same order.

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
    timeout: float = 300
) -> Any
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task` | `Task` | required | The Task instance to wait for |
| `timeout` | `float` | `300` | Maximum seconds to wait |

### Returns

`Any` - The task's return value.

### Raises

- `TimeoutError` - If task doesn't complete within timeout

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
class Priority(Enum):
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
class Task:
    agent_id: UUID
    task_name: str
    context: BaseModel
```

### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `agent_id` | `UUID` | Unique identifier for tracking |
| `task_name` | `str` | Name of the registered task |
| `context` | `BaseModel` | Task parameters |

### Methods

#### create()

```python
@classmethod
def create(
    cls,
    task_name: str,
    context: BaseModel,
) -> Task
```

Create a new task instance (called internally by `enqueue()`).

#### execute()

```python
async def execute(self) -> Any
```

Execute the task handler (called by workers).

---

## Pool

Manages multi-process worker execution.

```python
class Pool:
    def __init__(
        self,
        engine: Engine,
        database_url: str | None = None,
        queue_name: str | None = None,
    )
```

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `engine` | `Engine` | required | SQLAlchemy engine |
| `database_url` | `str \| None` | `None` | Database URL for workers |
| `queue_name` | `str \| None` | `None` | Redis queue name |

### Methods

#### task()

Register a task handler.

```python
def task(self, name: str) -> Callable
```

**Parameters:**
- `name` - Unique task identifier

**Returns:** Decorator function

**Example:**
```python
@pool.task("my_task")
async def my_task(agent_id: UUID, context: MyContext) -> str:
    return "result"
```

#### start()

Start workers (non-blocking).

```python
def start(self) -> None
```

#### run()

Start workers and block until shutdown.

```python
def run(self) -> None
```

#### shutdown()

Gracefully shut down workers.

```python
def shutdown(self, timeout: int | None = None) -> None
```

**Parameters:**
- `timeout` - Seconds to wait (uses `CONF.graceful_shutdown_timeout` if None)

### Example

```python
from sqlalchemy import create_engine
import agentexec as ax

engine = create_engine("sqlite:///agents.db")
ax.Base.metadata.create_all(engine)

pool = ax.Pool(
    engine=engine,
    database_url="sqlite:///agents.db"
)

@pool.task("research")
async def research(agent_id: UUID, context: ResearchContext):
    ...

pool.run()  # Blocks until SIGTERM/SIGINT
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

```python
from sqlalchemy import create_engine
import agentexec as ax

engine = create_engine("sqlite:///agents.db")

# Create all agentexec tables
ax.Base.metadata.create_all(engine)
```

### Tables Created

- `{prefix}activity` - Activity records
- `{prefix}activity_log` - Activity log entries

Where `{prefix}` is `CONF.table_prefix` (default: `agentexec_`).

---

## CONF

Global configuration instance.

```python
CONF: Config
```

### Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `redis_url` | `str` | required | Redis connection URL |
| `queue_name` | `str` | `"agentexec_tasks"` | Default queue name |
| `table_prefix` | `str` | `"agentexec_"` | Database table prefix |
| `num_workers` | `int` | `4` | Number of worker processes |
| `graceful_shutdown_timeout` | `int` | `300` | Shutdown timeout (seconds) |
| `redis_pool_size` | `int` | `10` | Redis connection pool size |
| `redis_pool_timeout` | `int` | `5` | Pool connection timeout |
| `result_ttl` | `int` | `3600` | Result cache TTL (seconds) |
| `activity_message_create` | `str` | `"Waiting to start."` | Initial activity message |
| `activity_message_started` | `str` | `"Task started."` | Running status message |
| `activity_message_complete` | `str` | `"Task completed successfully."` | Complete status message |
| `activity_message_error` | `str` | `"Task failed with error: {error}"` | Error message template |

### Example

```python
import agentexec as ax

print(ax.CONF.redis_url)
print(ax.CONF.num_workers)
print(ax.CONF.queue_name)
```

---

## close_redis()

Close the Redis connection pool.

```python
async def close_redis() -> None
```

### Usage

```python
# In FastAPI lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await ax.close_redis()
```

---

## Database Session Management

### set_global_session()

Set up process-local database session (called by workers).

```python
def set_global_session(engine: Engine) -> None
```

### get_global_session()

Get the current process-local session.

```python
def get_global_session() -> Session
```

### remove_global_session()

Clean up the process-local session.

```python
def remove_global_session() -> None
```

---

## Redis Client

### get_redis()

Get async Redis client.

```python
async def get_redis() -> redis.asyncio.Redis
```

### get_redis_sync()

Get synchronous Redis client.

```python
def get_redis_sync() -> redis.Redis
```

### Example

```python
from agentexec.core.redis_client import get_redis

async def check_queue():
    redis = await get_redis()
    length = await redis.llen(ax.CONF.queue_name)
    print(f"Queue length: {length}")
```
