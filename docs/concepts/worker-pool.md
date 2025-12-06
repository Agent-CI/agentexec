# Worker Pool

The worker pool is the execution engine of agentexec. It manages multiple Python processes that dequeue and execute tasks from Redis.

## Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Pool                           │
│  ┌─────────────────────────────────────────────────┐   │
│  │                Main Process                      │   │
│  │  • Spawns worker processes                       │   │
│  │  • Collects logs via Redis pub/sub              │   │
│  │  • Handles graceful shutdown                     │   │
│  └─────────────────────────────────────────────────┘   │
│                         │                               │
│       ┌─────────────────┼─────────────────┐            │
│       ▼                 ▼                 ▼            │
│  ┌─────────┐       ┌─────────┐       ┌─────────┐      │
│  │Worker 0 │       │Worker 1 │       │Worker 2 │      │
│  │         │       │         │       │         │      │
│  │ BRPOP   │       │ BRPOP   │       │ BRPOP   │      │
│  │ Execute │       │ Execute │       │ Execute │      │
│  │ Log     │       │ Log     │       │ Log     │      │
│  └─────────┘       └─────────┘       └─────────┘      │
└─────────────────────────────────────────────────────────┘
```

## Creating a Worker Pool

```python
from sqlalchemy import create_engine
import agentexec as ax

# Create database engine
engine = create_engine("postgresql://user:pass@localhost/mydb")

# Create database tables (if they don't exist)
ax.Base.metadata.create_all(engine)

# Create worker pool
pool = ax.Pool(
    engine=engine,
    database_url="postgresql://user:pass@localhost/mydb",
    queue_name=None,  # Uses CONF.queue_name by default
)
```

### Constructor Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `engine` | `Engine` | SQLAlchemy engine for activity tracking |
| `database_url` | `str` | Database URL (passed to worker processes) |
| `queue_name` | `str | None` | Redis queue name (default: `CONF.queue_name`) |

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
    """
    Task handler function.

    Args:
        agent_id: Unique identifier for this task instance
        context: Typed context with task parameters

    Returns:
        Optional result (stored in Redis for pipelines)
    """
    # Your task logic here
    return "result"
```

### Task Registration Details

When you use `@pool.task()`:

1. A `TaskDefinition` is created with the task name
2. The context type is inferred from the handler's type hints
3. The handler is stored for execution by workers

```python
# The decorator extracts information from your handler:
@pool.task("research")
async def research(agent_id: UUID, context: ResearchContext) -> dict:
    ...

# Equivalent to:
pool.tasks["research"] = TaskDefinition(
    name="research",
    handler=research,
    context_class=ResearchContext,  # Inferred from type hint
)
```

## Starting Workers

### Non-blocking Start

Start workers without blocking the main thread:

```python
pool.start()

# Main process continues...
# Useful for integration with web frameworks

# Later, shutdown workers
pool.shutdown(timeout=60)
```

### Blocking Run

Start workers and block until shutdown:

```python
pool.run()  # Blocks until SIGTERM/SIGINT
```

This is the typical pattern for standalone worker processes.

### Run Modes Comparison

| Method | Behavior | Use Case |
|--------|----------|----------|
| `start()` | Non-blocking, returns immediately | Web apps, custom lifecycle |
| `run()` | Blocking, handles signals | Standalone worker processes |

## Worker Process Lifecycle

Each worker process goes through this lifecycle:

```
1. Spawn (fork from main process)
      │
      ▼
2. Initialize
   • Create process-local DB session
   • Connect to Redis
   • Subscribe to shutdown events
      │
      ▼
3. Main Loop ◄──────────────────────┐
   • BRPOP from queue (blocking)    │
   • Deserialize task               │
   • Execute handler                │
   • Store result (if any)          │
   • Log completion                 │
   └────────────────────────────────┘
      │ (on shutdown signal)
      ▼
4. Cleanup
   • Complete current task
   • Close DB session
   • Close Redis connection
   • Exit process
```

## Worker Configuration

### Number of Workers

Control the number of worker processes:

```bash
# Via environment variable
export AGENTEXEC_NUM_WORKERS=8
```

```python
# Via configuration
print(ax.CONF.num_workers)  # 8
```

**Guidelines:**
- CPU-bound tasks: Set to number of CPU cores
- I/O-bound tasks (most LLM calls): Can exceed CPU cores
- Start with 4-8 and adjust based on monitoring

### Graceful Shutdown Timeout

Time to wait for workers to finish:

```bash
export AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT=300  # 5 minutes
```

```python
# Or override at shutdown
pool.shutdown(timeout=60)  # Override with 60 seconds
```

## Graceful Shutdown

### Signal Handling

The worker pool handles shutdown signals:

- **SIGTERM**: Graceful shutdown (default for `docker stop`, `kill`)
- **SIGINT**: Graceful shutdown (Ctrl+C)

### Shutdown Process

```
1. Main process receives signal
      │
      ▼
2. Broadcast shutdown event (via Redis)
      │
      ▼
3. Workers receive event
   • Finish current task
   • Stop accepting new tasks
   • Close connections
      │
      ▼
4. Main process waits (up to timeout)
      │
      ▼
5. Force kill any remaining workers
      │
      ▼
6. Cancel pending activities
```

### Manual Shutdown

```python
# Graceful shutdown with timeout
pool.shutdown(timeout=60)

# Immediate shutdown (not recommended)
pool.shutdown(timeout=0)
```

## Database Session Management

Each worker maintains its own database session:

```python
# This happens automatically in each worker process:
from agentexec.core.db import set_global_session, get_global_session

# Worker initialization
set_global_session(engine)

# During task execution
session = get_global_session()
# ... use session ...

# Worker cleanup
remove_global_session()
```

### Why Process-Local Sessions?

SQLAlchemy sessions are not thread-safe or process-safe. Each worker process needs its own session to:

- Avoid connection sharing issues
- Enable proper transaction isolation
- Support connection pooling per process

## Log Collection

Worker logs are collected via Redis pub/sub:

```
Worker 0 ──┐
Worker 1 ──┼──> Redis Pub/Sub ──> Main Process ──> stdout
Worker 2 ──┘
```

### Log Format

```
[Worker 0] Processing task: research_company
[Worker 0] Task completed: research_company
[Worker 1] Processing task: analyze_data
[Worker 1] Task failed: ValueError: Invalid input
```

### Custom Log Handling

For advanced use cases, you can customize log handling:

```python
import logging

# Configure root logger before starting workers
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)

pool.run()
```

## Error Handling

### Task Errors

When a task raises an exception:

1. Exception is caught by the worker
2. Activity status is updated to ERROR
3. Error message is logged
4. Worker continues to next task

```python
@pool.task("risky_task")
async def risky_task(agent_id: UUID, context: MyContext):
    # If this raises...
    raise ValueError("Something went wrong")
    # ...activity becomes ERROR, worker continues
```

### Worker Crashes

If a worker process crashes:

- Other workers continue running
- Main process may spawn replacement (depends on implementation)
- Current task may be lost (not automatically retried)

### Handling Stale Activities

On startup, clean up stale activities from previous runs:

```python
from sqlalchemy.orm import Session

with Session(engine) as session:
    canceled = ax.activity.cancel_pending(session)
    print(f"Canceled {canceled} stale activities")
```

## Multiple Worker Pools

Run multiple pools for different workloads:

```python
# High-priority tasks
pool_high = ax.Pool(
    engine=engine,
    database_url=DATABASE_URL,
    queue_name="high_priority",
)

@pool_high.task("urgent_task")
async def urgent_task(agent_id: UUID, context: UrgentContext):
    ...

# Low-priority tasks
pool_low = ax.Pool(
    engine=engine,
    database_url=DATABASE_URL,
    queue_name="low_priority",
)

@pool_low.task("background_task")
async def background_task(agent_id: UUID, context: BackgroundContext):
    ...

# Run in separate processes
# process1: pool_high.run()
# process2: pool_low.run()
```

## Monitoring Workers

### Health Checks

Check if workers are processing tasks:

```python
import asyncio
from agentexec.core.redis_client import get_redis

async def check_queue_depth():
    redis = await get_redis()
    depth = await redis.llen(ax.CONF.queue_name)
    print(f"Queue depth: {depth}")
```

### Metrics

Track worker performance:

```python
from sqlalchemy.orm import Session
from agentexec.activity.models import Activity, Status

def get_metrics(session: Session):
    # Active tasks
    running = session.query(Activity).filter(
        Activity.logs.any(status=Status.RUNNING)
    ).count()

    # Completed in last hour
    from datetime import datetime, timedelta
    hour_ago = datetime.utcnow() - timedelta(hours=1)
    completed = session.query(Activity).filter(
        Activity.updated_at >= hour_ago,
        Activity.logs.any(status=Status.COMPLETE)
    ).count()

    return {"running": running, "completed_last_hour": completed}
```

## Best Practices

### 1. Use Type Hints

Always use type hints for context classes:

```python
# Good - context type is inferred
@pool.task("task")
async def task(agent_id: UUID, context: MyContext):
    ...

# Bad - context type cannot be inferred
@pool.task("task")
async def task(agent_id, context):
    ...
```

### 2. Keep Tasks Focused

Each task should do one thing well:

```python
# Good - focused task
@pool.task("send_email")
async def send_email(agent_id: UUID, context: EmailContext):
    await send(context.to, context.subject, context.body)

# Bad - task does too many things
@pool.task("process_order")
async def process_order(agent_id: UUID, context: OrderContext):
    await validate_order()
    await charge_payment()
    await send_confirmation()
    await update_inventory()
    # If any step fails, everything is lost
```

### 3. Handle Cleanup

Ensure resources are cleaned up:

```python
@pool.task("file_task")
async def file_task(agent_id: UUID, context: FileContext):
    file = None
    try:
        file = open(context.path)
        # Process file...
    finally:
        if file:
            file.close()
```

### 4. Use Timeouts

Prevent tasks from running forever:

```python
import asyncio

@pool.task("api_task")
async def api_task(agent_id: UUID, context: APIContext):
    try:
        result = await asyncio.wait_for(
            call_external_api(),
            timeout=30
        )
        return result
    except asyncio.TimeoutError:
        ax.activity.error(agent_id, "API call timed out")
        raise
```

## Next Steps

- [Task Lifecycle](task-lifecycle.md) - Understand task states
- [Activity Tracking](activity-tracking.md) - Monitor task progress
- [Production Guide](../deployment/production.md) - Production deployment
