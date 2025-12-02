# Task Lifecycle

Understanding the task lifecycle is essential for building reliable applications with agentexec. This document explains how tasks move through different states and how to handle each stage.

## Lifecycle States

Every task goes through a defined lifecycle tracked by the activity system:

```
                     ┌──────────┐
                     │  QUEUED  │
                     └────┬─────┘
                          │
                    Task dequeued
                    by worker
                          │
                          ▼
                     ┌──────────┐
            ┌────────│ RUNNING  │────────┐
            │        └──────────┘        │
            │                            │
      Success                       Error/Exception
            │                            │
            ▼                            ▼
      ┌──────────┐                ┌──────────┐
      │ COMPLETE │                │  ERROR   │
      └──────────┘                └──────────┘


                     ┌──────────┐
                     │ CANCELED │  (shutdown/cleanup)
                     └──────────┘
```

### QUEUED

The initial state when a task is created.

- Activity record is created in the database
- Task is serialized and pushed to Redis queue
- Waiting for a worker to pick it up

```python
task = await ax.enqueue("my_task", MyContext(data="value"))
# Task is now QUEUED
```

### RUNNING

A worker has dequeued the task and is executing it.

- Worker updates activity status to RUNNING
- Task handler function is executing
- Agent may report progress during this phase

```python
@pool.task("my_task")
async def my_task(agent_id: UUID, context: MyContext):
    # Activity is RUNNING while this executes
    result = await do_work()
    return result
```

### COMPLETE

The task finished successfully.

- Handler returned without raising an exception
- Activity status updated to COMPLETE
- Result (if any) is stored in Redis for pipeline coordination

### ERROR

The task failed with an exception.

- Handler raised an exception
- Activity status updated to ERROR
- Error message is logged
- Worker continues to next task

### CANCELED

The task was canceled, typically during shutdown.

- Used when workers are shutting down
- `activity.cancel_pending()` marks stale QUEUED/RUNNING tasks as CANCELED
- Prevents misleading RUNNING status for abandoned tasks

## Task Definition

Tasks are defined using the `@pool.task()` decorator:

```python
from uuid import UUID
from pydantic import BaseModel
import agentexec as ax

class MyContext(BaseModel):
    """Type-safe context passed to the task."""
    company: str
    priority: int = 1

pool = ax.WorkerPool(engine=engine, database_url=DATABASE_URL)

@pool.task("research_company")
async def research_company(agent_id: UUID, context: MyContext) -> str:
    """
    Task handler function.

    Args:
        agent_id: Unique identifier for this task instance
        context: Typed context with task parameters

    Returns:
        Result to store (optional, used for pipelines)
    """
    # Your task logic here
    return "Research complete"
```

### Handler Signature

Task handlers must follow this signature:

```python
async def handler(agent_id: UUID, context: ContextType) -> Optional[Any]:
```

- `agent_id`: UUID identifying this specific task execution
- `context`: Pydantic model instance with task parameters
- Return value: Optional result stored for pipeline coordination

### Context Types

Contexts are Pydantic models that provide:

- **Type safety**: Validation at enqueue and dequeue time
- **IDE support**: Autocomplete and type hints
- **Documentation**: Self-documenting task parameters

```python
class ResearchContext(BaseModel):
    company: str
    focus_areas: list[str] = []
    max_sources: int = 10

    class Config:
        # Optional: add validation
        str_min_length = 1
```

## Enqueueing Tasks

### Basic Enqueue

```python
from agentexec import enqueue, Priority

# Default priority (LOW)
task = await enqueue("my_task", MyContext(data="value"))

# High priority (processed first)
task = await enqueue("my_task", MyContext(data="urgent"), priority=Priority.HIGH)
```

### Priority Levels

| Priority | Behavior | Use Case |
|----------|----------|----------|
| `Priority.LOW` | Added to back of queue (LPUSH) | Default, normal tasks |
| `Priority.HIGH` | Added to front of queue (RPUSH) | Urgent tasks, user-facing requests |

### Custom Queue

```python
# Enqueue to a specific queue
task = await enqueue("my_task", context, queue_name="high_priority_queue")
```

### Task Object

The `enqueue()` function returns a `Task` object:

```python
task = await enqueue("my_task", context)

print(task.agent_id)    # UUID for tracking
print(task.task_name)   # "my_task"
print(task.context)     # MyContext instance
```

## Task Execution

### Execution Flow

When a worker picks up a task:

1. **Dequeue**: `BRPOP` removes task from Redis queue
2. **Deserialize**: JSON is parsed, context is validated
3. **Status Update**: Activity marked as RUNNING
4. **Execute**: Handler function is called
5. **Result Storage**: Return value stored in Redis (if not None)
6. **Status Update**: Activity marked as COMPLETE or ERROR

### Automatic Lifecycle Management

The `Task.execute()` method handles lifecycle automatically:

```python
# This is what happens inside the worker:
await task.execute()
# - Updates status to RUNNING before handler
# - Calls your handler function
# - Updates status to COMPLETE/ERROR after
# - Stores result if returned
```

### Manual Lifecycle (Advanced)

For custom runners, you can manage lifecycle manually:

```python
@pool.task("custom_task")
async def custom_task(agent_id: UUID, context: MyContext):
    # Manual status updates
    ax.activity.update(agent_id, "Starting phase 1", completion_percentage=0)

    await phase_1()
    ax.activity.update(agent_id, "Phase 1 complete", completion_percentage=33)

    await phase_2()
    ax.activity.update(agent_id, "Phase 2 complete", completion_percentage=66)

    await phase_3()
    # Final status is set automatically
```

## Error Handling

### Automatic Error Capture

Exceptions in handlers are automatically caught and logged:

```python
@pool.task("risky_task")
async def risky_task(agent_id: UUID, context: MyContext):
    # If this raises, activity status becomes ERROR
    # Error message is logged
    raise ValueError("Something went wrong")
```

### Custom Error Messages

Use try/except for custom error handling:

```python
@pool.task("api_task")
async def api_task(agent_id: UUID, context: MyContext):
    try:
        result = await external_api_call()
        return result
    except APIError as e:
        ax.activity.error(agent_id, f"API failed: {e.message}")
        raise  # Re-raise to mark task as ERROR
```

### Retry Logic

agentexec doesn't have built-in retry. Implement retries in your handler:

```python
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential

@pool.task("retry_task")
async def retry_task(agent_id: UUID, context: MyContext):
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    async def do_work():
        return await unreliable_operation()

    try:
        return await do_work()
    except Exception as e:
        ax.activity.error(agent_id, f"Failed after 3 retries: {e}")
        raise
```

## Results and Pipelines

### Storing Results

Return values are automatically stored in Redis:

```python
@pool.task("analyze")
async def analyze(agent_id: UUID, context: AnalyzeContext) -> dict:
    result = await perform_analysis()
    return {"score": 0.85, "summary": "..."}  # Stored in Redis
```

### Retrieving Results

Use `get_result()` to retrieve stored results:

```python
# Wait for result (blocks up to timeout)
result = await ax.get_result(task.agent_id, timeout=300)
print(result["score"])  # 0.85
```

### Pipeline Coordination

Results enable multi-step pipelines:

```python
@pipeline.step(0)
async def step1(self, ctx: InputContext):
    task1 = await ax.enqueue("process_a", ctx)
    task2 = await ax.enqueue("process_b", ctx)
    return await ax.gather(task1, task2)

@pipeline.step(1)
async def step2(self, result_a, result_b):
    # Use results from previous step
    combined = await ax.enqueue("combine", CombineContext(a=result_a, b=result_b))
    return await ax.get_result(combined.agent_id)
```

## Monitoring Tasks

### Query Single Task

```python
from sqlalchemy.orm import Session

with Session(engine) as session:
    activity = ax.activity.detail(session, agent_id)

    print(f"Status: {activity.status}")
    print(f"Progress: {activity.latest_completion_percentage}%")

    for log in activity.logs:
        print(f"[{log.created_at}] {log.message}")
```

### List Tasks

```python
with Session(engine) as session:
    activities = ax.activity.list(session, page=1, page_size=20)

    print(f"Total: {activities.total}")
    for item in activities.items:
        print(f"{item.agent_id}: {item.status}")
```

### Query by Status

```python
from agentexec.activity.models import Activity, Status

with Session(engine) as session:
    running = session.query(Activity).filter(
        Activity.logs.any(status=Status.RUNNING)
    ).all()
```

## Best Practices

### 1. Keep Contexts Small

Don't embed large data in contexts. Use references instead:

```python
# Bad: Large data in context
class BadContext(BaseModel):
    file_contents: str  # Could be megabytes

# Good: Reference to data
class GoodContext(BaseModel):
    file_id: str  # Fetch from storage in handler
```

### 2. Idempotent Handlers

Design handlers to be safely re-runnable:

```python
@pool.task("process_order")
async def process_order(agent_id: UUID, context: OrderContext):
    # Check if already processed
    if await is_order_processed(context.order_id):
        return {"status": "already_processed"}

    # Process and mark as done atomically
    await process_and_mark(context.order_id)
```

### 3. Progress Reporting

Report progress for long-running tasks:

```python
@pool.task("long_task")
async def long_task(agent_id: UUID, context: MyContext):
    items = await get_items()

    for i, item in enumerate(items):
        await process_item(item)
        progress = int((i + 1) / len(items) * 100)
        ax.activity.update(agent_id, f"Processed {i+1}/{len(items)}", progress)
```

### 4. Graceful Timeout Handling

Handle timeouts gracefully in handlers:

```python
import asyncio

@pool.task("timed_task")
async def timed_task(agent_id: UUID, context: MyContext):
    try:
        result = await asyncio.wait_for(slow_operation(), timeout=60)
        return result
    except asyncio.TimeoutError:
        ax.activity.update(agent_id, "Operation timed out, using fallback")
        return await fallback_operation()
```

## Next Steps

- [Worker Pool](worker-pool.md) - How workers execute tasks
- [Activity Tracking](activity-tracking.md) - Monitoring and logging
- [Pipelines Guide](../guides/pipelines.md) - Multi-step workflows
