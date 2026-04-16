# Activity Tracking

Activity tracking provides observability into your AI agents. It records
the full lifecycle of each task execution, including status changes,
progress updates, and log messages.

All activity APIs are async. Pass an `AsyncSession` explicitly, or let the
function fall back to `get_session()` (which requires `configure_engine()`
to have been called — the Pool handles this automatically).

## Overview

Every task creates an **Activity** record. As the task executes, log entries
are appended:

```
Activity (task instance)
├── id: UUID
├── agent_id: UUID (for tracking)
├── agent_type: "research_company"
├── metadata: {...}
├── created_at: 2024-01-15 10:00:00
├── updated_at: 2024-01-15 10:05:00
│
└── ActivityLog entries:
    ├── [10:00:00] QUEUED    "Agent queued"
    ├── [10:00:05] RUNNING   "Task started."
    ├── [10:01:00] RUNNING   "Researching company profile..." (25%)
    ├── [10:03:00] RUNNING   "Analyzing competitors..." (50%)
    ├── [10:04:30] RUNNING   "Generating report..." (75%)
    └── [10:05:00] COMPLETE  "Task completed successfully." (100%)
```

## Status Lifecycle

```
QUEUED ──> RUNNING ──> COMPLETE
                  └──> ERROR
                  └──> CANCELED (cleanup/shutdown)
```

| Status | Description |
|--------|-------------|
| `QUEUED` | Task is waiting in the queue |
| `RUNNING` | Worker is executing the task |
| `COMPLETE` | Task finished successfully |
| `ERROR` | Task failed with an exception |
| `CANCELED` | Task was canceled (shutdown/cleanup) |

## Using the Activity API

### Creating Activities

Activities are created automatically by `ax.enqueue()`:

```python
task = await ax.enqueue("research", ResearchContext(company="Acme"))
# Activity created with status=QUEUED
```

For manual creation (advanced use cases):

```python
agent_id = await ax.activity.create(
    task_name="custom_task",
    message="Starting custom operation",
    metadata={"organization_id": "org-123"},
)
```

### Updating Progress

Report progress during task execution:

```python
@pool.task("long_task")
async def long_task(agent_id: UUID, context: MyContext):
    await ax.activity.update(agent_id, "Starting phase 1")
    await phase_1()

    await ax.activity.update(
        agent_id,
        "Phase 1 complete, starting phase 2",
        percentage=33,
    )
    await phase_2()

    await ax.activity.update(
        agent_id,
        "Phase 2 complete, starting phase 3",
        percentage=66,
    )
    await phase_3()
    # Final status (COMPLETE) is set automatically when the handler returns.
```

### Completing Tasks

Task completion is tracked automatically. If you need to mark a task
complete manually (e.g. partial completion):

```python
await ax.activity.complete(
    agent_id,
    message="All processing finished",
    percentage=100,
)
```

### Recording Errors

Uncaught exceptions automatically mark the activity as `ERROR`. Call
`activity.error` yourself when you want a specific error message recorded:

```python
try:
    await risky_operation()
except APIError as e:
    await ax.activity.error(agent_id, f"API failed: {e.message}")
    raise
```

### Canceling Pending Tasks

Reconcile stale activities (e.g. after a crash or restart):

```python
canceled = await ax.activity.cancel_pending()
print(f"Canceled {canceled} pending tasks")
```

## Querying Activities

### Get Activity Detail

```python
activity = await ax.activity.detail(agent_id=agent_id)

if activity:
    print(f"Task: {activity.agent_type}")
    print(f"Created: {activity.created_at}")
    print(f"Updated: {activity.updated_at}")
    for log in activity.logs:
        pct = f" ({log.percentage}%)" if log.percentage is not None else ""
        print(f"  [{log.created_at}] {log.status} {log.message}{pct}")
```

### List Activities

```python
result = await ax.activity.list(page=1, page_size=20)

print(f"Total: {result.total}")
print(f"Page {result.page}, total_pages: {result.total_pages}")

for item in result.items:
    print(f"{item.agent_id}: {item.agent_type} - {item.status}")
    if item.latest_log_message:
        print(f"  Latest: {item.latest_log_message}")
```

### Filter by Metadata

```python
result = await ax.activity.list(
    metadata_filter={"organization_id": "org-123"},
)
```

### Query Directly with the ORM

For complex queries, use `AsyncSession` directly:

```python
from sqlalchemy import select
from agentexec.activity.models import Activity, ActivityLog, Status
from agentexec.core.db import get_session

async with get_session() as db:
    # Activities with latest status = ERROR
    stmt = (
        select(Activity)
        .join(ActivityLog, ActivityLog.activity_id == Activity.id)
        .where(ActivityLog.status == Status.ERROR)
        .order_by(Activity.updated_at.desc())
    )
    result = await db.execute(stmt)
    errors = result.scalars().all()
```

## Response Schemas

### ActivityLogSchema

```python
class ActivityLogSchema(BaseModel):
    id: uuid.UUID
    message: str
    status: Status
    percentage: int | None = 0
    created_at: datetime
```

### ActivityDetailSchema

Returned by `activity.detail()`:

```python
class ActivityDetailSchema(BaseModel):
    id: uuid.UUID | None
    agent_id: uuid.UUID
    agent_type: str
    created_at: datetime
    updated_at: datetime
    logs: list[ActivityLogSchema]
```

### ActivityListItemSchema

```python
class ActivityListItemSchema(BaseModel):
    agent_id: uuid.UUID
    agent_type: str
    status: Status
    latest_log_message: str | None
    latest_log_timestamp: datetime | None
    percentage: int | None
    started_at: datetime | None
    elapsed_time_seconds: int  # computed
```

### ActivityListSchema

```python
class ActivityListSchema(BaseModel):
    items: list[ActivityListItemSchema]
    total: int
    page: int
    page_size: int
    total_pages: int  # computed
```

## Database Models

### Activity Table

```sql
CREATE TABLE agentexec_activity (
    id UUID PRIMARY KEY,
    agent_id UUID UNIQUE NOT NULL,
    agent_type VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    metadata JSON
);
CREATE INDEX ix_agentexec_activity_agent_id ON agentexec_activity(agent_id);
```

### ActivityLog Table

```sql
CREATE TABLE agentexec_activity_log (
    id UUID PRIMARY KEY,
    activity_id UUID REFERENCES agentexec_activity(id),
    message TEXT NOT NULL,
    status VARCHAR NOT NULL,
    percentage INTEGER,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX ix_agentexec_activity_log_activity_id
    ON agentexec_activity_log(activity_id);
```

### Table Prefix

Tables are prefixed with `CONF.table_prefix` (default: `agentexec_`):

```bash
AGENTEXEC_TABLE_PREFIX=myapp_
# Creates: myapp_activity, myapp_activity_log
```

## Agent Self-Reporting

### Built-in Reporting Tool

The OpenAI runner provides a tool for agents to report progress:

```python
runner = ax.OpenAIRunner(agent_id=agent_id)

agent = Agent(
    name="Research Agent",
    instructions=f"""
    You are a research agent.
    {runner.prompts.report_status}
    """,
    tools=[runner.tools.report_status],
    model="gpt-4o",
)
```

When the agent calls `report_status`, the runner writes an
`ax.activity.update()` with the message and percentage.

### Custom Reporting Prompts

```python
runner = ax.OpenAIRunner(
    agent_id=agent_id,
    report_status_prompt="""
    Use the report_activity tool to update progress:
    - Call it after completing each major step.
    - Include specific details about what you found.
    - Estimate percentage complete (0-100).
    """,
)
```

## Custom Activity Messages

Customize default messages via environment variables:

```bash
AGENTEXEC_ACTIVITY_MESSAGE_STARTED="Agent is working..."
AGENTEXEC_ACTIVITY_MESSAGE_COMPLETE="Successfully finished"
AGENTEXEC_ACTIVITY_MESSAGE_ERROR="Failed: {error}"
```

The `{error}` placeholder in the error message is replaced with the raised
exception's message.

## Building a Status API

Example FastAPI endpoints (all async):

```python
from fastapi import FastAPI, HTTPException
import agentexec as ax

app = FastAPI()

@app.get("/api/activities")
async def list_activities(page: int = 1, page_size: int = 20):
    return await ax.activity.list(page=page, page_size=page_size)

@app.get("/api/activities/{agent_id}")
async def get_activity(agent_id: str):
    activity = await ax.activity.detail(agent_id=agent_id)
    if not activity:
        raise HTTPException(404, "Activity not found")
    return activity

@app.get("/api/activities/{agent_id}/status")
async def get_status(agent_id: str):
    activity = await ax.activity.detail(agent_id=agent_id)
    if not activity:
        raise HTTPException(404, "Activity not found")
    latest = activity.logs[-1] if activity.logs else None
    return {
        "status": latest.status if latest else None,
        "progress": latest.percentage if latest else None,
        "message": latest.message if latest else None,
    }
```

## Best Practices

### 1. Report Progress Frequently

```python
@pool.task("long_task")
async def long_task(agent_id: UUID, context: MyContext):
    items = await get_items()
    for i, item in enumerate(items):
        await process_item(item)
        await ax.activity.update(
            agent_id,
            f"Processed {i+1}/{len(items)} items",
            percentage=int((i + 1) / len(items) * 100),
        )
```

### 2. Include Meaningful Messages

```python
# Good
await ax.activity.update(
    agent_id,
    f"Found {len(results)} matching documents, analyzing...",
    percentage=50,
)

# Bad
await ax.activity.update(agent_id, "Working...", percentage=50)
```

### 3. Clean Up on Startup

```python
# In your startup code
canceled = await ax.activity.cancel_pending()
if canceled:
    print(f"Cleaned up {canceled} stale activities from previous run")
```

### 4. Archive Old Activities

For high-volume systems:

```python
from datetime import datetime, timedelta, timezone
from sqlalchemy import delete
from agentexec.activity.models import Activity
from agentexec.core.db import get_session

async def archive_old_activities(days: int = 30) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    async with get_session() as db:
        result = await db.execute(
            delete(Activity).where(Activity.updated_at < cutoff)
        )
        await db.commit()
        return result.rowcount
```

## Next Steps

- [Task Lifecycle](task-lifecycle.md) — Understand task states
- [OpenAI Runner](../guides/openai-runner.md) — Agent self-reporting
- [FastAPI Integration](../guides/fastapi-integration.md) — Build status APIs
