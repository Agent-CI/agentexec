# Activity Tracking

Activity tracking provides observability into your AI agents. It records the full lifecycle of each task execution, including status changes, progress updates, and log messages.

## Overview

Every task creates an **Activity** record in your database. As the task executes, **ActivityLog** entries are appended to track progress:

```
Activity (task instance)
├── id: UUID
├── agent_id: UUID (for tracking)
├── agent_type: "research_company"
├── created_at: 2024-01-15 10:00:00
├── updated_at: 2024-01-15 10:05:00
│
└── ActivityLog entries:
    ├── [10:00:00] QUEUED - "Waiting to start."
    ├── [10:00:05] RUNNING - "Task started."
    ├── [10:01:00] RUNNING - "Researching company profile..." (25%)
    ├── [10:03:00] RUNNING - "Analyzing competitors..." (50%)
    ├── [10:04:30] RUNNING - "Generating report..." (75%)
    └── [10:05:00] COMPLETE - "Task completed successfully." (100%)
```

## Status Lifecycle

Activities progress through these statuses:

```
QUEUED ──> RUNNING ──> COMPLETE
                  └──> ERROR

CANCELED (cleanup/shutdown)
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

Activities are created automatically when you enqueue a task:

```python
task = await ax.enqueue("research", ResearchContext(company="Acme"))
# Activity is created with status=QUEUED
```

For manual creation (advanced use cases):

```python
from sqlalchemy.orm import Session

with Session(engine) as session:
    agent_id = ax.activity.create(
        task_name="custom_task",
        message="Starting custom operation",
        agent_id=None,  # Auto-generated if None
        session=session
    )
```

### Updating Progress

Report progress during task execution:

```python
@pool.task("long_task")
async def long_task(agent_id: UUID, context: MyContext):
    # Update with message only
    ax.activity.update(agent_id, "Starting phase 1")

    await phase_1()

    # Update with progress percentage
    ax.activity.update(
        agent_id,
        "Phase 1 complete, starting phase 2",
        completion_percentage=33
    )

    await phase_2()

    ax.activity.update(
        agent_id,
        "Phase 2 complete, starting phase 3",
        completion_percentage=66
    )

    await phase_3()

    # Final status is set automatically by the runner
```

### Completing Tasks

Mark a task as complete:

```python
# Automatic completion (via runner or task.execute())
result = await runner.run(agent, input="...")
# Activity is marked COMPLETE automatically

# Manual completion
ax.activity.complete(
    agent_id,
    message="All processing finished",
    completion_percentage=100
)
```

### Recording Errors

Mark a task as failed:

```python
try:
    await risky_operation()
except Exception as e:
    ax.activity.error(agent_id, f"Operation failed: {e}")
    raise  # Re-raise to trigger automatic ERROR status
```

### Canceling Pending Tasks

Clean up stale tasks (e.g., after restart):

```python
with Session(engine) as session:
    canceled_count = ax.activity.cancel_pending(session)
    print(f"Canceled {canceled_count} pending tasks")
```

## Querying Activities

### Get Activity Detail

Retrieve full activity with all logs:

```python
from sqlalchemy.orm import Session

with Session(engine) as session:
    activity = ax.activity.detail(session, agent_id)

    if activity:
        print(f"Task: {activity.agent_type}")
        print(f"Status: {activity.status}")
        print(f"Created: {activity.created_at}")
        print(f"Updated: {activity.updated_at}")
        print(f"Progress: {activity.latest_completion_percentage}%")

        print("\nLog history:")
        for log in activity.logs:
            print(f"  [{log.created_at}] [{log.status}] {log.message}")
```

### List Activities

Get a paginated list of activities:

```python
with Session(engine) as session:
    result = ax.activity.list(
        session,
        page=1,
        page_size=20
    )

    print(f"Total activities: {result.total}")
    print(f"Page {result.page} of {result.pages}")

    for item in result.items:
        print(f"{item.agent_id}: {item.agent_type} - {item.status}")
        if item.latest_log:
            print(f"  Latest: {item.latest_log}")
```

### Query by Status

Use SQLAlchemy to filter activities:

```python
from agentexec.activity.models import Activity, ActivityLog, Status

with Session(engine) as session:
    # Get running activities
    running = session.query(Activity).filter(
        Activity.logs.any(status=Status.RUNNING)
    ).all()

    # Get recently completed
    from datetime import datetime, timedelta
    hour_ago = datetime.utcnow() - timedelta(hours=1)

    recent = session.query(Activity).filter(
        Activity.updated_at >= hour_ago
    ).order_by(Activity.updated_at.desc()).all()

    # Get error activities
    errors = session.query(Activity).filter(
        Activity.logs.any(status=Status.ERROR)
    ).all()
```

## Response Schemas

### ActivityDetailSchema

Returned by `activity.detail()`:

```python
class ActivityDetailSchema(BaseModel):
    agent_id: UUID
    agent_type: str                    # Task name
    created_at: datetime
    updated_at: datetime
    status: Status                     # Current status
    latest_completion_percentage: int | None
    logs: list[ActivityLogSchema]      # All log entries
```

### ActivityListSchema

Returned by `activity.list()`:

```python
class ActivityListSchema(BaseModel):
    items: list[ActivityListItemSchema]
    total: int
    page: int
    page_size: int
    pages: int
```

### ActivityListItemSchema

Individual items in the list:

```python
class ActivityListItemSchema(BaseModel):
    agent_id: UUID
    agent_type: str
    created_at: datetime
    updated_at: datetime
    status: Status
    latest_log: str | None              # Most recent log message
    latest_completion_percentage: int | None
```

### ActivityLogSchema

Individual log entries:

```python
class ActivityLogSchema(BaseModel):
    id: int
    message: str
    status: Status
    completion_percentage: int | None
    created_at: datetime
```

## Database Models

### Activity Table

```sql
CREATE TABLE agentexec_activity (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID UNIQUE NOT NULL,
    agent_type VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX ix_agentexec_activity_agent_id ON agentexec_activity(agent_id);
```

### ActivityLog Table

```sql
CREATE TABLE agentexec_activity_log (
    id SERIAL PRIMARY KEY,
    activity_id UUID REFERENCES agentexec_activity(id),
    message TEXT NOT NULL,
    status VARCHAR NOT NULL,  -- QUEUED, RUNNING, COMPLETE, ERROR, CANCELED
    completion_percentage INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX ix_agentexec_activity_log_activity_id ON agentexec_activity_log(activity_id);
```

### Table Prefix

Tables are prefixed with `CONF.table_prefix` (default: `agentexec_`):

```bash
AGENTEXEC_TABLE_PREFIX=myapp_
# Creates: myapp_activity, myapp_activity_log
```

## Agent Self-Reporting

### Built-in Reporting Tool

The OpenAI runner provides a tool for agents to report their progress:

```python
runner = ax.OpenAIRunner(agent_id=agent_id)

agent = Agent(
    name="Research Agent",
    instructions=f"""
    You are a research agent.
    {runner.prompts.report_status}
    """,
    tools=[runner.tools.report_status],  # Add the reporting tool
    model="gpt-4o",
)
```

### How It Works

1. The `report_status` tool is added to the agent
2. The agent receives instructions to use it
3. When the agent calls the tool, it updates the activity
4. Progress is immediately visible via the API

Example agent tool call:
```json
{
    "name": "report_activity",
    "arguments": {
        "message": "Found 5 relevant articles",
        "percentage": 50
    }
}
```

### Custom Reporting Prompts

Customize the instructions given to agents:

```python
runner = ax.OpenAIRunner(
    agent_id=agent_id,
    report_status_prompt="""
    Use the report_activity tool to update progress:
    - Call it after completing each major step
    - Include specific details about what you found
    - Estimate percentage complete (0-100)
    """
)
```

## Custom Activity Messages

Customize default messages via environment variables:

```bash
AGENTEXEC_ACTIVITY_MESSAGE_CREATE="Queued for processing"
AGENTEXEC_ACTIVITY_MESSAGE_STARTED="Agent is working..."
AGENTEXEC_ACTIVITY_MESSAGE_COMPLETE="Successfully finished"
AGENTEXEC_ACTIVITY_MESSAGE_ERROR="Failed: {error}"
```

The `{error}` placeholder in the error message is replaced with the actual error message.

## Building a Status API

Example FastAPI endpoints for activity tracking:

```python
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
import agentexec as ax

app = FastAPI()

def get_db():
    with Session(engine) as session:
        yield session

@app.get("/api/activities")
def list_activities(
    page: int = 1,
    page_size: int = 20,
    db: Session = Depends(get_db)
):
    """List all activities with pagination."""
    return ax.activity.list(db, page=page, page_size=page_size)

@app.get("/api/activities/{agent_id}")
def get_activity(agent_id: str, db: Session = Depends(get_db)):
    """Get detailed activity with full log history."""
    activity = ax.activity.detail(db, agent_id)
    if not activity:
        raise HTTPException(404, "Activity not found")
    return activity

@app.get("/api/activities/{agent_id}/status")
def get_status(agent_id: str, db: Session = Depends(get_db)):
    """Get just the current status and progress."""
    activity = ax.activity.detail(db, agent_id)
    if not activity:
        raise HTTPException(404, "Activity not found")
    return {
        "status": activity.status,
        "progress": activity.latest_completion_percentage,
        "message": activity.logs[-1].message if activity.logs else None
    }
```

## Best Practices

### 1. Report Progress Frequently

For long-running tasks, report progress often:

```python
@pool.task("long_task")
async def long_task(agent_id: UUID, context: MyContext):
    items = await get_items()

    for i, item in enumerate(items):
        await process_item(item)

        # Report every item or batch
        progress = int((i + 1) / len(items) * 100)
        ax.activity.update(
            agent_id,
            f"Processed {i+1}/{len(items)} items",
            completion_percentage=progress
        )
```

### 2. Include Meaningful Messages

Make messages informative:

```python
# Good - descriptive message
ax.activity.update(
    agent_id,
    f"Found {len(results)} matching documents, analyzing...",
    completion_percentage=50
)

# Bad - vague message
ax.activity.update(agent_id, "Working...", completion_percentage=50)
```

### 3. Handle Errors Gracefully

Provide useful error messages:

```python
try:
    result = await api_call()
except APIError as e:
    ax.activity.error(
        agent_id,
        f"API call failed: {e.message} (code: {e.code})"
    )
    raise
except TimeoutError:
    ax.activity.error(
        agent_id,
        "API call timed out after 30 seconds"
    )
    raise
```

### 4. Clean Up on Startup

Cancel stale activities when your application starts:

```python
# In your startup code
with Session(engine) as session:
    canceled = ax.activity.cancel_pending(session)
    if canceled > 0:
        print(f"Cleaned up {canceled} stale activities from previous run")
```

### 5. Archive Old Activities

For high-volume systems, archive old activities:

```python
from datetime import datetime, timedelta

def archive_old_activities(session: Session, days: int = 30):
    cutoff = datetime.utcnow() - timedelta(days=days)

    old_activities = session.query(Activity).filter(
        Activity.updated_at < cutoff
    ).all()

    for activity in old_activities:
        # Move to archive table or delete
        session.delete(activity)

    session.commit()
```

## Next Steps

- [Task Lifecycle](task-lifecycle.md) - Understand task states
- [OpenAI Runner](../guides/openai-runner.md) - Agent self-reporting
- [FastAPI Integration](../guides/fastapi-integration.md) - Build status APIs
