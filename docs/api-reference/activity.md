# Activity API Reference

This document covers the activity tracking API for monitoring task execution.

All activity functions are `async` and operate on `AsyncSession` from
`sqlalchemy.ext.asyncio`. The session parameter is optional — when omitted,
functions fall back to `get_session()` from `agentexec.core.db`, which
requires `configure_engine()` to have been called.

## Module: agentexec.activity

```python
import agentexec as ax

# Lifecycle (async)
await ax.activity.create(task_name, message)
await ax.activity.update(agent_id, message)
await ax.activity.complete(agent_id)
await ax.activity.error(agent_id)
await ax.activity.cancel_pending()

# Query (async)
await ax.activity.list()
await ax.activity.detail(agent_id=...)
await ax.activity.count_active()
```

---

## create()

Create a new activity record with an initial `QUEUED` log entry.

```python
async def create(
    task_name: str,
    message: str = "Agent queued",
    agent_id: str | uuid.UUID | None = None,
    metadata: dict[str, Any] | None = None,
) -> uuid.UUID
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task_name` | `str` | required | Name of the task |
| `message` | `str` | `"Agent queued"` | Initial log message |
| `agent_id` | `str \| UUID \| None` | `None` | Pre-generated ID (auto-generated if omitted) |
| `metadata` | `dict \| None` | `None` | Arbitrary metadata attached to the activity |

### Returns

`uuid.UUID` — the activity's `agent_id`.

### Example

```python
import agentexec as ax

agent_id = await ax.activity.create(
    task_name="my_task",
    message="Task created",
    metadata={"organization_id": "org-123"},
)
```

Usually called automatically by `ax.enqueue()`. Call it directly only for
advanced use cases where you need to register an activity outside the normal
enqueue flow.

---

## update()

Append a log entry to an existing activity.

```python
async def update(
    agent_id: str | uuid.UUID,
    message: str,
    percentage: int | None = None,
    status: Status | None = None,
) -> bool
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `agent_id` | `str \| UUID` | required | Activity identifier |
| `message` | `str` | required | Log message |
| `percentage` | `int \| None` | `None` | Progress (0-100) |
| `status` | `Status \| None` | `None` | Status override (defaults to `RUNNING`) |

### Example

```python
# Progress update
await ax.activity.update(agent_id, "Processing data...", percentage=30)

# Explicit status
from agentexec.activity import Status
await ax.activity.update(agent_id, "Starting", status=Status.RUNNING)
```

---

## complete()

Mark an activity as successfully completed.

```python
async def complete(
    agent_id: str | uuid.UUID,
    message: str = "Agent completed",
    percentage: int = 100,
) -> bool
```

### Example

```python
await ax.activity.complete(agent_id)
await ax.activity.complete(agent_id, "Partial completion", percentage=75)
```

---

## error()

Mark an activity as failed.

```python
async def error(
    agent_id: str | uuid.UUID,
    message: str = "Agent failed",
    percentage: int = 100,
) -> bool
```

### Example

```python
try:
    await risky_operation()
except Exception as e:
    await ax.activity.error(agent_id, f"Failed: {e}")
    raise
```

---

## cancel_pending()

Cancel all queued and running activities. Typically called at pool startup
to reconcile state from a previous run.

```python
async def cancel_pending(
    session: AsyncSession | None = None,
) -> int
```

### Returns

`int` — number of activities canceled.

### Example

```python
from agentexec.core.db import get_session

async with get_session() as db:
    canceled = await ax.activity.cancel_pending(session=db)
    print(f"Canceled {canceled} activities")
```

---

## list()

Get a paginated list of activities.

```python
async def list(
    session: AsyncSession | None = None,
    page: int = 1,
    page_size: int = 50,
    metadata_filter: dict[str, Any] | None = None,
) -> ActivityListSchema
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `session` | `AsyncSession \| None` | `None` | Session (falls back to `get_session()`) |
| `page` | `int` | `1` | Page number (1-indexed) |
| `page_size` | `int` | `50` | Items per page |
| `metadata_filter` | `dict \| None` | `None` | Filter by metadata fields |

### Example

```python
result = await ax.activity.list(page=1, page_size=20)

print(f"Total: {result.total}")
print(f"Page {result.page}, total_pages: {result.total_pages}")

for item in result.items:
    print(f"{item.agent_id}: {item.status}")
```

### With Metadata Filter

```python
# Only activities for org-123
result = await ax.activity.list(
    metadata_filter={"organization_id": "org-123"},
)
```

---

## detail()

Get detailed activity with full log history.

```python
async def detail(
    session: AsyncSession | None = None,
    agent_id: str | uuid.UUID | None = None,
    metadata_filter: dict[str, Any] | None = None,
) -> ActivityDetailSchema | None
```

### Returns

`ActivityDetailSchema | None` — activity details, or `None` if not found (or
the metadata filter didn't match).

### Example

```python
activity = await ax.activity.detail(agent_id=agent_id)

if activity:
    print(f"Task: {activity.agent_type}")
    for log in activity.logs:
        print(f"  [{log.created_at}] {log.status} {log.message}")
```

---

## count_active()

Return the number of queued or running activities.

```python
async def count_active(
    session: AsyncSession | None = None,
) -> int
```

---

## Status Enum

```python
class Status(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"
    CANCELED = "CANCELED"
```

| Status | Description |
|--------|-------------|
| `QUEUED` | Task is waiting in queue |
| `RUNNING` | Task is being executed |
| `COMPLETE` | Task finished successfully |
| `ERROR` | Task failed with error |
| `CANCELED` | Task was canceled |

---

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

---

## ORM Models

For advanced queries, use the ORM models directly with an `AsyncSession`.

### Activity

```python
class Activity(Base):
    __tablename__ = "{prefix}activity"

    id: Mapped[UUID]
    agent_id: Mapped[UUID]       # Unique, indexed
    agent_type: Mapped[str | None]
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]
    metadata_: Mapped[dict | None]  # Column is "metadata"
    logs: Mapped[list[ActivityLog]]
```

#### Classmethods (all async)

- `Activity.create(session, agent_id, task_name, message, metadata=None)`
- `Activity.append_log(session, agent_id, message, status, percentage=None)`
- `Activity.get_by_agent_id(session, agent_id, metadata_filter=None)`
- `Activity.get_list(session, page=1, page_size=50, metadata_filter=None)`
- `Activity.get_pending_ids(session)`
- `Activity.get_active_count(session)`

### ActivityLog

```python
class ActivityLog(Base):
    __tablename__ = "{prefix}activity_log"

    id: Mapped[UUID]
    activity_id: Mapped[UUID]
    message: Mapped[str]
    status: Mapped[Status]
    percentage: Mapped[int | None]
    created_at: Mapped[datetime]
```

---

## Direct ORM Usage

```python
from sqlalchemy import select
from agentexec.activity.models import Activity, ActivityLog, Status
from agentexec.core.db import get_session

async with get_session() as db:
    # Latest log for a given agent
    result = await db.execute(
        select(ActivityLog)
        .where(ActivityLog.activity_id == activity_id)
        .order_by(ActivityLog.created_at.desc())
        .limit(1)
    )
    latest = result.scalar_one_or_none()
```
