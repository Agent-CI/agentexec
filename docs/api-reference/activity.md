# Activity API Reference

This document covers the activity tracking API for monitoring task execution.

## Module: agentexec.activity

Access via the `ax.activity` namespace:

```python
import agentexec as ax

ax.activity.create()
ax.activity.update()
ax.activity.complete()
ax.activity.error()
ax.activity.cancel_pending()
ax.activity.list()
ax.activity.detail()
```

---

## create()

Create a new activity record.

```python
def create(
    task_name: str,
    message: str,
    agent_id: UUID | None = None,
    session: Session | None = None,
) -> UUID
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task_name` | `str` | required | Name of the task |
| `message` | `str` | required | Initial log message |
| `agent_id` | `UUID \| None` | `None` | Custom UUID (auto-generated if None) |
| `session` | `Session \| None` | `None` | SQLAlchemy session (uses global if None) |

### Returns

`UUID` - The activity's agent_id.

### Example

```python
from sqlalchemy.orm import Session
import agentexec as ax

with Session(engine) as session:
    agent_id = ax.activity.create(
        task_name="my_task",
        message="Task created",
        session=session
    )
```

> **Note**: Usually called automatically by `enqueue()`. Manual creation is for advanced use cases.

---

## update()

Add a log entry to an activity.

```python
def update(
    agent_id: str | UUID,
    message: str,
    percentage: int | None = None,
    status: Status | None = None,
) -> None
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `agent_id` | `str \| UUID` | required | Activity identifier |
| `message` | `str` | required | Log message |
| `percentage` | `int \| None` | `None` | Progress (0-100) |
| `status` | `Status \| None` | `None` | New status (keeps current if None) |

### Example

```python
# Update with message only
ax.activity.update(agent_id, "Processing data...")

# Update with progress
ax.activity.update(agent_id, "50% complete", percentage=50)

# Update with status change
from agentexec.activity.models import Status
ax.activity.update(agent_id, "Starting", status=Status.RUNNING)
```

---

## complete()

Mark an activity as successfully completed.

```python
def complete(
    agent_id: str | UUID,
    message: str,
    percentage: int = 100,
) -> None
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `agent_id` | `str \| UUID` | required | Activity identifier |
| `message` | `str` | required | Completion message |
| `percentage` | `int` | `100` | Final progress |

### Example

```python
ax.activity.complete(agent_id, "Task finished successfully")

# With custom percentage
ax.activity.complete(agent_id, "Partial completion", percentage=75)
```

---

## error()

Mark an activity as failed.

```python
def error(
    agent_id: str | UUID,
    message: str,
) -> None
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `agent_id` | `str \| UUID` | required | Activity identifier |
| `message` | `str` | required | Error message |

### Example

```python
try:
    await risky_operation()
except Exception as e:
    ax.activity.error(agent_id, f"Failed: {e}")
    raise
```

---

## cancel_pending()

Cancel all pending activities (QUEUED or RUNNING).

```python
def cancel_pending(
    session: Session | None = None,
) -> int
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `session` | `Session \| None` | `None` | SQLAlchemy session |

### Returns

`int` - Number of activities canceled.

### Example

```python
from sqlalchemy.orm import Session

with Session(engine) as session:
    canceled = ax.activity.cancel_pending(session)
    print(f"Canceled {canceled} activities")
```

Use on startup to clean up stale activities from previous runs.

---

## list()

Get a paginated list of activities.

```python
def list(
    session: Session,
    page: int = 1,
    page_size: int = 50,
) -> ActivityListSchema
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `session` | `Session` | required | SQLAlchemy session |
| `page` | `int` | `1` | Page number (1-indexed) |
| `page_size` | `int` | `50` | Items per page |

### Returns

`ActivityListSchema` - Paginated activity list.

### Example

```python
with Session(engine) as session:
    result = ax.activity.list(session, page=1, page_size=20)

    print(f"Total: {result.total}")
    print(f"Page {result.page} of {result.pages}")

    for item in result.items:
        print(f"{item.agent_id}: {item.status}")
```

---

## detail()

Get detailed activity with full log history.

```python
def detail(
    session: Session,
    agent_id: str | UUID,
) -> ActivityDetailSchema | None
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `session` | `Session` | required | SQLAlchemy session |
| `agent_id` | `str \| UUID` | required | Activity identifier |

### Returns

`ActivityDetailSchema | None` - Activity details or None if not found.

### Example

```python
with Session(engine) as session:
    activity = ax.activity.detail(session, agent_id)

    if activity:
        print(f"Status: {activity.status}")
        print(f"Progress: {activity.latest_percentage}%")
        for log in activity.logs:
            print(f"  [{log.created_at}] {log.message}")
```

---

## Response Schemas

### ActivityDetailSchema

```python
class ActivityDetailSchema(BaseModel):
    agent_id: UUID
    agent_type: str
    created_at: datetime
    updated_at: datetime
    status: Status
    latest_percentage: int | None
    logs: list[ActivityLogSchema]
```

### ActivityListSchema

```python
class ActivityListSchema(BaseModel):
    items: list[ActivityListItemSchema]
    total: int
    page: int
    page_size: int
    pages: int
```

### ActivityListItemSchema

```python
class ActivityListItemSchema(BaseModel):
    agent_id: UUID
    agent_type: str
    created_at: datetime
    updated_at: datetime
    status: Status
    latest_log: str | None
    latest_percentage: int | None
```

### ActivityLogSchema

```python
class ActivityLogSchema(BaseModel):
    id: int
    message: str
    status: Status
    percentage: int | None
    created_at: datetime
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

## ORM Models

### Activity

```python
class Activity(Base):
    __tablename__ = "{prefix}activity"

    id: Mapped[UUID]           # Primary key
    agent_id: Mapped[UUID]     # Unique, indexed
    agent_type: Mapped[str]    # Task name
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]
    logs: Mapped[list["ActivityLog"]]  # Relationship
```

#### Methods

##### get_by_agent_id()

```python
@classmethod
def get_by_agent_id(
    cls,
    agent_id: str | UUID,
    session: Session | None = None,
) -> Activity | None
```

##### get_list()

```python
@classmethod
def get_list(
    cls,
    session: Session,
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[Activity], int]
```

##### get_pending_ids()

```python
@classmethod
def get_pending_ids(
    cls,
    session: Session,
) -> list[UUID]
```

##### append_log()

```python
def append_log(
    self,
    message: str,
    status: Status | None = None,
    percentage: int | None = None,
) -> None
```

### ActivityLog

```python
class ActivityLog(Base):
    __tablename__ = "{prefix}activity_log"

    id: Mapped[int]                    # Primary key
    activity_id: Mapped[UUID]          # Foreign key
    message: Mapped[str]               # Log message
    status: Mapped[Status]             # Status at time of log
    percentage: Mapped[int | None]
    created_at: Mapped[datetime]
```

---

## Direct Model Usage

For advanced queries, use SQLAlchemy directly:

```python
from agentexec.activity.models import Activity, ActivityLog, Status

# Query running activities
with Session(engine) as session:
    running = session.query(Activity).filter(
        Activity.logs.any(status=Status.RUNNING)
    ).all()

# Query recent activities
from datetime import datetime, timedelta
cutoff = datetime.utcnow() - timedelta(hours=1)

recent = session.query(Activity).filter(
    Activity.created_at >= cutoff
).order_by(Activity.created_at.desc()).all()

# Count by status
from sqlalchemy import func

counts = session.query(
    ActivityLog.status,
    func.count(ActivityLog.id)
).group_by(ActivityLog.status).all()
```
