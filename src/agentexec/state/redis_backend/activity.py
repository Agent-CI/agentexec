"""Redis backend activity operations — delegates to SQLAlchemy/Postgres.

The Redis deployment stack uses Postgres for activity tracking. All
functions use lazy imports to avoid circular dependencies with the
activity models module.
"""

from __future__ import annotations

import uuid
from typing import Any


def activity_create(
    agent_id: uuid.UUID,
    agent_type: str,
    message: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Create a new activity record with initial QUEUED log entry."""
    from agentexec.activity.models import Activity, ActivityLog, Status
    from agentexec.core.db import get_global_session

    db = get_global_session()
    activity_record = Activity(
        agent_id=agent_id,
        agent_type=agent_type,
        metadata_=metadata,
    )
    db.add(activity_record)
    db.flush()

    log = ActivityLog(
        activity_id=activity_record.id,
        message=message,
        status=Status.QUEUED,
        percentage=0,
    )
    db.add(log)
    db.commit()


def activity_append_log(
    agent_id: uuid.UUID,
    message: str,
    status: str,
    percentage: int | None = None,
) -> None:
    """Append a log entry to an existing activity record."""
    from agentexec.activity.models import Activity, Status as ActivityStatus
    from agentexec.core.db import get_global_session

    db = get_global_session()
    Activity.append_log(
        session=db,
        agent_id=agent_id,
        message=message,
        status=ActivityStatus(status),
        percentage=percentage,
    )


def activity_get(
    agent_id: uuid.UUID,
    metadata_filter: dict[str, Any] | None = None,
) -> Any:
    """Get a single activity record by agent_id.

    Returns an Activity ORM object (compatible with ActivityDetailSchema
    via from_attributes=True), or None if not found.
    """
    from agentexec.activity.models import Activity
    from agentexec.core.db import get_global_session

    db = get_global_session()
    return Activity.get_by_agent_id(db, agent_id, metadata_filter=metadata_filter)


def activity_list(
    page: int = 1,
    page_size: int = 50,
    metadata_filter: dict[str, Any] | None = None,
) -> tuple[list[Any], int]:
    """List activity records with pagination.

    Returns (rows, total) where rows are RowMapping objects compatible
    with ActivityListItemSchema via from_attributes=True.
    """
    from agentexec.activity.models import Activity
    from agentexec.core.db import get_global_session

    db = get_global_session()

    query = db.query(Activity)
    if metadata_filter:
        for key, value in metadata_filter.items():
            query = query.filter(Activity.metadata_[key].as_string() == str(value))
    total = query.count()

    rows = Activity.get_list(
        db, page=page, page_size=page_size, metadata_filter=metadata_filter,
    )
    return rows, total


def activity_count_active() -> int:
    """Count activities with QUEUED or RUNNING status."""
    from agentexec.activity.models import Activity
    from agentexec.core.db import get_global_session

    db = get_global_session()
    return Activity.get_active_count(db)


def activity_get_pending_ids() -> list[uuid.UUID]:
    """Get agent_ids for all activities with QUEUED or RUNNING status."""
    from agentexec.activity.models import Activity
    from agentexec.core.db import get_global_session

    db = get_global_session()
    return Activity.get_pending_ids(db)
