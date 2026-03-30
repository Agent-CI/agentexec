from agentexec.activity.models import Activity, ActivityLog
from agentexec.activity.status import Status
from agentexec.activity.schemas import (
    ActivityDetailSchema,
    ActivityListItemSchema,
    ActivityListSchema,
    ActivityLogSchema,
)
from agentexec.activity.handlers import ActivityHandler, PostgresHandler
from agentexec.activity.producer import (
    create,
    update,
    complete,
    error,
    cancel_pending,
    generate_agent_id,
    normalize_agent_id,
)

handler: ActivityHandler = PostgresHandler()

import uuid
from typing import Any

from sqlalchemy.orm import Session


async def list(
    session: Session | None = None,
    page: int = 1,
    page_size: int = 50,
    metadata_filter: dict[str, Any] | None = None,
) -> ActivityListSchema:
    """List activities with pagination."""
    from agentexec.core.db import get_session

    with session or get_session() as db:
        query = db.query(Activity)
        if metadata_filter:
            for key, value in metadata_filter.items():
                query = query.filter(Activity.metadata_[key].as_string() == str(value))
        total = query.count()

        rows = Activity.get_list(db, page=page, page_size=page_size, metadata_filter=metadata_filter)
        return ActivityListSchema(
            items=[ActivityListItemSchema.model_validate(row) for row in rows],
            total=total,
            page=page,
            page_size=page_size,
        )


async def detail(
    session: Session | None = None,
    agent_id: str | uuid.UUID | None = None,
    metadata_filter: dict[str, Any] | None = None,
) -> ActivityDetailSchema | None:
    """Get a single activity by agent_id."""
    from agentexec.core.db import get_session

    if agent_id is None:
        return None
    if isinstance(agent_id, str):
        agent_id = uuid.UUID(agent_id)

    with session or get_session() as db:
        item = Activity.get_by_agent_id(db, agent_id, metadata_filter=metadata_filter)
        if item is not None:
            return ActivityDetailSchema.model_validate(item)
        return None


async def count_active(session: Session | None = None) -> int:
    """Count active (queued or running) agents."""
    from agentexec.core.db import get_session

    with session or get_session() as db:
        return Activity.get_active_count(db)


__all__ = [
    # Models
    "Activity",
    "ActivityLog",
    "Status",
    # Schemas
    "ActivityLogSchema",
    "ActivityDetailSchema",
    "ActivityListItemSchema",
    "ActivityListSchema",
    # Lifecycle API
    "create",
    "update",
    "complete",
    "error",
    "cancel_pending",
    "generate_agent_id",
    "normalize_agent_id",
    # Query API
    "list",
    "detail",
    "count_active",
]
