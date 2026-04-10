import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from agentexec.activity.handlers import ActivityHandler, PostgresHandler
from agentexec.activity.models import Activity, ActivityLog
from agentexec.activity.producer import (
    create,
    update,
    complete,
    error,
    cancel_pending,
    generate_agent_id,
    normalize_agent_id,
)
from agentexec.activity.schemas import (
    ActivityDetailSchema,
    ActivityListItemSchema,
    ActivityListSchema,
    ActivityLogSchema,
)
from agentexec.activity.status import Status
from agentexec.core.db import get_session

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

handler: ActivityHandler = PostgresHandler()


async def list(
    session: AsyncSession | None = None,
    page: int = 1,
    page_size: int = 50,
    metadata_filter: dict[str, Any] | None = None,
) -> ActivityListSchema:
    """List activities with pagination.

    Args:
        session: Optional async SQLAlchemy session. Falls back to ``get_session()``.
        page: Page number (1-indexed).
        page_size: Number of items per page.
        metadata_filter: Optional dict to filter by metadata fields.
    """
    from sqlalchemy import func, select

    async with session or get_session() as db:
        count_query = select(func.count(Activity.id))
        if metadata_filter:
            for key, value in metadata_filter.items():
                count_query = count_query.where(Activity.metadata_[key].as_string() == str(value))
        total_result = await db.execute(count_query)
        total = total_result.scalar() or 0

        rows = await Activity.get_list(
            db,
            page=page,
            page_size=page_size,
            metadata_filter=metadata_filter,
        )
        return ActivityListSchema(
            items=[ActivityListItemSchema.model_validate(row) for row in rows],
            total=total,
            page=page,
            page_size=page_size,
        )


async def detail(
    session: AsyncSession | None = None,
    agent_id: str | uuid.UUID | None = None,
    metadata_filter: dict[str, Any] | None = None,
) -> ActivityDetailSchema | None:
    """Get a single activity by agent_id.

    Args:
        session: Optional async SQLAlchemy session. Falls back to ``get_session()``.
        agent_id: The agent_id to look up.
        metadata_filter: Optional dict to filter by metadata fields.
    """
    if agent_id is None:
        return None
    if isinstance(agent_id, str):
        agent_id = uuid.UUID(agent_id)

    async with session or get_session() as db:
        item = await Activity.get_by_agent_id(db, agent_id, metadata_filter=metadata_filter)
        if item is not None:
            return ActivityDetailSchema.model_validate(item)
        return None


async def count_active(session: AsyncSession | None = None) -> int:
    """Count active (queued or running) agents.

    Args:
        session: Optional async SQLAlchemy session. Falls back to ``get_session()``.
    """
    async with session or get_session() as db:
        return await Activity.get_active_count(db)
