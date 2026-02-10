import uuid
from typing import Any, Coroutine, overload

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from agentexec.activity.models import Activity, ActivityLog, Status
from agentexec.activity.schemas import (
    ActivityDetailSchema,
    ActivityListItemSchema,
    ActivityListSchema,
)
from agentexec.core.db import get_global_session


def generate_agent_id() -> uuid.UUID:
    """Generate a new UUID for an agent.

    This is the centralized function for generating agent IDs.
    Users can override this if they need custom ID generation logic.

    Returns:
        A new UUID4 object
    """
    return uuid.uuid4()


def normalize_agent_id(agent_id: str | uuid.UUID) -> uuid.UUID:
    """Normalize agent_id to UUID object.

    Args:
        agent_id: Either a string UUID or UUID object

    Returns:
        UUID object

    Raises:
        ValueError: If string is not a valid UUID
    """
    if isinstance(agent_id, str):
        return uuid.UUID(agent_id)
    return agent_id


# ---------------------------------------------------------------------------
# Private async implementations
# ---------------------------------------------------------------------------


async def _acreate(
    task_name: str,
    message: str,
    agent_id: str | uuid.UUID | None,
    session: AsyncSession,
    metadata: dict[str, Any] | None,
) -> uuid.UUID:
    aid = normalize_agent_id(agent_id) if agent_id else generate_agent_id()

    activity_record = Activity(
        agent_id=aid,
        agent_type=task_name,
        metadata_=metadata,
    )
    session.add(activity_record)
    await session.flush()

    log = ActivityLog(
        activity_id=activity_record.id,
        message=message,
        status=Status.QUEUED,
        percentage=0,
    )
    session.add(log)
    await session.commit()

    return aid


async def _aupdate(
    agent_id: str | uuid.UUID,
    message: str,
    percentage: int | None,
    status: Status | None,
    session: AsyncSession,
) -> bool:
    await Activity.aappend_log(
        session=session,
        agent_id=normalize_agent_id(agent_id),
        message=message,
        status=status if status else Status.RUNNING,
        percentage=percentage,
    )
    return True


async def _acomplete(
    agent_id: str | uuid.UUID,
    message: str,
    percentage: int,
    session: AsyncSession,
) -> bool:
    await Activity.aappend_log(
        session=session,
        agent_id=normalize_agent_id(agent_id),
        message=message,
        status=Status.COMPLETE,
        percentage=percentage,
    )
    return True


async def _aerror(
    agent_id: str | uuid.UUID,
    message: str,
    percentage: int,
    session: AsyncSession,
) -> bool:
    await Activity.aappend_log(
        session=session,
        agent_id=normalize_agent_id(agent_id),
        message=message,
        status=Status.ERROR,
        percentage=percentage,
    )
    return True


async def _acancel_pending(session: AsyncSession) -> int:
    pending_agent_ids = await Activity.aget_pending_ids(session)
    for aid in pending_agent_ids:
        await Activity.aappend_log(
            session=session,
            agent_id=aid,
            message="Canceled due to shutdown",
            status=Status.CANCELED,
            percentage=None,
        )
    await session.commit()
    return len(pending_agent_ids)


async def _alist(
    session: AsyncSession,
    page: int,
    page_size: int,
    metadata_filter: dict[str, Any] | None,
) -> ActivityListSchema:
    # Count query
    count_stmt = select(func.count()).select_from(Activity)
    if metadata_filter:
        for key, value in metadata_filter.items():
            count_stmt = count_stmt.filter(Activity.metadata_[key].as_string() == str(value))
    result = await session.execute(count_stmt)
    total = result.scalar() or 0

    rows = await Activity.aget_list(
        session,
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


async def _adetail(
    session: AsyncSession,
    agent_id: str | uuid.UUID,
    metadata_filter: dict[str, Any] | None,
) -> ActivityDetailSchema | None:
    if item := await Activity.aget_by_agent_id(session, agent_id, metadata_filter=metadata_filter):
        return ActivityDetailSchema.model_validate(item)
    return None


async def _acount_active(session: AsyncSession) -> int:
    return await Activity.aget_active_count(session)


# ---------------------------------------------------------------------------
# Public API – overloaded for sync / async dispatch
# ---------------------------------------------------------------------------


@overload
def create(
    task_name: str,
    message: str = ...,
    agent_id: str | uuid.UUID | None = ...,
    session: AsyncSession = ...,
    metadata: dict[str, Any] | None = ...,
) -> Coroutine[Any, Any, uuid.UUID]: ...


@overload
def create(
    task_name: str,
    message: str = ...,
    agent_id: str | uuid.UUID | None = ...,
    session: Session | None = ...,
    metadata: dict[str, Any] | None = ...,
) -> uuid.UUID: ...


def create(
    task_name: str,
    message: str = "Agent queued",
    agent_id: str | uuid.UUID | None = None,
    session: Session | AsyncSession | None = None,
    metadata: dict[str, Any] | None = None,
) -> uuid.UUID | Coroutine[Any, Any, uuid.UUID]:
    """Create a new agent activity record with initial queued status.

    Accepts both sync and async sessions.  When an ``AsyncSession`` is
    passed the call returns an awaitable coroutine; otherwise it executes
    synchronously and returns the ``agent_id`` directly.

    Args:
        task_name: Name/type of the task (e.g., "research", "analysis")
        message: Initial log message (default: "Agent queued")
        agent_id: Optional custom agent ID (string or UUID). If not provided, one will be auto-generated.
        session: SQLAlchemy session.  Pass an ``AsyncSession`` to use async I/O.
            If ``None``, falls back to the global sync session.
        metadata: Optional dict of arbitrary metadata to attach to the activity.
            Useful for multi-tenancy (e.g., {"organization_id": "org-123"}).

    Returns:
        The agent_id (as UUID object) of the created record
    """
    if isinstance(session, AsyncSession):
        return _acreate(task_name, message, agent_id, session, metadata)

    agent_id = normalize_agent_id(agent_id) if agent_id else generate_agent_id()
    db = session or get_global_session()

    activity_record = Activity(
        agent_id=agent_id,
        agent_type=task_name,
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

    return agent_id


@overload
def update(
    agent_id: str | uuid.UUID,
    message: str,
    percentage: int | None = ...,
    status: Status | None = ...,
    session: AsyncSession = ...,
) -> Coroutine[Any, Any, bool]: ...


@overload
def update(
    agent_id: str | uuid.UUID,
    message: str,
    percentage: int | None = ...,
    status: Status | None = ...,
    session: Session | None = ...,
) -> bool: ...


def update(
    agent_id: str | uuid.UUID,
    message: str,
    percentage: int | None = None,
    status: Status | None = None,
    session: Session | AsyncSession | None = None,
) -> bool | Coroutine[Any, Any, bool]:
    """Update an agent's activity by adding a new log message.

    This function will set the status to RUNNING unless a different status is explicitly provided.

    Args:
        agent_id: The agent_id of the agent to update
        message: Log message to append
        percentage: Optional completion percentage (0-100)
        status: Optional status to set (default: RUNNING)
        session: SQLAlchemy session.  Pass an ``AsyncSession`` to use async I/O.

    Returns:
        True if successful

    Raises:
        ValueError: If agent_id not found
    """
    if isinstance(session, AsyncSession):
        return _aupdate(agent_id, message, percentage, status, session)

    db = session or get_global_session()

    Activity.append_log(
        session=db,
        agent_id=normalize_agent_id(agent_id),
        message=message,
        status=status if status else Status.RUNNING,
        percentage=percentage,
    )
    return True


@overload
def complete(
    agent_id: str | uuid.UUID,
    message: str = ...,
    percentage: int = ...,
    session: AsyncSession = ...,
) -> Coroutine[Any, Any, bool]: ...


@overload
def complete(
    agent_id: str | uuid.UUID,
    message: str = ...,
    percentage: int = ...,
    session: Session | None = ...,
) -> bool: ...


def complete(
    agent_id: str | uuid.UUID,
    message: str = "Agent completed",
    percentage: int = 100,
    session: Session | AsyncSession | None = None,
) -> bool | Coroutine[Any, Any, bool]:
    """Mark an agent activity as complete.

    Args:
        agent_id: The agent_id of the agent to mark as complete
        message: Log message (default: "Agent completed")
        percentage: Completion percentage (default: 100)
        session: SQLAlchemy session.  Pass an ``AsyncSession`` to use async I/O.

    Returns:
        True if successful

    Raises:
        ValueError: If agent_id not found
    """
    if isinstance(session, AsyncSession):
        return _acomplete(agent_id, message, percentage, session)

    db = session or get_global_session()

    Activity.append_log(
        session=db,
        agent_id=normalize_agent_id(agent_id),
        message=message,
        status=Status.COMPLETE,
        percentage=percentage,
    )
    return True


@overload
def error(
    agent_id: str | uuid.UUID,
    message: str = ...,
    percentage: int = ...,
    session: AsyncSession = ...,
) -> Coroutine[Any, Any, bool]: ...


@overload
def error(
    agent_id: str | uuid.UUID,
    message: str = ...,
    percentage: int = ...,
    session: Session | None = ...,
) -> bool: ...


def error(
    agent_id: str | uuid.UUID,
    message: str = "Agent failed",
    percentage: int = 100,
    session: Session | AsyncSession | None = None,
) -> bool | Coroutine[Any, Any, bool]:
    """Mark an agent activity as failed.

    Args:
        agent_id: The agent_id of the agent to mark as failed
        message: Log message (default: "Agent failed")
        percentage: Completion percentage (default: 100)
        session: SQLAlchemy session.  Pass an ``AsyncSession`` to use async I/O.

    Returns:
        True if successful

    Raises:
        ValueError: If agent_id not found
    """
    if isinstance(session, AsyncSession):
        return _aerror(agent_id, message, percentage, session)

    db = session or get_global_session()

    Activity.append_log(
        session=db,
        agent_id=normalize_agent_id(agent_id),
        message=message,
        status=Status.ERROR,
        percentage=percentage,
    )
    return True


@overload
def cancel_pending(
    session: AsyncSession = ...,
) -> Coroutine[Any, Any, int]: ...


@overload
def cancel_pending(
    session: Session | None = ...,
) -> int: ...


def cancel_pending(
    session: Session | AsyncSession | None = None,
) -> int | Coroutine[Any, Any, int]:
    """Mark all queued and running agents as canceled.

    Useful during application shutdown to clean up pending tasks.

    Returns:
        Number of agents that were canceled
    """
    if isinstance(session, AsyncSession):
        return _acancel_pending(session)

    db = session or get_global_session()

    pending_agent_ids = Activity.get_pending_ids(db)
    for aid in pending_agent_ids:
        Activity.append_log(
            session=db,
            agent_id=aid,
            message="Canceled due to shutdown",
            status=Status.CANCELED,
            percentage=None,
        )

    db.commit()
    return len(pending_agent_ids)


@overload
def list(
    session: AsyncSession,
    page: int = ...,
    page_size: int = ...,
    metadata_filter: dict[str, Any] | None = ...,
) -> Coroutine[Any, Any, ActivityListSchema]: ...


@overload
def list(
    session: Session,
    page: int = ...,
    page_size: int = ...,
    metadata_filter: dict[str, Any] | None = ...,
) -> ActivityListSchema: ...


def list(
    session: Session | AsyncSession,
    page: int = 1,
    page_size: int = 50,
    metadata_filter: dict[str, Any] | None = None,
) -> ActivityListSchema | Coroutine[Any, Any, ActivityListSchema]:
    """List activities with pagination.

    Args:
        session: SQLAlchemy session (sync or async)
        page: Page number (1-indexed)
        page_size: Number of items per page
        metadata_filter: Optional dict of key-value pairs to filter by.
            Activities must have metadata containing all specified keys
            with exactly matching values.

    Returns:
        ActivityList with list of ActivityListItemSchema items
    """
    if isinstance(session, AsyncSession):
        return _alist(session, page, page_size, metadata_filter)

    # Build base query for total count
    query = session.query(Activity)
    if metadata_filter:
        for key, value in metadata_filter.items():
            query = query.filter(Activity.metadata_[key].as_string() == str(value))
    total = query.count()

    rows = Activity.get_list(
        session,
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


@overload
def detail(
    session: AsyncSession,
    agent_id: str | uuid.UUID,
    metadata_filter: dict[str, Any] | None = ...,
) -> Coroutine[Any, Any, ActivityDetailSchema | None]: ...


@overload
def detail(
    session: Session,
    agent_id: str | uuid.UUID,
    metadata_filter: dict[str, Any] | None = ...,
) -> ActivityDetailSchema | None: ...


def detail(
    session: Session | AsyncSession,
    agent_id: str | uuid.UUID,
    metadata_filter: dict[str, Any] | None = None,
) -> (ActivityDetailSchema | None) | Coroutine[Any, Any, ActivityDetailSchema | None]:
    """Get a single activity by agent_id with all logs.

    Args:
        session: SQLAlchemy session (sync or async)
        agent_id: The agent_id to look up
        metadata_filter: Optional dict of key-value pairs to filter by.
            If provided and the activity's metadata doesn't match,
            returns None (same as if not found).

    Returns:
        ActivityDetailSchema with full log history, or None if not found
        or if metadata doesn't match
    """
    if isinstance(session, AsyncSession):
        return _adetail(session, agent_id, metadata_filter)

    if item := Activity.get_by_agent_id(session, agent_id, metadata_filter=metadata_filter):
        return ActivityDetailSchema.model_validate(item)
    return None


@overload
def count_active(session: AsyncSession) -> Coroutine[Any, Any, int]: ...


@overload
def count_active(session: Session) -> int: ...


def count_active(session: Session | AsyncSession) -> int | Coroutine[Any, Any, int]:
    """Get count of active (queued or running) agents.

    Args:
        session: SQLAlchemy session (sync or async)

    Returns:
        Count of agents with QUEUED or RUNNING status
    """
    if isinstance(session, AsyncSession):
        return _acount_active(session)

    return Activity.get_active_count(session)
