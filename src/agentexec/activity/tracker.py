import uuid

from sqlalchemy.orm import Session

from agentexec.activity.models import Activity, ActivityLog, Status
from agentexec.activity.schemas import (
    ActivityDetailSchema,
    ActivityListItemSchema,
    ActivityListSchema,
)
from agentexec.core.db import get_global_session
from agentexec import state


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


def create(
    task_name: str,
    message: str = "Agent queued",
    agent_id: str | uuid.UUID | None = None,
    session: Session | None = None,
) -> uuid.UUID:
    """Create a new agent activity record with initial queued status.

    Args:
        task_name: Name/type of the task (e.g., "research", "analysis")
        initial_message: Initial log message (default: "Agent queued")
        agent_id: Optional custom agent ID (string or UUID). If not provided, one will be auto-generated.

    Returns:
        The agent_id (as UUID object) of the created record
    """
    agent_id = normalize_agent_id(agent_id) if agent_id else generate_agent_id()
    db = session or get_global_session()

    activity_record = Activity(
        agent_id=agent_id,
        agent_type=task_name,
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


def update(
    agent_id: str | uuid.UUID,
    message: str,
    percentage: int | None = None,
    status: Status | None = None,
    session: Session | None = None,
) -> bool:
    """Update an agent's activity by adding a new log message.

    This function will set the status to RUNNING unless a different status is explicitly provided.

    Args:
        agent_id: The agent_id of the agent to update
        message: Log message to append
        percentage: Optional completion percentage (0-100)
        status: Optional status to set (default: RUNNING)
        session: Optional SQLAlchemy session. If not provided, uses global session factory.

    Returns:
        True if successful

    Raises:
        ValueError: If agent_id not found
    """
    db = session or get_global_session()

    Activity.append_log(
        session=db,
        agent_id=normalize_agent_id(agent_id),
        message=message,
        status=status if status else Status.RUNNING,
        percentage=percentage,
    )
    return True


def complete(
    agent_id: str | uuid.UUID,
    message: str = "Agent completed",
    percentage: int = 100,
    session: Session | None = None,
) -> bool:
    """Mark an agent activity as complete.

    Args:
        agent_id: The agent_id of the agent to mark as complete
        message: Log message (default: "Agent completed")
        percentage: Completion percentage (default: 100)
        session: Optional SQLAlchemy session. If not provided, uses global session factory.

    Returns:
        True if successful

    Raises:
        ValueError: If agent_id not found
    """
    db = session or get_global_session()

    Activity.append_log(
        session=db,
        agent_id=normalize_agent_id(agent_id),
        message=message,
        status=Status.COMPLETE,
        percentage=percentage,
    )
    return True


def error(
    agent_id: str | uuid.UUID,
    message: str = "Agent failed",
    percentage: int = 100,
    session: Session | None = None,
) -> bool:
    """Mark an agent activity as failed.

    Args:
        agent_id: The agent_id of the agent to mark as failed
        message: Log message (default: "Agent failed")
        percentage: Completion percentage (default: 100)
        session: Optional SQLAlchemy session. If not provided, uses ScopedSession.

    Returns:
        True if successful

    Raises:
        ValueError: If agent_id not found
    """
    db = session or get_global_session()

    Activity.append_log(
        session=db,
        agent_id=normalize_agent_id(agent_id),
        message=message,
        status=Status.ERROR,
        percentage=percentage,
    )
    return True


def cancel_pending(
    session: Session | None = None,
    clear_redis: bool = True,
) -> int:
    """Mark all queued and running agents as canceled.

    Useful during application shutdown to clean up pending tasks.

    Args:
        session: Optional SQLAlchemy session. If not provided, uses global session.
        clear_redis: If True, also clear Redis keys managed by this application
            (task queue, results, events, trackers). Defaults to True.

    Returns:
        Number of agents that were canceled
    """
    db = session or get_global_session()

    pending_agent_ids = Activity.get_pending_ids(db)
    for agent_id in pending_agent_ids:
        Activity.append_log(
            session=db,
            agent_id=agent_id,
            message="Canceled due to shutdown",
            status=Status.CANCELED,
            percentage=None,
        )

    db.commit()

    if clear_redis:
        state.clear_keys()

    return len(pending_agent_ids)


def list(
    session: Session,
    page: int = 1,
    page_size: int = 50,
) -> ActivityListSchema:
    """List activities with pagination.

    Args:
        session: SQLAlchemy session to use for the query
        page: Page number (1-indexed)
        page_size: Number of items per page

    Returns:
        ActivityList with list of ActivityListItemSchema items
    """
    total = session.query(Activity).count()
    rows = Activity.get_list(session, page=page, page_size=page_size)

    return ActivityListSchema(
        items=[ActivityListItemSchema.model_validate(row) for row in rows],
        total=total,
        page=page,
        page_size=page_size,
    )


def detail(
    session: Session,
    agent_id: str | uuid.UUID,
) -> ActivityDetailSchema | None:
    """Get a single activity by agent_id with all logs.

    Args:
        session: SQLAlchemy session to use for the query
        agent_id: The agent_id to look up

    Returns:
        ActivityDetailSchema with full log history, or None if not found
    """
    if item := Activity.get_by_agent_id(session, agent_id):
        return ActivityDetailSchema.model_validate(item)
    return None


def count_active(session: Session) -> int:
    """Get count of active (queued or running) agents.

    Args:
        session: SQLAlchemy session to use for the query

    Returns:
        Count of agents with QUEUED or RUNNING status
    """
    return Activity.get_active_count(session)
