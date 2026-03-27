import uuid
from typing import Any

from agentexec.activity.models import Status
from agentexec.activity.schemas import (
    ActivityDetailSchema,
    ActivityListItemSchema,
    ActivityListSchema,
)
from agentexec.state import ops


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
    session: Any = None,
    metadata: dict[str, Any] | None = None,
) -> uuid.UUID:
    """Create a new agent activity record with initial queued status.

    Args:
        task_name: Name/type of the task (e.g., "research", "analysis")
        message: Initial log message (default: "Agent queued")
        agent_id: Optional custom agent ID (string or UUID). If not provided, one will be auto-generated.
        session: Deprecated. Ignored — sessions are managed by the backend.
        metadata: Optional dict of arbitrary metadata to attach to the activity.
            Useful for multi-tenancy (e.g., {"organization_id": "org-123"}).

    Returns:
        The agent_id (as UUID object) of the created record
    """
    agent_id = normalize_agent_id(agent_id) if agent_id else generate_agent_id()
    ops.activity_create(agent_id, task_name, message, metadata)
    return agent_id


def update(
    agent_id: str | uuid.UUID,
    message: str,
    percentage: int | None = None,
    status: Status | None = None,
    session: Any = None,
) -> bool:
    """Update an agent's activity by adding a new log message.

    This function will set the status to RUNNING unless a different status is explicitly provided.

    Args:
        agent_id: The agent_id of the agent to update
        message: Log message to append
        percentage: Optional completion percentage (0-100)
        status: Optional status to set (default: RUNNING)
        session: Deprecated. Ignored — sessions are managed by the backend.

    Returns:
        True if successful

    Raises:
        ValueError: If agent_id not found
    """
    status_value = (status if status else Status.RUNNING).value
    ops.activity_append_log(
        normalize_agent_id(agent_id), message, status_value, percentage,
    )
    return True


def complete(
    agent_id: str | uuid.UUID,
    message: str = "Agent completed",
    percentage: int = 100,
    session: Any = None,
) -> bool:
    """Mark an agent activity as complete.

    Args:
        agent_id: The agent_id of the agent to mark as complete
        message: Log message (default: "Agent completed")
        percentage: Completion percentage (default: 100)
        session: Deprecated. Ignored — sessions are managed by the backend.

    Returns:
        True if successful

    Raises:
        ValueError: If agent_id not found
    """
    ops.activity_append_log(
        normalize_agent_id(agent_id), message, Status.COMPLETE.value, percentage,
    )
    return True


def error(
    agent_id: str | uuid.UUID,
    message: str = "Agent failed",
    percentage: int = 100,
    session: Any = None,
) -> bool:
    """Mark an agent activity as failed.

    Args:
        agent_id: The agent_id of the agent to mark as failed
        message: Log message (default: "Agent failed")
        percentage: Completion percentage (default: 100)
        session: Deprecated. Ignored — sessions are managed by the backend.

    Returns:
        True if successful

    Raises:
        ValueError: If agent_id not found
    """
    ops.activity_append_log(
        normalize_agent_id(agent_id), message, Status.ERROR.value, percentage,
    )
    return True


def cancel_pending(
    session: Any = None,
) -> int:
    """Mark all queued and running agents as canceled.

    Useful during application shutdown to clean up pending tasks.

    Returns:
        Number of agents that were canceled
    """
    pending_agent_ids = ops.activity_get_pending_ids()
    for aid in pending_agent_ids:
        ops.activity_append_log(
            aid, "Canceled due to shutdown", Status.CANCELED.value, None,
        )
    return len(pending_agent_ids)


def list(
    session: Any = None,
    page: int = 1,
    page_size: int = 50,
    metadata_filter: dict[str, Any] | None = None,
) -> ActivityListSchema:
    """List activities with pagination.

    Args:
        session: Deprecated. Ignored — sessions are managed by the backend.
        page: Page number (1-indexed)
        page_size: Number of items per page
        metadata_filter: Optional dict of key-value pairs to filter by.
            Activities must have metadata containing all specified keys
            with exactly matching values.

    Returns:
        ActivityList with list of ActivityListItemSchema items
    """
    rows, total = ops.activity_list(page, page_size, metadata_filter)
    return ActivityListSchema(
        items=[ActivityListItemSchema.model_validate(row) for row in rows],
        total=total,
        page=page,
        page_size=page_size,
    )


def detail(
    session: Any = None,
    agent_id: str | uuid.UUID | None = None,
    metadata_filter: dict[str, Any] | None = None,
) -> ActivityDetailSchema | None:
    """Get a single activity by agent_id with all logs.

    Args:
        session: Deprecated. Ignored — sessions are managed by the backend.
        agent_id: The agent_id to look up
        metadata_filter: Optional dict of key-value pairs to filter by.
            If provided and the activity's metadata doesn't match,
            returns None (same as if not found).

    Returns:
        ActivityDetailSchema with full log history, or None if not found
        or if metadata doesn't match
    """
    if agent_id is None:
        return None
    item = ops.activity_get(normalize_agent_id(agent_id), metadata_filter)
    if item is not None:
        return ActivityDetailSchema.model_validate(item)
    return None


def count_active(session: Any = None) -> int:
    """Get count of active (queued or running) agents.

    Args:
        session: Deprecated. Ignored — sessions are managed by the backend.

    Returns:
        Count of agents with QUEUED or RUNNING status
    """
    return ops.activity_count_active()
