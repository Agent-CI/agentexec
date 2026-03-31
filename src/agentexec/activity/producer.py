"""Activity event producer — the public API for activity lifecycle.

All activity methods emit typed events routed through ``activity.handler``.
By default, events are written directly to Postgres. In worker processes,
the handler is swapped to send events via IPC to the pool.

See ``activity.handlers`` for the handler implementations.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

import agentexec.activity as activity
from agentexec.activity.events import ActivityCreated, ActivityUpdated
from agentexec.activity.status import Status


def generate_agent_id() -> uuid.UUID:
    """Generate a new UUID4 agent identifier."""
    return uuid.uuid4()


def normalize_agent_id(agent_id: str | uuid.UUID) -> uuid.UUID:
    """Coerce a string or UUID to a UUID object."""
    if isinstance(agent_id, str):
        return uuid.UUID(agent_id)
    return agent_id




async def create(
    task_name: str,
    message: str = "Agent queued",
    agent_id: str | uuid.UUID | None = None,
    session: Session | None = None,
    metadata: dict[str, Any] | None = None,
) -> uuid.UUID:
    """Create a new activity record with an initial "queued" log entry.

    Called during ``ax.enqueue()`` to register the task in the activity
    stream before it hits the queue.

    Args:
        task_name: The registered task name (e.g. ``"research"``).
        message: Initial log message.
        agent_id: Optional pre-generated agent ID. Auto-generated if omitted.
        session: Unused — kept for backwards compatibility.
        metadata: Arbitrary key-value pairs attached to the activity
            (e.g. ``{"organization_id": "org-123"}``).

    Returns:
        The agent_id (UUID) of the created record.

    Example::

        agent_id = await activity.create("research", metadata={"org": "acme"})
    """
    agent_id = normalize_agent_id(agent_id) if agent_id else generate_agent_id()
    activity.handler(ActivityCreated(
        agent_id=agent_id,
        task_name=task_name,
        message=message,
        metadata=metadata,
    ))
    return agent_id


async def update(
    agent_id: str | uuid.UUID,
    message: str,
    percentage: int | None = None,
    status: Status | None = None,
    session: Session | None = None,
) -> bool:
    """Append a log entry to an existing activity record.

    Defaults to ``Status.RUNNING`` if no status is provided.

    Args:
        agent_id: The agent to update.
        message: Log message describing the current state.
        percentage: Optional completion percentage (0-100).
        status: Optional status override (default: ``RUNNING``).
        session: Unused — kept for backwards compatibility.

    Example::

        await activity.update(agent_id, "Fetching data", percentage=30)
    """
    activity.handler(ActivityUpdated(
        agent_id=normalize_agent_id(agent_id),
        message=message,
        status=(status or Status.RUNNING).value,
        percentage=percentage,
    ))
    return True


async def complete(
    agent_id: str | uuid.UUID,
    message: str = "Agent completed",
    percentage: int = 100,
    session: Session | None = None,
) -> bool:
    """Mark an activity as complete.

    Args:
        agent_id: The agent to mark complete.
        message: Completion log message.
        percentage: Final percentage (default: 100).
        session: Unused — kept for backwards compatibility.

    Example::

        await activity.complete(agent_id)
    """
    activity.handler(ActivityUpdated(
        agent_id=normalize_agent_id(agent_id),
        message=message,
        status=Status.COMPLETE.value,
        percentage=percentage,
    ))
    return True


async def error(
    agent_id: str | uuid.UUID,
    message: str = "Agent failed",
    percentage: int = 100,
    session: Session | None = None,
) -> bool:
    """Mark an activity as failed.

    Args:
        agent_id: The agent to mark as errored.
        message: Error log message.
        percentage: Final percentage (default: 100).
        session: Unused — kept for backwards compatibility.

    Example::

        await activity.error(agent_id, "Connection timeout")
    """
    activity.handler(ActivityUpdated(
        agent_id=normalize_agent_id(agent_id),
        message=message,
        status=Status.ERROR.value,
        percentage=percentage,
    ))
    return True


async def cancel_pending(session: Session | None = None) -> int:
    """Cancel all queued and running activities.

    Typically called during pool shutdown to mark in-flight tasks as
    canceled. Reads pending IDs from Postgres and emits cancel events.

    Returns:
        Number of activities canceled.
    """
    from agentexec.activity.models import Activity
    from agentexec.core.db import get_session

    with session or get_session() as db:
        pending_ids = Activity.get_pending_ids(db)
        for agent_id in pending_ids:
            activity.handler(ActivityUpdated(
                agent_id=agent_id,
                message="Canceled due to shutdown",
                status=Status.CANCELED.value,
                percentage=None,
            ))
        return len(pending_ids)
