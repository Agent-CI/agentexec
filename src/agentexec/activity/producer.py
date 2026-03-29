"""Activity event producer — called by workers to emit lifecycle events.

Events are sent via the multiprocessing queue to the pool, which writes
them to Postgres. Workers never touch the database directly.
"""

from __future__ import annotations

import multiprocessing as mp
import uuid
import warnings
from typing import Any

from agentexec.activity.status import Status


_tx: mp.Queue | None = None


def configure(tx: mp.Queue | None) -> None:
    """Set the multiprocessing queue for activity events."""
    global _tx
    _tx = tx


def _send(message: Any) -> None:
    """Send a message to the pool via the multiprocessing queue."""
    if _tx is not None:
        _tx.put_nowait(message)


def generate_agent_id() -> uuid.UUID:
    """Generate a new UUID for an agent."""
    return uuid.uuid4()


def normalize_agent_id(agent_id: str | uuid.UUID) -> uuid.UUID:
    """Normalize agent_id to UUID object."""
    if isinstance(agent_id, str):
        return uuid.UUID(agent_id)
    return agent_id


async def create(
    task_name: str,
    message: str = "Agent queued",
    agent_id: str | uuid.UUID | None = None,
    session: Any = None,
    metadata: dict[str, Any] | None = None,
) -> uuid.UUID:
    """Create a new agent activity record with initial queued status.

    Writes directly to Postgres — this runs on the API server / pool
    process, not on a worker.
    """
    if session is not None:
        warnings.warn("session is deprecated and will be removed", DeprecationWarning, stacklevel=2)

    from agentexec.activity.models import Activity, ActivityLog
    from agentexec.activity.status import Status as ActivityStatus
    from agentexec.core.db import get_global_session

    agent_id = normalize_agent_id(agent_id) if agent_id else generate_agent_id()
    db = get_global_session()
    record = Activity(agent_id=agent_id, agent_type=task_name, metadata_=metadata)
    db.add(record)
    db.flush()
    db.add(ActivityLog(activity_id=record.id, message=message, status=ActivityStatus.QUEUED, percentage=0))
    db.commit()
    return agent_id


async def update(
    agent_id: str | uuid.UUID,
    message: str,
    percentage: int | None = None,
    status: Status | None = None,
    session: Any = None,
) -> bool:
    """Update an agent's activity by adding a new log message."""
    if session is not None:
        warnings.warn("session is deprecated and will be removed", DeprecationWarning, stacklevel=2)

    from agentexec.worker.pool import ActivityUpdated

    _send(ActivityUpdated(
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
    session: Any = None,
) -> bool:
    """Mark an agent activity as complete."""
    if session is not None:
        warnings.warn("session is deprecated and will be removed", DeprecationWarning, stacklevel=2)

    from agentexec.worker.pool import ActivityUpdated

    _send(ActivityUpdated(
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
    session: Any = None,
) -> bool:
    """Mark an agent activity as failed."""
    if session is not None:
        warnings.warn("session is deprecated and will be removed", DeprecationWarning, stacklevel=2)

    from agentexec.worker.pool import ActivityUpdated

    _send(ActivityUpdated(
        agent_id=normalize_agent_id(agent_id),
        message=message,
        status=Status.ERROR.value,
        percentage=percentage,
    ))
    return True


async def cancel_pending(session: Any = None) -> int:
    """Mark all queued and running agents as canceled.

    NOTE: This runs on the pool process (not a worker), so it
    writes to Postgres directly.
    """
    if session is not None:
        warnings.warn("session is deprecated and will be removed", DeprecationWarning, stacklevel=2)

    from agentexec.activity.models import Activity
    from agentexec.core.db import get_global_session

    db = get_global_session()
    pending_ids = Activity.get_pending_ids(db)
    for agent_id in pending_ids:
        Activity.append_log(
            session=db,
            agent_id=agent_id,
            message="Canceled due to shutdown",
            status=Status.CANCELED,
            percentage=None,
        )
    return len(pending_ids)
