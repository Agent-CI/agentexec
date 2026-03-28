"""Activity event producer — called by workers to emit lifecycle events.

Events are sent via the state backend's transport (Redis pubsub or Kafka topic).
The pool's activity consumer receives these and writes them to Postgres.
"""

from __future__ import annotations

import json
import uuid
import warnings
from typing import Any

from agentexec.activity.status import Status
from agentexec.config import CONF
from agentexec.state import backend


ACTIVITY_CHANNEL = None


def _channel() -> str:
    global ACTIVITY_CHANNEL
    if ACTIVITY_CHANNEL is None:
        ACTIVITY_CHANNEL = backend.format_key(CONF.key_prefix, "activity")
    return ACTIVITY_CHANNEL


def generate_agent_id() -> uuid.UUID:
    """Generate a new UUID for an agent."""
    return uuid.uuid4()


def normalize_agent_id(agent_id: str | uuid.UUID) -> uuid.UUID:
    """Normalize agent_id to UUID object."""
    if isinstance(agent_id, str):
        return uuid.UUID(agent_id)
    return agent_id


async def _emit(event: dict[str, Any]) -> None:
    """Emit an activity event via the backend transport."""
    await backend.state.publish(_channel(), json.dumps(event, default=str))


async def create(
    task_name: str,
    message: str = "Agent queued",
    agent_id: str | uuid.UUID | None = None,
    session: Any = None,
    metadata: dict[str, Any] | None = None,
) -> uuid.UUID:
    """Create a new agent activity record with initial queued status."""
    if session is not None:
        warnings.warn("session is deprecated and will be removed", DeprecationWarning, stacklevel=2)

    agent_id = normalize_agent_id(agent_id) if agent_id else generate_agent_id()
    await _emit({
        "type": "create",
        "agent_id": str(agent_id),
        "task_name": task_name,
        "message": message,
        "metadata": metadata,
    })
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

    await _emit({
        "type": "append_log",
        "agent_id": str(normalize_agent_id(agent_id)),
        "message": message,
        "status": (status or Status.RUNNING).value,
        "percentage": percentage,
    })
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

    await _emit({
        "type": "append_log",
        "agent_id": str(normalize_agent_id(agent_id)),
        "message": message,
        "status": Status.COMPLETE.value,
        "percentage": percentage,
    })
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

    await _emit({
        "type": "append_log",
        "agent_id": str(normalize_agent_id(agent_id)),
        "message": message,
        "status": Status.ERROR.value,
        "percentage": percentage,
    })
    return True


async def cancel_pending(session: Any = None) -> int:
    """Mark all queued and running agents as canceled.

    NOTE: This queries Postgres directly since only the pool calls it
    during shutdown (when the consumer is still running).
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
