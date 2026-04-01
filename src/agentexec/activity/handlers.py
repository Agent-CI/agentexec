"""Activity event handlers — pluggable persistence for lifecycle events.

The activity system uses a handler pattern to decouple event emission from
persistence. Every call to ``activity.create()``, ``activity.update()``, etc.
emits a typed event (``ActivityCreated`` or ``ActivityUpdated``) and routes it
through ``activity.handler``, a callable that decides what to do with it.

Two handlers are provided:

- ``PostgresHandler`` (default): Writes events directly to Postgres via
  SQLAlchemy. Used by API servers and the pool's main process.

- ``IPCHandler``: Serializes events onto a ``multiprocessing.Queue`` for
  the pool to receive and persist. Used by worker processes, which don't
  have database access.

The handler is swapped at process init time. Workers set the IPC handler
during startup; everything else uses the default Postgres handler::

    # Worker process (set automatically by Pool)
    from agentexec.activity.handlers import IPCHandler
    activity.handler = IPCHandler(tx_queue)

    # API server or pool process (default, no setup needed)
    await activity.update(agent_id, "Processing", percentage=50)
    # → writes directly to Postgres

Custom handlers can be implemented by conforming to the ``ActivityHandler``
protocol — any callable that accepts ``ActivityCreated | ActivityUpdated``.
"""

from __future__ import annotations

import multiprocessing as mp
from typing import Protocol

from agentexec.activity.events import ActivityCreated, ActivityEvent, ActivityUpdated
from agentexec.activity.status import Status


class ActivityHandler(Protocol):
    """Protocol for activity event handlers.

    Any callable that accepts an ``ActivityCreated`` or ``ActivityUpdated``
    event satisfies this protocol.
    """
    def __call__(self, event: ActivityEvent) -> None: ...


class PostgresHandler:
    """Writes activity events directly to Postgres.

    This is the default handler. It creates a short-lived database session
    for each event, writes the appropriate records, and commits.
    """

    def __call__(self, event: ActivityEvent) -> None:
        match event:
            case ActivityCreated(agent_id=agent_id, task_name=task_name, message=message, metadata=metadata):
                from agentexec.activity.models import Activity, ActivityLog
                from agentexec.core.db import get_session

                with get_session() as db:
                    record = Activity(agent_id=agent_id, agent_type=task_name, metadata_=metadata)
                    db.add(record)
                    db.flush()
                    db.add(ActivityLog(
                        activity_id=record.id,
                        message=message,
                        status=Status.QUEUED,
                        percentage=0,
                    ))
                    db.commit()

            case ActivityUpdated(agent_id=agent_id, message=message, status=status, percentage=percentage):
                from agentexec.activity.models import Activity
                from agentexec.core.db import get_session

                with get_session() as db:
                    Activity.append_log(
                        session=db,
                        agent_id=agent_id,
                        message=message,
                        status=Status(status),
                        percentage=percentage,
                    )


class IPCHandler:
    """Sends activity events to the pool via multiprocessing queue.

    Worker processes use this handler so they don't need database access.
    Events are picked up by the pool's event loop and written to Postgres
    using the default ``PostgresHandler``.
    """

    tx: mp.Queue

    def __init__(self, tx: mp.Queue) -> None:
        self.tx = tx

    def __call__(self, event: ActivityEvent) -> None:
        self.tx.put_nowait(event)
