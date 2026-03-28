"""Activity event consumer — receives events from workers and writes to Postgres.

Run as a concurrent task in the pool's event loop alongside log streaming
and schedule processing.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from agentexec.activity.status import Status
from agentexec.config import CONF
from agentexec.state import backend


def _channel() -> str:
    return backend.format_key(CONF.key_prefix, "activity")


async def process_activity_stream() -> None:
    """Subscribe to activity events and persist them to Postgres."""
    from agentexec.activity.models import Activity, ActivityLog
    from agentexec.core.db import get_global_session

    async for message in backend.state.subscribe(_channel()):
        event = json.loads(message)
        db = get_global_session()

        if event["type"] == "create":
            activity_record = Activity(
                agent_id=UUID(event["agent_id"]),
                agent_type=event["task_name"],
                metadata_=event.get("metadata"),
            )
            db.add(activity_record)
            db.flush()

            log = ActivityLog(
                activity_id=activity_record.id,
                message=event["message"],
                status=Status.QUEUED,
                percentage=0,
            )
            db.add(log)
            db.commit()

        elif event["type"] == "append_log":
            Activity.append_log(
                session=db,
                agent_id=UUID(event["agent_id"]),
                message=event["message"],
                status=Status(event["status"]),
                percentage=event.get("percentage"),
            )
