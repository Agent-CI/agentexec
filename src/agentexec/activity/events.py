from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel


class ActivityEvent(BaseModel):
    """Base class for all activity lifecycle events."""
    agent_id: uuid.UUID


class ActivityCreated(ActivityEvent):
    task_name: str
    message: str
    metadata: dict[str, Any] | None = None


class ActivityUpdated(ActivityEvent):
    message: str
    status: str
    percentage: int | None = None


# Resolve forward references
ActivityCreated.model_rebuild()
ActivityUpdated.model_rebuild()
