from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel


class ActivityCreated(BaseModel):
    agent_id: uuid.UUID
    task_name: str
    message: str
    metadata: dict[str, Any] | None = None


class ActivityUpdated(BaseModel):
    agent_id: uuid.UUID
    message: str
    status: str
    percentage: int | None = None


# Resolve forward references
ActivityCreated.model_rebuild()
ActivityUpdated.model_rebuild()
