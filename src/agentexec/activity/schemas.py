import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, computed_field

from agentexec.activity.models import Status


class ActivityLogSchema(BaseModel):
    """Schema for an agent activity log entry."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    message: str
    status: Status
    percentage: int | None = 0
    created_at: datetime


class ActivityDetailSchema(BaseModel):
    """Schema for an agent activity record with optional logs."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    agent_id: uuid.UUID
    agent_type: str
    created_at: datetime
    updated_at: datetime
    logs: list[ActivityLogSchema] = Field(default_factory=list)
    metadata: dict[str, Any] | None = Field(default=None, alias="metadata_", exclude=True)


class ActivityListItemSchema(BaseModel):
    """Lightweight summary of agent activity showing only latest update.

    Note: Elapsed time can be calculated on the frontend as:
    latest_log_timestamp - started_at
    """

    model_config = ConfigDict(from_attributes=True)

    agent_id: uuid.UUID
    agent_type: str
    status: Status
    latest_log_message: str | None = None
    latest_log_timestamp: datetime | None = None
    percentage: int | None = 0
    started_at: datetime | None = None
    metadata: dict[str, Any] | None = Field(default=None, exclude=True)

    @computed_field
    def elapsed_time_seconds(self) -> int:
        if self.latest_log_timestamp and self.started_at:
            return int((self.latest_log_timestamp - self.started_at).total_seconds())
        return 0


class ActivityListSchema(BaseModel):
    """Paginated list of activity summaries."""

    model_config = ConfigDict(from_attributes=True)

    items: list[ActivityListItemSchema]
    total: int
    page: int
    page_size: int

    @computed_field
    def total_pages(self) -> int:
        return (self.total + self.page_size - 1) // self.page_size
