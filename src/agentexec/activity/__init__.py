"""Activity tracking for agent execution."""

from agentexec.activity import tracker as activity
from agentexec.activity.models import Activity, ActivityLog, Status
from agentexec.activity.schemas import (
    ActivityDetailSchema,
    ActivityListItemSchema,
    ActivityListSchema,
    ActivityLogSchema,
)
from agentexec.activity.tracker import (
    cancel_pending,
    complete,
    count_active,
    create,
    detail,
    error,
    generate_agent_id,
    list,
    normalize_agent_id,
    update,
)

__all__ = [
    # Namespace for activity tracking
    "activity",
    # Models
    "Activity",
    "ActivityLog",
    "Status",
    # Schemas
    "ActivityLogSchema",
    "ActivityDetailSchema",
    "ActivityListItemSchema",
    "ActivityListSchema",
    # UUID Helpers
    "generate_agent_id",
    "normalize_agent_id",
    # Lifecycle API
    "create",
    "update",
    "complete",
    "error",
    "cancel_pending",
    # Query API
    "list",
    "detail",
    "count_active",
]

# Users manage their own database setup with SQLAlchemy
# See examples/fastapi-app/ for a complete example
