from enum import Enum


class Status(str, Enum):
    """Agent execution status."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETE = "complete"
    ERROR = "error"
    CANCELED = "canceled"
