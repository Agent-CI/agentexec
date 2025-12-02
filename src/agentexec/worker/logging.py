from __future__ import annotations
import logging
from pydantic import BaseModel
from agentexec import state

LOGGER_NAME = "agentexec"
LOG_CHANNEL = "agentexec:logs"
DEFAULT_FORMAT = "[%(levelname)s/%(processName)s] %(name)s: %(message)s"


class LogMessage(BaseModel):
    """Schema for log messages sent via state backend pubsub."""

    name: str
    levelno: int
    levelname: str
    msg: str
    processName: str
    process: int | None
    thread: int | None
    created: float

    @classmethod
    def from_log_record(cls, record: logging.LogRecord) -> LogMessage:
        """Create a LogMessage from a logging.LogRecord."""
        return cls(
            name=record.name,
            levelno=record.levelno,
            levelname=record.levelname,
            msg=record.getMessage(),
            processName=record.processName,
            process=record.process,
            thread=record.thread,
            created=record.created,
        )

    def to_log_record(self) -> logging.LogRecord:
        """Convert back to a logging.LogRecord."""
        record = logging.LogRecord(
            name=self.name,
            level=self.levelno,
            pathname="",
            lineno=0,
            msg=self.msg,
            args=(),
            exc_info=None,
        )
        record.processName = self.processName
        record.process = self.process
        record.created = self.created
        return record


class StateLogHandler(logging.Handler):
    """Logging handler that publishes log records to state backend pubsub.

    Used by worker processes to send logs to the main process.
    """

    def __init__(self, channel: str = LOG_CHANNEL):
        super().__init__()
        self.channel = channel

    def emit(self, record: logging.LogRecord) -> None:
        """Publish log record to log channel."""
        try:
            message = LogMessage.from_log_record(record)
            state.publish_log(message.model_dump_json())
        except Exception:
            self.handleError(record)


_worker_logging_configured = False


def get_worker_logger(name: str) -> logging.Logger:
    """Configure worker logging and return a logger.

    On first call, sets up a state handler that publishes log records
    to the main process via state backend pubsub. Subsequent calls just return
    a logger under the agentexec namespace.

    Args:
        name: Logger name. Typically __name__.

    Returns:
        Configured logger instance.

    Example:
        logger = get_worker_logger(__name__)
        logger.info("Worker starting")
    """
    global _worker_logging_configured

    if not _worker_logging_configured:
        root = logging.getLogger(LOGGER_NAME)
        root.setLevel(logging.INFO)
        root.addHandler(StateLogHandler())
        root.propagate = False
        _worker_logging_configured = True

    if name.startswith(LOGGER_NAME):
        return logging.getLogger(name)
    return logging.getLogger(f"{LOGGER_NAME}.{name}")
