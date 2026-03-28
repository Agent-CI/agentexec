from __future__ import annotations
import asyncio
import logging
from pydantic import BaseModel
from agentexec.state import backend

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
        return cls(
            name=record.name,
            levelno=record.levelno,
            levelname=record.levelname,
            msg=record.getMessage(),
            processName=record.processName or "MainProcess",
            process=record.process,
            thread=record.thread,
            created=record.created,
        )

    def to_log_record(self) -> logging.LogRecord:
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
    """Logging handler that publishes log records to state backend pubsub."""

    def __init__(self, channel: str = LOG_CHANNEL):
        super().__init__()
        self.channel = channel

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = LogMessage.from_log_record(record)
            loop = asyncio.get_running_loop()
            loop.create_task(backend.state.log_publish(message.model_dump_json()))
        except RuntimeError:
            pass  # No running loop — discard silently
        except Exception:
            self.handleError(record)


_worker_logging_configured = False


def get_worker_logger(name: str) -> logging.Logger:
    """Configure worker logging and return a logger."""
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
