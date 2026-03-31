from __future__ import annotations
import logging
import multiprocessing as mp
from pydantic import BaseModel

LOGGER_NAME = "agentexec"
LOG_CHANNEL = "agentexec:logs"
DEFAULT_FORMAT = "[%(levelname)s/%(processName)s] %(name)s: %(message)s"


class LogMessage(BaseModel):
    """Schema for log messages sent via the worker message queue."""

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


class QueueLogHandler(logging.Handler):
    """Logging handler that sends log records to the pool via multiprocessing queue."""

    def __init__(self, tx: mp.Queue):
        super().__init__()
        self.tx = tx

    def emit(self, record: logging.LogRecord) -> None:
        try:
            from agentexec.worker.pool import LogEntry
            message = LogMessage.from_log_record(record)
            self.tx.put_nowait(LogEntry(record=message))
        except Exception:
            self.handleError(record)


_worker_logging_configured = False


def get_worker_logger(name: str, tx: mp.Queue | None = None) -> logging.Logger:
    """Configure worker logging and return a logger."""
    global _worker_logging_configured

    if not _worker_logging_configured and tx is not None:
        root = logging.getLogger(LOGGER_NAME)
        root.setLevel(logging.INFO)
        root.addHandler(QueueLogHandler(tx))
        root.propagate = False
        _worker_logging_configured = True

    if name.startswith(LOGGER_NAME):
        return logging.getLogger(name)
    return logging.getLogger(f"{LOGGER_NAME}.{name}")
