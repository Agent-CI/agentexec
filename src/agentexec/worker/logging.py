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
    exc_text: str | None = None

    @classmethod
    def from_log_record(cls, record: logging.LogRecord) -> LogMessage:
        exc_text = None
        if record.exc_info and record.exc_info[1] is not None:
            import traceback
            exc_text = "".join(traceback.format_exception(*record.exc_info))

        return cls(
            name=record.name,
            levelno=record.levelno,
            levelname=record.levelname,
            msg=record.getMessage(),
            processName=record.processName or "MainProcess",
            process=record.process,
            thread=record.thread,
            created=record.created,
            exc_text=exc_text,
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
        record.exc_text = self.exc_text
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
    """Configure worker logging and return a logger.

    On first call with a tx queue, attaches a QueueLogHandler to the root
    logger so all worker logs (including third-party libraries) are
    forwarded to the pool process via IPC.
    """
    global _worker_logging_configured

    if not _worker_logging_configured and tx is not None:
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        root.addHandler(QueueLogHandler(tx))
        _worker_logging_configured = True

    return logging.getLogger(name)
