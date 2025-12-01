"""Unified logging for main and worker processes.

Uses multiprocessing's built-in logger which handles cross-process
logging correctly on macOS (spawn mode).
"""

import logging
import multiprocessing


def get_logger(name: str | None = None) -> logging.Logger:
    """Get the multiprocessing logger.

    Works in both main process and spawned worker subprocesses.
    Configures stderr output on first call in each process.

    Args:
        name: Optional logger name (included in log messages).

    Returns:
        Configured logger instance.

    Example:
        logger = get_logger(__name__)
        logger.info("Worker starting")
    """
    logger = multiprocessing.get_logger()

    # Only add handler if none exist (prevents duplicates)
    if not logger.handlers:
        multiprocessing.log_to_stderr(logging.INFO)

    return logger
