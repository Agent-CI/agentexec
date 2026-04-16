"""agentexec CLI — manage worker pools from the command line.

Usage:
    agentexec run myapp.worker:pool
    agentexec run myapp.worker:pool --workers 4
    agentexec run myapp.worker:pool --create-tables
"""

import argparse
import asyncio
import importlib
import logging
import os
import signal
import sys

logger = logging.getLogger(__name__)


def _import_pool(path: str):
    """Import a Pool instance from a dotted module path.

    Args:
        path: ``module.path:attribute`` or ``module.path`` (defaults to ``pool``).

    Returns:
        The Pool instance.
    """
    from agentexec.worker.pool import Pool

    if ":" in path:
        module_path, attr_name = path.rsplit(":", 1)
    else:
        module_path = path
        attr_name = "pool"

    # Ensure cwd is on the path so user modules are importable
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        print(f"Error: could not import module '{module_path}': {e}", file=sys.stderr)
        sys.exit(1)

    try:
        pool = getattr(module, attr_name)
    except AttributeError:
        print(f"Error: module '{module_path}' has no attribute '{attr_name}'", file=sys.stderr)
        sys.exit(1)

    if not isinstance(pool, Pool):
        print(f"Error: '{module_path}:{attr_name}' is not a Pool instance", file=sys.stderr)
        sys.exit(1)

    return pool


async def _create_tables(pool):
    from agentexec.core.db import Base

    async with pool._engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def run(args):
    """Run a worker pool."""
    pool = _import_pool(args.pool)

    from agentexec.config import CONF

    if args.workers is not None:
        CONF.num_workers = args.workers
    if args.max_retries is not None:
        CONF.max_task_retries = args.max_retries
    if args.shutdown_timeout is not None:
        CONF.graceful_shutdown_timeout = args.shutdown_timeout
    if args.scheduler_timezone is not None:
        CONF.scheduler_timezone = args.scheduler_timezone

    if args.create_tables:
        await _create_tables(pool)

    loop = asyncio.get_running_loop()
    start_task = asyncio.ensure_future(pool.start())
    # SIGTERM (systemd/docker stop) doesn't raise in Python by default; cancel explicitly.
    loop.add_signal_handler(signal.SIGTERM, start_task.cancel)

    try:
        await start_task
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass


def main():
    parser = argparse.ArgumentParser(
        prog="agentexec",
        description="agentexec — background task orchestration.",
    )
    subparsers = parser.add_subparsers(dest="command")

    # agentexec run
    run_parser = subparsers.add_parser(
        "run",
        help="Start a worker pool",
        description="Import a Pool instance and start processing tasks.",
    )
    run_parser.add_argument(
        "pool",
        help="Pool instance to run, e.g. 'myapp.worker:pool'",
    )
    run_parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=None,
        help="Number of worker processes (default: AGENTEXEC_NUM_WORKERS or 4)",
    )
    run_parser.add_argument(
        "--create-tables",
        action="store_true",
        help="Create database tables before starting",
    )
    run_parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO)",
    )
    run_parser.add_argument(
        "--max-retries",
        type=int,
        default=None,
        help="Max task retries before giving up (default: 3)",
    )
    run_parser.add_argument(
        "--shutdown-timeout",
        type=int,
        default=None,
        help="Seconds to wait for workers on shutdown (default: 300)",
    )
    run_parser.add_argument(
        "--scheduler-timezone",
        default=None,
        help="IANA timezone for cron schedules (default: UTC)",
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="[%(levelname)s/%(processName)s] %(name)s: %(message)s",
    )

    if args.command == "run":
        asyncio.run(run(args))
