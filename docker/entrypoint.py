#!/usr/bin/env python3
"""Worker entrypoint for agentexec Docker container.

This script dynamically imports the user's worker module and starts the pool.
The module must expose a `pool` variable (Pool instance) or a
`create_pool()` function that returns one.

Environment Variables:
    AGENTEXEC_WORKER_MODULE: Python module path containing the pool
                             (e.g., "myapp.worker" or "tasks")
    DATABASE_URL: Database connection URL (required)
    REDIS_URL: Redis connection URL (required)

Example:
    AGENTEXEC_WORKER_MODULE=myapp.worker python -m docker.entrypoint
"""

import importlib
import os
import sys


def get_pool():
    """Import user module and extract the Pool instance."""
    module_path = os.environ.get("AGENTEXEC_WORKER_MODULE")

    if not module_path:
        print("Error: AGENTEXEC_WORKER_MODULE environment variable is required")
        print("Set it to your Python module containing the Pool instance")
        print("Example: AGENTEXEC_WORKER_MODULE=myapp.worker")
        sys.exit(1)

    # Validate required configuration
    if not os.environ.get("DATABASE_URL"):
        print("Error: DATABASE_URL environment variable is required")
        sys.exit(1)

    if not os.environ.get("REDIS_URL") and not os.environ.get("AGENTEXEC_REDIS_URL"):
        print("Error: REDIS_URL environment variable is required")
        sys.exit(1)

    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        print(f"Error: Could not import module '{module_path}': {e}")
        print("Make sure your code is in the Python path (PYTHONPATH or /app)")
        sys.exit(1)

    # Try to get pool from module
    if hasattr(module, "pool"):
        pool = module.pool
    elif hasattr(module, "create_pool"):
        pool = module.create_pool()
    else:
        print(f"Error: Module '{module_path}' must expose 'pool' or 'create_pool()'")
        print("Example:")
        print("  pool = ax.Pool(database_url=os.environ['DATABASE_URL'])")
        sys.exit(1)

    return pool


def main():
    """Main entrypoint - start the worker pool."""
    import agentexec as ax

    pool = get_pool()

    print("Starting agentexec worker pool...")
    print(f"Workers: {ax.CONF.num_workers}")
    print(f"Queue: {ax.CONF.queue_name}")
    print("Press Ctrl+C to shutdown gracefully")

    pool.run()


if __name__ == "__main__":
    main()
