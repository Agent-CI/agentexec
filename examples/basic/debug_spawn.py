"""Debug: patch Worker to print tasks dict on startup, then run via Pool.run()."""
import sys
import os
import logging

sys.path.insert(0, os.getcwd())

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s/%(processName)s] %(name)s: %(message)s",
)

from worker import pool
from agentexec.worker.pool import Worker

_orig_init = Worker.__init__

def _debug_init(self, worker_id, context):
    _orig_init(self, worker_id, context)
    print(f"[DEBUG] Worker {worker_id} context.tasks = {list(context.tasks.keys())}", flush=True)
    print(f"[DEBUG] Worker {worker_id} sys.path[:5] = {sys.path[:5]}", flush=True)

Worker.__init__ = _debug_init

from agentexec.config import CONF
CONF.num_workers = 1

if __name__ == "__main__":
    print(f"[DEBUG] Parent tasks = {list(pool._context.tasks.keys())}")
    pool.run()
