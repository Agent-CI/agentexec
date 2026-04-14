from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from croniter import croniter
from pydantic import BaseModel, Field

from agentexec.config import CONF
from agentexec.state import backend

logger = logging.getLogger(__name__)

__all__ = [
    "register",
]

REPEAT_FOREVER: int = -1


class ScheduledTask(BaseModel):
    """A task scheduled to run on a recurring interval.

    Stored in the schedule backend with a time index for efficient
    due-time polling. Each time it fires, a fresh Task (with its own
    agent_id) is enqueued for the worker pool. Stays registered until
    its repeat budget is exhausted.
    """

    task_name: str
    context: bytes
    cron: str
    repeat: int = REPEAT_FOREVER
    next_run: float = 0
    created_at: float = Field(default_factory=lambda: time.time())
    metadata: dict[str, Any] | None = None

    @property
    def key(self) -> str:
        """Unique identity: task_name + cron + context hash."""
        import hashlib
        context_hash = hashlib.md5(self.context).hexdigest()[:8]
        return f"{self.task_name}:{self.cron}:{context_hash}"

    def model_post_init(self, __context: Any) -> None:
        """Compute next_run from cron if not explicitly set."""
        if self.next_run == 0:
            self.next_run = self._next_after(self.created_at)

    def advance(self) -> None:
        """Advance next_run to the next future cron occurrence.

        Skips past any missed intervals so we don't enqueue a burst of
        catch-up tasks after downtime. Decrements repeat for each skipped
        interval (finite schedules only; -1 stays unchanged).
        """
        now = time.time()
        while True:
            self.next_run = self._next_after(self.next_run)
            if self.repeat >= 0:
                self.repeat -= 1
            if self.next_run > now or self.repeat == 0:
                break

    def _next_after(self, anchor: float) -> float:
        """Compute the next cron occurrence after the given anchor time."""
        dt = datetime.fromtimestamp(anchor, tz=CONF.scheduler_tz)
        return float(croniter(self.cron, dt).get_next(float))


async def register(
    task_name: str,
    every: str,
    context: BaseModel,
    *,
    repeat: int = REPEAT_FOREVER,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Register a new scheduled task.

    The task will first fire at the next cron occurrence from now.

    Args:
        task_name: Name of the registered task to enqueue on each tick.
        every: Schedule expression (cron syntax: min hour dom mon dow).
        context: Pydantic context payload passed to the handler each time.
        repeat: How many additional executions after the first.
            -1 = forever (default), 0 = one-shot, N = N more times.
        metadata: Optional metadata dict (e.g. for multi-tenancy).
    """
    task = ScheduledTask(
        task_name=task_name,
        context=backend.serialize(context),
        cron=every,
        repeat=repeat,
        metadata=metadata,
    )
    await backend.schedule.register(task)


