from __future__ import annotations

import time
from datetime import datetime
from typing import Any
from croniter import croniter
from pydantic import BaseModel, Field, ValidationError

from agentexec.config import CONF
from agentexec.core.logging import get_logger
from agentexec.core.queue import enqueue
from agentexec.state import ops

logger = get_logger(__name__)

__all__ = [
    "register",
    "tick",
]

REPEAT_FOREVER: int = -1


class ScheduledTask(BaseModel):
    """A task scheduled to run on a recurring interval.

    Stored in the backend with a sorted-set index for efficient due-time
    polling. Each time it fires, a fresh Task (with its own agent_id) is
    enqueued for the worker pool.
    """

    task_name: str
    context: bytes
    cron: str
    repeat: int = REPEAT_FOREVER
    next_run: float = 0
    created_at: float = Field(default_factory=lambda: time.time())
    metadata: dict[str, Any] | None = None

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
        """Compute the next cron occurrence after anchor."""
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
        context=ops.serialize(context),
        cron=every,
        repeat=repeat,
        metadata=metadata,
    )

    await ops.schedule_set(task_name, task.model_dump_json().encode())
    await ops.schedule_index_add(task_name, task.next_run)
    logger.info(f"Scheduled {task_name}")


async def tick() -> None:
    """Process all scheduled tasks that are due right now.

    For each due task, enqueues it into the normal task queue. If repeats
    remain, advances to the next run time. Otherwise removes the schedule.
    """
    for task_name in await ops.schedule_index_due(time.time()):
        try:
            data = await ops.schedule_get(task_name)
            task = ScheduledTask.model_validate_json(data)
        except (ValidationError, TypeError):
            logger.warning(f"Failed to load schedule {task_name}, skipping")
            continue

        await enqueue(
            task.task_name,
            context=ops.deserialize(task.context),
            metadata=task.metadata,
        )

        if task.repeat == 0:
            await ops.schedule_index_remove(task_name)
            await ops.schedule_delete(task_name)
            logger.info(f"Schedule for '{task_name}' exhausted")
        else:
            task.advance()
            await ops.schedule_set(task_name, task.model_dump_json().encode())
            await ops.schedule_index_add(task_name, task.next_run)
