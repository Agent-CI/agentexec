from __future__ import annotations

import time
from datetime import datetime
from typing import Any
from uuid import uuid4

from croniter import croniter
from pydantic import BaseModel, Field, ValidationError

from agentexec import state
from agentexec.config import CONF
from agentexec.core.logging import get_logger
from agentexec.core.queue import enqueue

logger = get_logger(__name__)

__all__ = [
    "register",
    "tick",
]

REPEAT_FOREVER: int = -1


class ScheduledTask(BaseModel):
    """A task scheduled to run on a recurring interval.

    Stored in Redis with a sorted-set index for efficient due-time polling.
    Each time it fires, a fresh Task (with its own agent_id) is enqueued
    for the worker pool. Stays in Redis until its repeat budget is exhausted.
    """

    schedule_id: str = Field(default_factory=lambda: str(uuid4()))
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
        """Advance to the next scheduled run and decrement the repeat count.

        Computes the next run time from the current anchor (not wall clock)
        to prevent clock-drift accumulation. Decrements repeat for finite
        schedules; infinite (-1) stays unchanged.
        """
        self.next_run = self._next_after(self.next_run)
        if self.repeat >= 0:
            self.repeat -= 1

    def _next_after(self, anchor: float) -> float:
        """Compute the next cron occurrence after anchor."""
        dt = datetime.fromtimestamp(anchor, tz=CONF.scheduler_tz)
        return float(croniter(self.cron, dt).get_next(float))


def _schedule_key(schedule_id: str) -> str:
    """Redis key for a schedule definition."""
    return state.backend.format_key(*state.KEY_SCHEDULE, schedule_id)


def _queue_key() -> str:
    """Redis sorted-set key that indexes schedules by next_run."""
    return state.backend.format_key(*state.KEY_SCHEDULE_QUEUE)


def register(
    task_name: str,
    every: str,
    context: BaseModel,
    *,
    repeat: int = REPEAT_FOREVER,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Register a new scheduled task in Redis.

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
        context=state.backend.serialize(context),
        cron=every,
        repeat=repeat,
        metadata=metadata,
    )

    state.backend.set(
        _schedule_key(task.schedule_id),
        task.model_dump_json().encode(),
    )
    state.backend.zadd(_queue_key(), {task.schedule_id: task.next_run})
    logger.info(f"Scheduled {task_name} ({task.schedule_id})")


async def tick() -> None:
    """Process all scheduled tasks that are due right now.

    For each due task, enqueues it into the normal task queue. If repeats
    remain, advances to the next run time. Otherwise removes the schedule.
    """
    schedule_ids = await state.backend.zrangebyscore(_queue_key(), 0, time.time())
    for _schedule_id in schedule_ids:
        schedule_id = _schedule_id.decode("utf-8")

        try:
            data = state.backend.get(_schedule_key(schedule_id))
            task = ScheduledTask.model_validate_json(data)
        except ValidationError:
            logger.warning(f"Failed to load schedule {schedule_id}, skipping")
            continue

        await enqueue(
            task.task_name,
            context=state.backend.deserialize(task.context),
            metadata=task.metadata,
        )

        if task.repeat == 0:
            state.backend.zrem(_queue_key(), schedule_id)
            state.backend.delete(_schedule_key(schedule_id))
            logger.info(f"Task '{task.task_name}' schedule exhausted")
        else:
            task.advance()
            state.backend.set(
                _schedule_key(schedule_id),
                task.model_dump_json().encode(),
            )
            state.backend.zadd(_queue_key(), {
                schedule_id: task.next_run,
            })
