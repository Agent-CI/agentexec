from __future__ import annotations

import time
from datetime import datetime
from typing import Any
from croniter import croniter
from pydantic import BaseModel, Field, ValidationError

from agentexec.config import CONF
from agentexec.core.logging import get_logger
from agentexec.core.queue import enqueue
from agentexec.state import KEY_SCHEDULE, KEY_SCHEDULE_QUEUE, backend

logger = get_logger(__name__)

__all__ = [
    "register",
    "tick",
]

REPEAT_FOREVER: int = -1


class ScheduledTask(BaseModel):
    """A task scheduled to run on a recurring interval."""

    task_name: str
    context: bytes
    cron: str
    repeat: int = REPEAT_FOREVER
    next_run: float = 0
    created_at: float = Field(default_factory=lambda: time.time())
    metadata: dict[str, Any] | None = None

    def model_post_init(self, __context: Any) -> None:
        if self.next_run == 0:
            self.next_run = self._next_after(self.created_at)

    def advance(self) -> None:
        now = time.time()
        while True:
            self.next_run = self._next_after(self.next_run)
            if self.repeat >= 0:
                self.repeat -= 1
            if self.next_run > now or self.repeat == 0:
                break

    def _next_after(self, anchor: float) -> float:
        dt = datetime.fromtimestamp(anchor, tz=CONF.scheduler_tz)
        return float(croniter(self.cron, dt).get_next(float))


def _schedule_key(task_name: str) -> str:
    return backend.format_key(*KEY_SCHEDULE, task_name)


def _queue_key() -> str:
    return backend.format_key(*KEY_SCHEDULE_QUEUE)


async def register(
    task_name: str,
    every: str,
    context: BaseModel,
    *,
    repeat: int = REPEAT_FOREVER,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Register a new scheduled task."""
    task = ScheduledTask(
        task_name=task_name,
        context=backend.serialize(context),
        cron=every,
        repeat=repeat,
        metadata=metadata,
    )

    await backend.state.set(_schedule_key(task_name), task.model_dump_json().encode())
    await backend.state.index_add(_queue_key(), {task_name: task.next_run})
    logger.info(f"Scheduled {task_name}")


async def tick() -> None:
    """Process all scheduled tasks that are due right now."""
    raw = await backend.state.index_range(_queue_key(), 0, time.time())
    due_names = [item.decode("utf-8") for item in raw]

    for task_name in due_names:
        try:
            data = await backend.state.get(_schedule_key(task_name))
            task = ScheduledTask.model_validate_json(data)
        except (ValidationError, TypeError):
            logger.warning(f"Failed to load schedule {task_name}, skipping")
            continue

        await enqueue(
            task.task_name,
            context=backend.deserialize(task.context),
            metadata=task.metadata,
        )

        if task.repeat == 0:
            await backend.state.index_remove(_queue_key(), task_name)
            await backend.state.delete(_schedule_key(task_name))
            logger.info(f"Schedule for '{task_name}' exhausted")
        else:
            task.advance()
            await backend.state.set(_schedule_key(task_name), task.model_dump_json().encode())
            await backend.state.index_add(_queue_key(), {task_name: task.next_run})
