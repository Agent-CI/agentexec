from __future__ import annotations

import time
from datetime import datetime
from typing import Any
from croniter import croniter
from pydantic import BaseModel, Field

from agentexec.config import CONF
from agentexec.core.logging import get_logger
from agentexec.state import backend

logger = get_logger(__name__)

__all__ = [
    "register",
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

    @property
    def key(self) -> str:
        """Unique identity: task_name + cron + context hash."""
        import hashlib
        context_hash = hashlib.md5(self.context).hexdigest()[:8]
        return f"{self.task_name}:{self.cron}:{context_hash}"

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
    await backend.schedule.register(task)
    logger.info(f"Scheduled {task_name}")


