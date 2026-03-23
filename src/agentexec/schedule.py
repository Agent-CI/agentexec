"""Scheduled task support for agentexec.

Uses Redis sorted sets to track when tasks are due. The scheduler loop
runs automatically inside ``Pool.run()`` — no extra configuration needed.
It polls for due tasks, enqueues them into the normal task queue, and
reschedules repeating tasks based on their cron expression.

Clock-drift resilience: the next run time is always computed from the
*intended* run time (the anchor), not from wall-clock time at the moment
of rescheduling.  This prevents cumulative drift — if a task was supposed
to fire at 12:00 but the scheduler only picks it up at 12:02, the next
occurrence is still computed relative to 12:00.

Example:
    import agentexec as ax
    from agentexec.schedule import Schedule

    pool = ax.Pool(database_url="sqlite:///tasks.db")

    class RefreshContext(BaseModel):
        scope: str

    @pool.task("refresh_cache")
    async def refresh(agent_id: UUID, context: RefreshContext):
        ...

    # Run every 5 minutes, forever
    pool.schedule("refresh_cache", RefreshContext(scope="all"), Schedule(cron="*/5 * * * *"))

    # Run 3 more times then stop
    pool.schedule("refresh_cache", RefreshContext(scope="users"), Schedule(cron="0 * * * *", repeat=3))

    pool.run()  # scheduler loop runs automatically alongside workers
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from croniter import croniter
from pydantic import BaseModel, Field

from agentexec import state
from agentexec.config import CONF
from agentexec.core.logging import get_logger
from agentexec.core.queue import enqueue

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

REPEAT_FOREVER: int = -1


class Schedule(BaseModel):
    """How and when a task should repeat.

    Attributes:
        cron: Standard cron expression (5 fields: min hour dom mon dow).
            Examples: ``"*/5 * * * *"`` (every 5 min), ``"0 9 * * 1"`` (Mon 9 AM).
        repeat: How many *additional* executions remain after the current one.
            * ``-1`` — repeat forever (default).
            * ``0``  — one-shot; task is removed after a single execution.
            * ``N``  — run N more times, then remove.
    """

    cron: str
    repeat: int = REPEAT_FOREVER

    def next_after(self, anchor: float) -> float:
        """Compute the next run timestamp after *anchor*.

        Uses the cron expression to find the first occurrence strictly
        after the anchor, preventing clock-drift accumulation.

        Args:
            anchor: Unix timestamp to compute the next occurrence from.

        Returns:
            Unix timestamp of the next scheduled run.
        """
        dt = datetime.fromtimestamp(anchor, tz=timezone.utc)
        return float(croniter(self.cron, dt).get_next(float))


class ScheduledTask(BaseModel):
    """Persistent representation of a scheduled task stored in Redis.

    The ``schedule_id`` is the stable identity of this recurring job.
    Each time it fires, a fresh ``Task`` (with its own ``agent_id``) is
    enqueued for the worker pool.  The scheduled task itself stays in
    Redis until its repeat budget is exhausted.
    """

    schedule_id: str = Field(default_factory=lambda: str(uuid4()))
    task_name: str
    context: dict[str, Any]
    schedule: Schedule
    next_run: float
    created_at: float = Field(default_factory=lambda: time.time())
    metadata: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Redis key helpers
# ---------------------------------------------------------------------------


def _schedule_key(schedule_id: str) -> str:
    """Redis key for a schedule definition."""
    return state.backend.format_key(*state.KEY_SCHEDULE, schedule_id)


def _queue_key() -> str:
    """Redis sorted-set key that indexes schedules by next_run."""
    return state.backend.format_key(*state.KEY_SCHEDULE_QUEUE)


# ---------------------------------------------------------------------------
# Registration (called by Pool.schedule)
# ---------------------------------------------------------------------------


def register(
    task_name: str,
    context: BaseModel,
    schedule: Schedule,
    *,
    metadata: dict[str, Any] | None = None,
) -> str:
    """Register a new scheduled task in Redis.

    The task will first fire at the next cron occurrence from *now*.

    Args:
        task_name: Name of the registered task to enqueue on each tick.
        context: Pydantic context payload — serialized and stored alongside
            the schedule so it is available every time the task fires.
        schedule: ``Schedule`` describing cron expression and repeat budget.
        metadata: Optional metadata dict (e.g. for multi-tenancy).

    Returns:
        The ``schedule_id`` for this scheduled task.
    """
    now = time.time()
    next_run = schedule.next_after(now)

    st = ScheduledTask(
        task_name=task_name,
        context=context.model_dump(mode="json"),
        schedule=schedule,
        next_run=next_run,
        created_at=now,
        metadata=metadata,
    )

    # Store the full definition
    state.backend.set(
        _schedule_key(st.schedule_id),
        st.model_dump_json().encode(),
    )

    # Index by next_run for efficient polling
    state.backend.zadd(_queue_key(), {st.schedule_id: next_run})

    logger.info(
        f"Scheduled {task_name} as {st.schedule_id}, "
        f"next_run={datetime.fromtimestamp(next_run, tz=timezone.utc).isoformat()}"
    )

    return st.schedule_id


# ---------------------------------------------------------------------------
# Scheduler loop
# ---------------------------------------------------------------------------


async def tick() -> int:
    """Process all tasks that are due right now.

    For each due task:
    1. Deserialise the ``ScheduledTask`` from Redis.
    2. Enqueue it into the normal task queue (creating a fresh ``Task``).
    3. If repeats remain, compute the next anchor-based run time and
       update Redis.  Otherwise, remove the schedule.

    Returns:
        Number of tasks that were enqueued during this tick.
    """
    now = time.time()
    due_members = await state.backend.zrangebyscore(_queue_key(), 0, now)

    if not due_members:
        return 0

    enqueued = 0

    for raw_id in due_members:
        schedule_id = raw_id.decode("utf-8") if isinstance(raw_id, bytes) else raw_id

        data = state.backend.get(_schedule_key(schedule_id))
        if data is None:
            # Stale entry — clean it up
            state.backend.zrem(_queue_key(), schedule_id)
            continue

        st = ScheduledTask.model_validate_json(data)

        # Reconstruct the context as a plain BaseModel for enqueue().
        # The context is already a dict; wrap it in a generic model so
        # the downstream handler's TaskDefinition can re-validate it
        # into the correct concrete type.
        context_model = _dict_to_model(st.context)

        await enqueue(st.task_name, context_model, metadata=st.metadata)
        enqueued += 1

        logger.info(f"Scheduled task {st.task_name} ({schedule_id}) fired")

        # Decide whether to reschedule or remove
        if st.schedule.repeat == 0:
            # One-shot — done
            state.backend.zrem(_queue_key(), schedule_id)
            state.backend.delete(_schedule_key(schedule_id))
            logger.info(f"Schedule {schedule_id} exhausted, removed")
        else:
            # Compute next run from the *intended* time (clock-drift resilience)
            anchor = st.next_run
            next_run = st.schedule.next_after(anchor)

            # Update repeat count (-1 stays -1, positive values decrement)
            new_repeat = st.schedule.repeat if st.schedule.repeat < 0 else st.schedule.repeat - 1

            st.next_run = next_run
            st.schedule.repeat = new_repeat

            state.backend.set(
                _schedule_key(schedule_id),
                st.model_dump_json().encode(),
            )
            state.backend.zadd(_queue_key(), {schedule_id: next_run})

            logger.debug(
                f"Rescheduled {schedule_id}, next_run="
                f"{datetime.fromtimestamp(next_run, tz=timezone.utc).isoformat()}, "
                f"repeat={new_repeat}"
            )

    return enqueued


async def run_loop() -> None:
    """Run the scheduler loop indefinitely.

    Calls ``tick()`` at the configured poll interval
    (``CONF.scheduler_poll_interval``).  Called automatically by
    ``Pool.run()`` — you don't need to call this directly.

    Exits cleanly on ``asyncio.CancelledError``.
    """
    logger.info(
        f"Scheduler started, poll_interval={CONF.scheduler_poll_interval}s"
    )
    try:
        while True:
            await tick()
            await asyncio.sleep(CONF.scheduler_poll_interval)
    except asyncio.CancelledError:
        logger.info("Scheduler stopped")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


class _Payload(BaseModel):
    """Thin wrapper so a plain dict can travel through ``enqueue()``."""

    model_config = {"extra": "allow"}


def _dict_to_model(d: dict[str, Any]) -> BaseModel:
    """Wrap a dict in a BaseModel for enqueue compatibility."""
    return _Payload.model_validate(d)
