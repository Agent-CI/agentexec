import logging
from enum import Enum
from typing import Any, cast

from agentexec.config import CONF
from agentexec.core.redis_client import get_redis
from agentexec.core.task import Task

logger = logging.getLogger(__name__)


class Priority(str, Enum):
    """Task priority levels.

    HIGH: Push to front of queue (processed first).
    LOW: Push to back of queue (processed later).
    """

    HIGH = "high"
    LOW = "low"


def enqueue(
    task_name: str,
    payload: dict[str, Any],
    *,
    priority: Priority = Priority.LOW,
    queue_name: str | None = None,
) -> Task:
    """Enqueue a task for background execution with automatic activity tracking.

    This function automatically creates an activity record for the task and
    enqueues it to Redis for worker processing.

    Args:
        task: Task to enqueue.
        db: Database session for activity tracking.
        priority: Task priority (Priority.HIGH or Priority.LOW).
        queue_name: Redis list name to use. If not provided, uses
                   agentexec.CONF.queue_name.

    Returns:
        agent_id: UUID for tracking the task's progress.

    Example:
        task = Task(task_type="research", payload={"company": "Acme"})
        agent_id = enqueue(task, db=session, priority=Priority.HIGH)
        # Track progress: ax.activity.get_activity(db, agent_id)
    """

    try:
        redis = get_redis()
        redis_push = {
            Priority.HIGH: redis.rpush,
            Priority.LOW: redis.lpush,
        }[priority]
    except KeyError:
        raise ValueError(f"Invalid priority: {priority}. Must be Priority.HIGH or Priority.LOW")

    task = Task.create(
        task_name=task_name,
        payload=payload,
    )
    redis_push(
        queue_name or CONF.queue_name,
        task.model_dump_json(),
    )
    logger.info(f"Enqueued task {task.task_name} with agent_id {task.agent_id}")
    # TODO track errors

    return task


def dequeue(queue_name: str | None = None, timeout: int = 1) -> Task | None:
    """Dequeue a task from the queue.

    Blocks for up to timeout seconds waiting for a task. Returns None if
    no task is available within the timeout period.

    Args:
        queue_name: Redis list name to use. If not provided, uses
                   agentexec.CONF.queue_name.
        timeout: Maximum seconds to wait for a task.

    Returns:
        Task if available, None otherwise.

    Example:
        task = dequeue(timeout=5)
        if task is not None:
            # Process task
            pass
    """
    redis = get_redis()

    result = cast(
        tuple[str, str] | None,
        redis.brpop(
            [queue_name or CONF.queue_name],
            timeout=timeout,
        ),
    )

    if result is None:
        # No task available within the timeout
        return None

    _, task_json = result
    return Task.model_validate_json(task_json)
