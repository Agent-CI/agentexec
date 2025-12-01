"""Task queue operations using Redis."""

import json
from enum import Enum
from typing import Any

from pydantic import BaseModel

from agentexec.config import CONF
from agentexec.core.logging import get_logger
from agentexec.core.redis_client import get_redis
from agentexec.core.task import Task

logger = get_logger(__name__)


class Priority(str, Enum):
    """Task priority levels.

    HIGH: Push to front of queue (processed first).
    LOW: Push to back of queue (processed later).
    """

    HIGH = "high"
    LOW = "low"


async def enqueue(
    task_name: str,
    context: BaseModel,
    *,
    priority: Priority = Priority.LOW,
    queue_name: str | None = None,
) -> Task:
    """Enqueue a task for background execution.

    Pushes the task to Redis for worker processing. The task must be
    registered with a WorkerPool via @pool.task() decorator.

    Args:
        task_name: Name of the task to execute.
        context: Task context as a Pydantic BaseModel.
        priority: Task priority (Priority.HIGH or Priority.LOW).
        queue_name: Redis queue name. Defaults to CONF.queue_name.

    Returns:
        Task instance with typed context and agent_id for tracking.

    Example:
        @pool.task("research_company")
        async def research(agent_id: UUID, context: ResearchContext):
            ...

        task = await ax.enqueue("research_company", ResearchContext(company="Acme"))
    """
    redis = get_redis()
    redis_push = {
        Priority.HIGH: redis.rpush,
        Priority.LOW: redis.lpush,
    }[priority]

    task = Task.create(
        task_name=task_name,
        context=context,
    )
    await redis_push(  # type: ignore[misc]
        queue_name or CONF.queue_name,
        task.model_dump_json(),
    )

    logger.info(f"Enqueued task {task.task_name} with agent_id {task.agent_id}")
    return task


async def dequeue(
    queue_name: str | None = None,
    timeout: int = 1,
) -> dict[str, Any] | None:
    """Dequeue a task from the queue.

    Blocks for up to timeout seconds waiting for a task.

    Args:
        queue_name: Redis queue name. Defaults to CONF.queue_name.
        timeout: Maximum seconds to wait for a task.

    Returns:
        Parsed task data if available, None otherwise.
    """
    redis = get_redis()
    result = await redis.brpop(  # type: ignore[misc]
        [queue_name or CONF.queue_name],
        timeout=timeout,
    )

    if result is None:
        return None

    _, task_bytes = result
    data: dict[str, Any] = json.loads(task_bytes.decode("utf-8"))
    return data
