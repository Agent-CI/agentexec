import json
from enum import Enum
from typing import Any

from pydantic import BaseModel

from agentexec.config import CONF
from agentexec.core.logging import get_logger
from agentexec.core.task import Task
from agentexec.state import backend

logger = get_logger(__name__)


class Priority(str, Enum):
    HIGH = "high"
    LOW = "low"


async def enqueue(
    task_name: str,
    context: BaseModel,
    *,
    priority: Priority = Priority.LOW,
    queue_name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> Task:
    """Enqueue a task for background execution."""
    task = await Task.create(
        task_name=task_name,
        context=context,
        metadata=metadata,
    )

    partition_key = None
    if task._definition is not None:
        partition_key = task.get_lock_key()

    await backend.queue.push(
        queue_name or CONF.queue_name,
        task.model_dump_json(),
        high_priority=(priority == Priority.HIGH),
        partition_key=partition_key,
    )

    logger.info(f"Enqueued task {task.task_name} with agent_id {task.agent_id}")
    return task


async def requeue(
    task: Task,
    *,
    queue_name: str | None = None,
) -> None:
    """Push a task back to the end of the queue."""
    await backend.queue.push(
        queue_name or CONF.queue_name,
        task.model_dump_json(),
        high_priority=False,
    )


async def dequeue(
    tasks: dict[str, Any],
    *,
    queue_name: str | None = None,
    timeout: int = 1,
) -> Task | None:
    """Dequeue and hydrate a task from the queue."""
    data = await backend.queue.pop(
        queue_name or CONF.queue_name,
        timeout=timeout,
    )
    if data is None:
        return None

    return Task.from_serialized(
        definition=tasks[data["task_name"]],
        data=data,
    )
