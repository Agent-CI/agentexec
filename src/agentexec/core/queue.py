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

    await backend.queue.push(
        queue_name or CONF.queue_name,
        task.model_dump_json(),
        high_priority=(priority == Priority.HIGH),
    )

    logger.info(f"Enqueued task {task.task_name} with agent_id {task.agent_id}")
    return task


async def dequeue(
    *,
    queue_name: str | None = None,
    timeout: int = 1,
) -> Task | None:
    """Dequeue a task from the queue. Returns raw Task (context is a dict)."""
    data = await backend.queue.pop(
        queue_name or CONF.queue_name,
        timeout=timeout,
    )
    return Task.model_validate(data) if data else None
