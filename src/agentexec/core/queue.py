from enum import Enum
from typing import Any

from pydantic import BaseModel

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
    metadata: dict[str, Any] | None = None,
) -> Task:
    """Enqueue a task for background execution."""
    task = await Task.create(
        task_name=task_name,
        context=context,
        metadata=metadata,
    )

    await backend.queue.push(
        task.model_dump_json(),
        high_priority=(priority == Priority.HIGH),
    )

    logger.info(f"Enqueued task {task.task_name} with agent_id {task.agent_id}")
    return task


