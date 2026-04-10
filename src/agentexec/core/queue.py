from enum import Enum
from typing import Any

from pydantic import BaseModel

from agentexec.core.logging import get_logger
from agentexec.core.task import Task
from agentexec.state import backend

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
    metadata: dict[str, Any] | None = None,
) -> Task:
    """Enqueue a task for background execution.

    Creates an activity record, serializes the context, and pushes the
    task to the queue for workers to process.

    Args:
        task_name: Name of the registered task (must match a ``@pool.task()``).
        context: Pydantic model with the task's input data.
        priority: ``Priority.HIGH`` pushes to the front of the queue.
        metadata: Optional dict attached to the activity record (e.g.
            ``{"organization_id": "org-123"}`` for multi-tenancy).

    Returns:
        The created Task with its ``agent_id`` for tracking.

    Example::

        task = await ax.enqueue("research", ResearchContext(company="Acme"))
        print(task.agent_id)  # UUID for tracking
    """
    task = await Task.create(
        task_name=task_name,
        context=context,
        metadata=metadata,
    )

    await backend.queue.push(
        task.model_dump_json(),
        priority=priority,
    )

    logger.info(f"Enqueued task {task.task_name} with agent_id {task.agent_id}")
    return task


