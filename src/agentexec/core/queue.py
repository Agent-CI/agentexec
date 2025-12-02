import json
from enum import Enum
from typing import Any

from pydantic import BaseModel

from agentexec import state
from agentexec.config import CONF
from agentexec.core.logging import get_logger
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

    Pushes the task to the queue for worker processing. The task must be
    registered with a WorkerPool via @pool.task() decorator.

    Args:
        task_name: Name of the task to execute.
        context: Task context as a Pydantic BaseModel.
        priority: Task priority (Priority.HIGH or Priority.LOW).
        queue_name: Queue name. Defaults to CONF.queue_name.

    Returns:
        Task instance with typed context and agent_id for tracking.

    Example:
        @pool.task("research_company")
        async def research(agent_id: UUID, context: ResearchContext):
            ...

        task = await ax.enqueue("research_company", ResearchContext(company="Acme"))
    """
    push_func = {
        Priority.HIGH: state.backend.rpush,
        Priority.LOW: state.backend.lpush,
    }[priority]

    task = Task.create(
        task_name=task_name,
        context=context,
    )
    push_func(
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
        queue_name: Queue name. Defaults to CONF.queue_name.
        timeout: Maximum seconds to wait for a task.

    Returns:
        Parsed task data if available, None otherwise.
    """
    result = await state.backend.brpop(
        queue_name or CONF.queue_name,
        timeout=timeout,
    )

    if result is None:
        return None

    _, task_data = result
    data: dict[str, Any] = json.loads(task_data)
    return data
