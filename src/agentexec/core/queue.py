import json
from enum import Enum
from typing import Any

from pydantic import BaseModel

from agentexec.config import CONF
from agentexec.core.logging import get_logger
from agentexec.core.task import Task
from agentexec.state import ops

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
    metadata: dict[str, Any] | None = None,
) -> Task:
    """Enqueue a task for background execution.

    Pushes the task to the queue for worker processing. The task must be
    registered with a WorkerPool via @pool.task() decorator.

    Args:
        task_name: Name of the task to execute.
        context: Task context as a Pydantic BaseModel.
        priority: Task priority (Priority.HIGH or Priority.LOW).
        queue_name: Queue name. Defaults to CONF.queue_name.
        metadata: Optional dict of arbitrary metadata to attach to the activity.
            Useful for multi-tenancy (e.g., {"organization_id": "org-123"}).

    Returns:
        Task instance with typed context and agent_id for tracking.

    Example:
        @pool.task("research_company")
        async def research(agent_id: UUID, context: ResearchContext):
            ...

        task = await ax.enqueue("research_company", ResearchContext(company="Acme"))

        # With metadata for multi-tenancy
        task = await ax.enqueue(
            "research_company",
            ResearchContext(company="Acme"),
            metadata={"organization_id": "org-123"}
        )
    """
    task = Task.create(
        task_name=task_name,
        context=context,
        metadata=metadata,
    )

    # For stream backends, the partition_key is derived from the task's
    # lock_key template if the task has one. This ensures all tasks for
    # the same lock scope land on the same partition.
    partition_key = None
    if task._definition is not None:
        partition_key = task.get_lock_key()

    ops.queue_push(
        queue_name or CONF.queue_name,
        task.model_dump_json(),
        high_priority=(priority == Priority.HIGH),
        partition_key=partition_key,
    )

    logger.info(f"Enqueued task {task.task_name} with agent_id {task.agent_id}")
    return task


def requeue(
    task: Task,
    *,
    queue_name: str | None = None,
) -> None:
    """Push a task back to the end of the queue.

    Used when a task's lock cannot be acquired — the task is returned to the
    queue so it can be retried after the lock is released.

    Args:
        task: Task to requeue.
        queue_name: Queue name. Defaults to CONF.queue_name.
    """
    ops.queue_push(
        queue_name or CONF.queue_name,
        task.model_dump_json(),
        high_priority=False,
    )


async def dequeue(
    *,
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
    return await ops.queue_pop(
        queue_name or CONF.queue_name,
        timeout=timeout,
    )
