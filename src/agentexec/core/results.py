from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import BaseModel

from agentexec.state import KEY_RESULT, backend

if TYPE_CHECKING:
    from agentexec.core.task import Task


DEFAULT_TIMEOUT: int = 300  # TODO improve this polling approach


class TaskFailure(BaseModel):
    """Terminal failure record stored at the result key when retries are exhausted."""

    task_name: str
    agent_id: UUID
    error: str
    attempts: int


class TaskFailedError(Exception):
    """A task permanently failed after exhausting its retries."""

    def __init__(self, failure: TaskFailure) -> None:
        self.task_name = failure.task_name
        self.agent_id = failure.agent_id
        self.error = failure.error
        self.attempts = failure.attempts
        super().__init__(
            f"Task {failure.task_name} ({failure.agent_id}) failed "
            f"after {failure.attempts} attempts: {failure.error}"
        )


async def _get_result(agent_id: str | UUID) -> BaseModel | None:
    key = backend.format_key(*KEY_RESULT, str(agent_id))
    data = await backend.state.get(key)
    return backend.deserialize(data) if data else None


async def get_result(task: Task, timeout: int = DEFAULT_TIMEOUT) -> BaseModel:
    """Poll for a task result.

    Raises:
        TaskFailedError: If the task permanently failed after exhausting retries.
        TimeoutError: If no result is available within the timeout.
    """
    start = time.time()

    while time.time() - start < timeout:
        result = await _get_result(task.agent_id)
        if isinstance(result, TaskFailure):
            raise TaskFailedError(result)
        if result is not None:
            return result
        await asyncio.sleep(0.5)

    raise TimeoutError(f"Result for {task.agent_id} not available within {timeout}s")


async def gather(*tasks: Task, timeout: int = DEFAULT_TIMEOUT) -> tuple[BaseModel, ...]:
    """Wait for multiple tasks and return their results.

    Raises on the first permanently failed task. The remaining tasks keep
    running in their workers and their results stay retrievable — a
    subsequent gather over the same tasks resolves finished ones instantly.

    Raises:
        TaskFailedError: If any task permanently failed after exhausting retries.
        TimeoutError: If any result is not available within the timeout.
    """
    results = await asyncio.gather(*[get_result(task, timeout) for task in tasks])
    return tuple(results)
