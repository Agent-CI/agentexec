from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from pydantic import BaseModel

from agentexec.state import KEY_RESULT, backend

if TYPE_CHECKING:
    from agentexec.core.task import Task


DEFAULT_TIMEOUT: int = 300  # TODO improve this polling approach


async def _get_result(agent_id: str) -> BaseModel | None:
    key = backend.format_key(*KEY_RESULT, str(agent_id))
    data = await backend.state.get(key)
    return backend.deserialize(data) if data else None


async def get_result(task: Task, timeout: int = DEFAULT_TIMEOUT) -> BaseModel:
    """Poll for a task result."""
    start = time.time()

    while time.time() - start < timeout:
        result = await _get_result(task.agent_id)
        if result is not None:
            return result
        await asyncio.sleep(0.5)

    raise TimeoutError(f"Result for {task.agent_id} not available within {timeout}s")


async def gather(*tasks: Task, timeout: int = DEFAULT_TIMEOUT) -> tuple[BaseModel, ...]:
    """Wait for multiple tasks and return their results."""
    results = await asyncio.gather(*[get_result(task, timeout) for task in tasks])
    return tuple(results)
