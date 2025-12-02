from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from pydantic import BaseModel

from agentexec import state

if TYPE_CHECKING:
    from agentexec.core.task import Task


DEFAULT_TIMEOUT: int = 300  # TODO improve this polling approach


async def get_result(task: Task, timeout: int = DEFAULT_TIMEOUT) -> BaseModel:
    """Poll for a task result.

    Waits for a task to complete and returns its result.
    Uses automatic type reconstruction from serialized class information.

    Args:
        task: The Task instance to wait for
        timeout: Maximum seconds to wait for result

    Returns:
        Deserialized result as BaseModel instance

    Raises:
        TimeoutError: If result not available within timeout
    """
    start = time.time()

    while time.time() - start < timeout:
        result = await state.aget_result(task.agent_id)
        if result is not None:
            return result
        await asyncio.sleep(0.5)

    raise TimeoutError(f"Result for {task.agent_id} not available within {timeout}s")


async def gather(*tasks: Task, timeout: int = DEFAULT_TIMEOUT) -> tuple[BaseModel, ...]:
    """Wait for multiple tasks and return their results.

    Similar to asyncio.gather, but for background tasks.

    Args:
        *tasks: Task instances to wait for
        timeout: Maximum seconds to wait for each result

    Returns:
        Tuple of deserialized results as BaseModel instances

    Example:
        brand = await ax.enqueue("brand_research", ctx)
        market = await ax.enqueue("market_research", ctx)

        brand_result, market_result = await ax.gather(brand, market)
    """
    results = await asyncio.gather(*[get_result(task, timeout) for task in tasks])
    return tuple(results)
