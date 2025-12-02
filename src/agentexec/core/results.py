"""Task result storage and retrieval."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

from agentexec import state

if TYPE_CHECKING:
    from agentexec.core.task import Task


async def get_result(task: Task, timeout: float = 300) -> Any:
    """Poll for a task result.

    Waits for a task to complete and returns its result.

    Args:
        task: The Task instance to wait for
        timeout: Maximum seconds to wait for result

    Returns:
        The task's return value

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


async def gather(*tasks: Task) -> tuple[Any, ...]:
    """Wait for multiple tasks and return their results.

    Similar to asyncio.gather, but for background tasks.

    Args:
        *tasks: Task instances to wait for

    Returns:
        Tuple of results in the same order as input tasks

    Example:
        brand = await ax.enqueue("brand_research", ctx)
        market = await ax.enqueue("market_research", ctx)

        brand_result, market_result = await ax.gather(brand, market)
    """
    results = await asyncio.gather(*[get_result(task) for task in tasks])
    return tuple(results)
