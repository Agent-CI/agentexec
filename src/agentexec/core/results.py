"""Task result storage and retrieval."""

from __future__ import annotations

import asyncio
import pickle
import time
from typing import TYPE_CHECKING, Any

from agentexec.core.redis_client import get_redis

if TYPE_CHECKING:
    from agentexec.core.task import Task


async def get_result(task: Task, timeout: float = 300) -> Any:
    """Poll Redis for a task result.

    Waits for a task to complete and returns its result.

    Args:
        task: The Task instance to wait for
        timeout: Maximum seconds to wait for result

    Returns:
        The task's return value (unpickled)

    Raises:
        TimeoutError: If result not available within timeout
    """
    redis = get_redis()
    start = time.time()

    while time.time() - start < timeout:
        data = await redis.get(f"result:{task.agent_id}")
        if data is not None:
            return pickle.loads(data)
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
