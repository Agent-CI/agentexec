# cspell:ignore rpush lpush brpop RPUSH LPUSH BRPOP
"""Redis queue operations using lists with rpush/lpush/brpop."""

from __future__ import annotations

import json
from typing import Any

from agentexec.state.redis_backend.connection import get_async_client


async def queue_push(
    queue_name: str,
    value: str,
    *,
    high_priority: bool = False,
    partition_key: str | None = None,
) -> None:
    """Push a task onto the Redis list queue.

    HIGH priority: rpush (right/front, dequeued first).
    LOW priority: lpush (left/back, dequeued later).
    partition_key is ignored (Redis uses locks for isolation).
    """
    client = get_async_client()
    if high_priority:
        await client.rpush(queue_name, value)
    else:
        await client.lpush(queue_name, value)


async def queue_pop(
    queue_name: str,
    *,
    timeout: int = 1,
) -> dict[str, Any] | None:
    """Pop the next task from the Redis list queue (blocking).

    BRPOP atomically removes the message — delivery is implicit.
    """
    client = get_async_client()
    result = await client.brpop([queue_name], timeout=timeout)  # type: ignore[misc]
    if result is None:
        return None
    _, value = result
    return json.loads(value.decode("utf-8"))
