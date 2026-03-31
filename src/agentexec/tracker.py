"""Tracker for coordinating dynamic fan-out patterns.

Use Tracker to coordinate tasks that are queued dynamically (e.g., by an agent)
and need to trigger a follow-up step when all complete.

Example:
    tracker = ax.Tracker("research", batch_id)
    await tracker.incr()  # Count the discovery process itself

    @function_tool
    async def queue_research(company: str) -> str:
        await tracker.incr()
        await ax.enqueue("research", ResearchContext(company=company, batch_id=batch_id))
        return f"Queued {company}"

    # When discovery finishes, decrement itself
    if await tracker.decr() == 0:
        await ax.enqueue("aggregate", AggregateContext(batch_id=batch_id))

    # In research task - decrement when done
    tracker = ax.Tracker("research", context.batch_id)
    # ... do research ...
    if await tracker.decr() == 0:
        await ax.enqueue("aggregate", AggregateContext(batch_id=context.batch_id))
"""

from agentexec.config import CONF
from agentexec.state import backend


class Tracker:
    """Coordinate dynamic fan-out with an atomic counter."""

    def __init__(self, *args: str):
        self._key = backend.format_key(CONF.key_prefix, "tracker", *args)

    async def incr(self) -> int:
        return await backend.state.counter_incr(self._key)

    async def decr(self) -> int:
        return await backend.state.counter_decr(self._key)

    async def count(self) -> int:
        result = await backend.state.get(self._key)
        return int(result) if result else 0

    async def complete(self) -> bool:
        return await self.count() == 0
