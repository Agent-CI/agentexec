from __future__ import annotations
from agentexec import state


class StateEvent:
    """Event primitive backed by the state backend.

    Provides an interface similar to threading.Event/multiprocessing.Event,
    but backed by the state backend for cross-process and cross-machine coordination.

    This class is fully picklable (just stores name and optional id) and works
    across any process that can connect to the same state backend.

    set() and clear() are synchronous for use from pool management code.
    is_set() is async for use from worker event loops.

    Example:
        event = StateEvent("shutdown", "pool1")

        # In pool (sync context)
        event.set()

        # In worker (async context)
        if await event.is_set():
            print("Shutdown signal received")
    """

    def __init__(self, name: str, id: str) -> None:
        """Initialize the event.

        Args:
            name: Event name (e.g., "shutdown", "ready")
            id: Identifier to scope the event (e.g., pool id)
        """
        self.name = name
        self.id = id

    def set(self) -> None:
        """Set the event flag to True."""
        state.set_event(self.name, self.id)

    def clear(self) -> None:
        """Reset the event flag to False."""
        state.clear_event(self.name, self.id)

    async def is_set(self) -> bool:
        """Check if the event flag is True."""
        return await state.acheck_event(self.name, self.id)
