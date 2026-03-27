from __future__ import annotations
from agentexec.state import ops


class StateEvent:
    """Event primitive backed by the state backend.

    Provides an interface similar to threading.Event/multiprocessing.Event,
    but backed by the state backend for cross-process and cross-machine coordination.

    This class is fully picklable (just stores name and optional id) and works
    across any process that can connect to the same state backend.

    Example:
        event = StateEvent("shutdown", "pool1")

        # Set the event
        await event.set()

        # Check if set
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

    async def set(self) -> None:
        """Set the event flag to True."""
        await ops.set_event(self.name, self.id)

    async def clear(self) -> None:
        """Reset the event flag to False."""
        await ops.clear_event(self.name, self.id)

    async def is_set(self) -> bool:
        """Check if the event flag is True."""
        return await ops.check_event(self.name, self.id)
