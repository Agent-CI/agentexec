from __future__ import annotations

from agentexec.config import CONF
from agentexec.state import KEY_EVENT, backend


class StateEvent:
    """Event primitive backed by the state backend.

    Provides an interface similar to threading.Event/multiprocessing.Event,
    but backed by the state backend for cross-process and cross-machine coordination.
    """

    def __init__(self, name: str, id: str) -> None:
        self.name = name
        self.id = id

    def _key(self) -> str:
        return backend.format_key(*KEY_EVENT, self.name, self.id)

    async def set(self) -> None:
        """Set the event flag to True."""
        await backend.state.set(self._key(), b"1")

    async def clear(self) -> None:
        """Reset the event flag to False."""
        await backend.state.delete(self._key())

    async def is_set(self) -> bool:
        """Check if the event flag is True."""
        return await backend.state.get(self._key()) is not None
