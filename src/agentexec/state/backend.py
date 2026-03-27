"""Backend loader and validation.

Validates that a backend module implements all three domain protocols
(StateProtocol, QueueProtocol, ActivityProtocol) plus connection management.

Pick one backend via AGENTEXEC_STATE_BACKEND:
  - 'agentexec.state.redis_backend'  (default)
  - 'agentexec.state.kafka_backend'
"""

from __future__ import annotations

from types import ModuleType

from agentexec.state.protocols import ActivityProtocol, QueueProtocol, StateProtocol


def load_backend(module: ModuleType) -> ModuleType:
    """Load and validate a backend module conforms to all protocols.

    Checks that the module exposes the required functions from
    StateProtocol, QueueProtocol, and ActivityProtocol, plus
    connection management (close).

    Args:
        module: Backend module to validate.

    Returns:
        The validated module.

    Raises:
        TypeError: If the module is missing required functions.
    """
    required: set[str] = set()

    for protocol_cls in (StateProtocol, QueueProtocol, ActivityProtocol):
        attrs = getattr(protocol_cls, "__protocol_attrs__", None)
        if attrs is None:
            attrs = {
                name
                for name in dir(protocol_cls)
                if not name.startswith("_") and callable(getattr(protocol_cls, name, None))
            }
        required.update(attrs)

    # Connection management is always required
    required.add("close")

    missing = [name for name in sorted(required) if not hasattr(module, name)]
    if missing:
        raise TypeError(
            f"Backend module '{module.__name__}' missing required functions: {missing}"
        )

    return module
