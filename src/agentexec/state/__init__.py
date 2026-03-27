# cspell:ignore acheck

"""State management layer.

Initializes the configured backend and exposes high-level operations for
the rest of agentexec. Pick one backend via AGENTEXEC_STATE_BACKEND:

  - 'agentexec.state.redis_backend'  (default)
  - 'agentexec.state.kafka_backend'

All state operations go through the ops layer (``state.ops``), which
delegates to whichever backend is loaded. Modules like queue.py,
schedule.py, and tracker.py should call ops functions rather than
touching backend primitives directly.
"""

from typing import AsyncGenerator, Coroutine
from uuid import UUID

from pydantic import BaseModel

from agentexec.config import CONF
from agentexec.state import ops
from agentexec.state.backend import load_backend

# ---------------------------------------------------------------------------
# Backend initialization
# ---------------------------------------------------------------------------

# Initialize the ops layer with the configured backend.
ops.init(CONF.state_backend)

# Also load the backend module directly for backward compatibility.
# Modules that still reference ``state.backend`` will work during migration.
import importlib as _importlib

backend = load_backend(
    _importlib.import_module(CONF.state_backend)
)

# Re-export key constants from ops for backward compatibility.
KEY_RESULT = ops.KEY_RESULT
KEY_EVENT = ops.KEY_EVENT
KEY_LOCK = ops.KEY_LOCK
KEY_SCHEDULE = ops.KEY_SCHEDULE
KEY_SCHEDULE_QUEUE = ops.KEY_SCHEDULE_QUEUE
CHANNEL_LOGS = ops.CHANNEL_LOGS


# ---------------------------------------------------------------------------
# Public API — delegates to ops layer
# ---------------------------------------------------------------------------

__all__ = [
    "backend",
    "ops",
    "get_result",
    "aget_result",
    "set_result",
    "aset_result",
    "delete_result",
    "adelete_result",
    "publish_log",
    "subscribe_logs",
    "set_event",
    "clear_event",
    "check_event",
    "acheck_event",
    "acquire_lock",
    "release_lock",
    "clear_keys",
]


def get_result(agent_id: UUID | str) -> BaseModel | None:
    """Get result for an agent (sync)."""
    return ops.get_result(agent_id)


def aget_result(agent_id: UUID | str) -> Coroutine[None, None, BaseModel | None]:
    """Get result for an agent (async)."""
    return ops.aget_result(agent_id)


def set_result(
    agent_id: UUID | str,
    data: BaseModel,
    ttl_seconds: int | None = None,
) -> bool:
    """Set result for an agent (sync)."""
    ops.set_result(agent_id, data, ttl_seconds=ttl_seconds)
    return True


def aset_result(
    agent_id: UUID | str,
    data: BaseModel,
    ttl_seconds: int | None = None,
) -> Coroutine[None, None, bool]:
    """Set result for an agent (async)."""

    async def _set() -> bool:
        await ops.aset_result(agent_id, data, ttl_seconds=ttl_seconds)
        return True

    return _set()


def delete_result(agent_id: UUID | str) -> int:
    """Delete result for an agent (sync)."""
    return ops.delete_result(agent_id)


def adelete_result(agent_id: UUID | str) -> Coroutine[None, None, int]:
    """Delete result for an agent (async)."""

    async def _delete() -> int:
        await ops.adelete_result(agent_id)
        return 1

    return _delete()


def publish_log(message: str) -> None:
    """Publish a log message to the log channel."""
    ops.publish_log(message)


def subscribe_logs() -> AsyncGenerator[str, None]:
    """Subscribe to log messages."""
    return ops.subscribe_logs()


def set_event(name: str, id: str) -> bool:
    """Set an event flag."""
    ops.set_event(name, id)
    return True


def clear_event(name: str, id: str) -> int:
    """Clear an event flag."""
    ops.clear_event(name, id)
    return 1


def check_event(name: str, id: str) -> bool:
    """Check if an event flag is set (sync)."""
    return ops.check_event(name, id)


def acheck_event(name: str, id: str) -> Coroutine[None, None, bool]:
    """Check if an event flag is set (async)."""

    async def _check() -> bool:
        return await ops.acheck_event(name, id)

    return _check()


async def acquire_lock(lock_key: str, agent_id: str) -> bool:
    """Attempt to acquire a task lock.

    Kafka backend: always True (partition isolation).
    Redis backend: SET NX EX with TTL safety net.
    """
    return await ops.acquire_lock(lock_key, agent_id)


async def release_lock(lock_key: str) -> int:
    """Release a task lock.

    Kafka backend: no-op (returns 0).
    Redis backend: deletes the lock key.
    """
    return await ops.release_lock(lock_key)


def clear_keys() -> int:
    """Clear all state keys managed by this application."""
    return ops.clear_keys()
