# cspell:ignore acheck

"""State management layer.

Initializes the backend(s) and exposes high-level operations for the rest
of agentexec. Supports two modes:

1. **Legacy (single backend)**: A single ``StateBackend`` module handles
   everything (queue, KV, pub/sub). Set via ``AGENTEXEC_STATE_BACKEND``.
   This is the default for backward compatibility with the Redis backend.

2. **Split backends**: Separate KV and stream backends.
   - ``AGENTEXEC_KV_BACKEND``: Key-value operations (Redis, etc.)
   - ``AGENTEXEC_STREAM_BACKEND``: Stream operations (Kafka, etc.)
   When a stream backend is configured, queue and pub/sub operations go
   through it. Lock operations become no-ops (partitioning handles isolation).

The operations layer (``ops``) provides a unified API that modules like
``queue.py``, ``schedule.py``, and ``tracker.py`` call into.
"""

from typing import AsyncGenerator, Coroutine
from uuid import UUID

from pydantic import BaseModel

from agentexec.config import CONF
from agentexec.state import ops
from agentexec.state.backend import StateBackend, load_backend

# ---------------------------------------------------------------------------
# Key constants (used by other modules via state.KEY_*)
# ---------------------------------------------------------------------------

KEY_RESULT = (CONF.key_prefix, "result")
KEY_EVENT = (CONF.key_prefix, "event")
KEY_LOCK = (CONF.key_prefix, "lock")
KEY_SCHEDULE = (CONF.key_prefix, "schedule")
KEY_SCHEDULE_QUEUE = (CONF.key_prefix, "schedule_queue")
CHANNEL_LOGS = (CONF.key_prefix, "logs")

# ---------------------------------------------------------------------------
# Backend initialization
# ---------------------------------------------------------------------------

# Legacy backend — always loaded for backward compatibility.
# Modules that still reference `state.backend` directly will work.
_legacy_backend: StateBackend | None = None

try:
    import importlib
    from typing import cast

    _mod = importlib.import_module(CONF.state_backend)
    _legacy_backend = load_backend(_mod)
except Exception:
    # If the legacy backend can't load (e.g. Redis not installed but Kafka
    # is configured), that's fine — the ops layer will use the new backends.
    pass

# Expose legacy backend for modules that import `state.backend` directly.
# This keeps existing code working during the migration.
backend: StateBackend = _legacy_backend  # type: ignore[assignment]

# Initialize the operations layer with configured backends.
# The KV backend defaults to the legacy state_backend module path — but only
# if the legacy backend actually loaded (i.e. it conforms to the KV protocol).
# If someone sets state_backend to the kafka module, we don't use it as KV.
_kv_module = CONF.kv_backend
if not _kv_module and _legacy_backend is not None:
    _kv_module = CONF.state_backend

ops.init(
    kv_backend=_kv_module,
    stream_backend=CONF.stream_backend,
)


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

    With a stream backend, this is a no-op (always returns True)
    because Kafka partitioning provides natural task isolation.
    """
    return await ops.acquire_lock(lock_key, agent_id)


async def release_lock(lock_key: str) -> int:
    """Release a task lock.

    With a stream backend, this is a no-op (returns 0).
    """
    return await ops.release_lock(lock_key)


def clear_keys() -> int:
    """Clear all state keys managed by this application."""
    return ops.clear_keys()
