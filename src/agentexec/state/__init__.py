# cspell:ignore acheck

from typing import cast, AsyncGenerator, Coroutine
import importlib
from uuid import UUID

from pydantic import BaseModel

from agentexec.config import CONF
from agentexec.state.backend import StateBackend

KEY_RESULT = (CONF.key_prefix, "result")
KEY_EVENT = (CONF.key_prefix, "event")
CHANNEL_LOGS = (CONF.key_prefix, "logs")

__all__ = [
    "backend",
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
]


def _load_backend(module_name: str) -> StateBackend:
    module = cast(StateBackend, importlib.import_module(module_name))
    if not isinstance(module, StateBackend):  # type: ignore[invalid-argument-type]
        raise RuntimeError(f"State backend ({module_name}) does not conform to protocol.")
    return module


backend: StateBackend = _load_backend(CONF.state_backend)


def get_result(agent_id: UUID | str) -> BaseModel | None:
    """Get result for an agent (sync).

    Returns deserialized BaseModel instance with automatic type reconstruction.

    Args:
        agent_id: Unique agent identifier (UUID or string)

    Returns:
        Deserialized BaseModel or None if not found
    """
    data = backend.get(backend.format_key(*KEY_RESULT, str(agent_id)))
    return backend.deserialize(data) if data else None


def aget_result(agent_id: UUID | str) -> Coroutine[None, None, BaseModel | None]:
    """Get result for an agent (async).

    Returns deserialized BaseModel instance with automatic type reconstruction.

    Args:
        agent_id: Unique agent identifier (UUID or string)

    Returns:
        Coroutine that resolves to deserialized BaseModel or None if not found
    """

    async def _get() -> BaseModel | None:
        data = await backend.aget(backend.format_key(*KEY_RESULT, str(agent_id)))
        return backend.deserialize(data) if data else None

    return _get()


def set_result(
    agent_id: UUID | str,
    data: BaseModel,
    ttl_seconds: int | None = None,
) -> bool:
    """Set result for an agent (sync).

    Args:
        agent_id: Unique agent identifier (UUID or string)
        data: Result data (must be Pydantic BaseModel)
        ttl_seconds: Optional time-to-live in seconds

    Returns:
        True if successful
    """
    return backend.set(
        backend.format_key(*KEY_RESULT, str(agent_id)),
        backend.serialize(data),
        ttl_seconds=ttl_seconds,
    )


def aset_result(
    agent_id: UUID | str,
    data: BaseModel,
    ttl_seconds: int | None = None,
) -> Coroutine[None, None, bool]:
    """Set result for an agent (async).

    Args:
        agent_id: Unique agent identifier (UUID or string)
        data: Result data (must be Pydantic BaseModel)
        ttl_seconds: Optional time-to-live in seconds

    Returns:
        Coroutine that resolves to True if successful
    """
    return backend.aset(
        backend.format_key(*KEY_RESULT, str(agent_id)),
        backend.serialize(data),
        ttl_seconds=ttl_seconds,
    )


def delete_result(agent_id: UUID | str) -> int:
    """Delete result for an agent (sync).

    Args:
        agent_id: Unique agent identifier (UUID or string)

    Returns:
        Number of keys deleted (0 or 1)
    """
    return backend.delete(backend.format_key(*KEY_RESULT, str(agent_id)))


def adelete_result(agent_id: UUID | str) -> Coroutine[None, None, int]:
    """Delete result for an agent (async).

    Args:
        agent_id: Unique agent identifier (UUID or string)

    Returns:
        Coroutine that resolves to number of keys deleted (0 or 1)
    """
    return backend.adelete(backend.format_key(*KEY_RESULT, str(agent_id)))


def publish_log(message: str) -> None:
    """Publish a log message to the log channel (sync).

    Args:
        message: Log message to publish (should be JSON string)
    """
    backend.publish(backend.format_key(*CHANNEL_LOGS), message)


def subscribe_logs() -> AsyncGenerator[str, None]:
    """Subscribe to log messages (async generator).

    Yields:
        Log messages from the channel
    """
    return backend.subscribe(backend.format_key(*CHANNEL_LOGS))


def set_event(name: str, id: str) -> bool:
    """Set an event flag.

    Args:
        name: Event name (e.g., "shutdown", "ready")
        id: Event identifier (e.g., pool id)

    Returns:
        True if successful
    """
    return backend.set(backend.format_key(*KEY_EVENT, name, id), b"1")


def clear_event(name: str, id: str) -> int:
    """Clear an event flag.

    Args:
        name: Event name (e.g., "shutdown", "ready")
        id: Event identifier (e.g., pool id)

    Returns:
        Number of keys deleted (0 or 1)
    """
    return backend.delete(backend.format_key(*KEY_EVENT, name, id))


def check_event(name: str, id: str) -> bool:
    """Check if an event flag is set (sync).

    Args:
        name: Event name (e.g., "shutdown", "ready")
        id: Event identifier (e.g., pool id)

    Returns:
        True if event is set, False otherwise
    """
    return backend.get(backend.format_key(*KEY_EVENT, name, id)) is not None


def acheck_event(name: str, id: str) -> Coroutine[None, None, bool]:
    """Check if an event flag is set (async).

    Args:
        name: Event name (e.g., "shutdown", "ready")
        id: Event identifier (e.g., pool id)

    Returns:
        Coroutine that resolves to True if event is set, False otherwise
    """

    async def _check() -> bool:
        return await backend.aget(backend.format_key(*KEY_EVENT, name, id)) is not None

    return _check()
