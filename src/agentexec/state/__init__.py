# cspell:ignore acheck

from typing import AsyncGenerator, Coroutine
from agentexec.config import CONF

KEY_RESULT = (CONF.key_prefix, "result")
KEY_SHUTDOWN = (CONF.key_prefix, "shutdown")
CHANNEL_LOGS = (CONF.key_prefix, "logs")


match CONF.state_backend:
    case "redis":
        from agentexec.state import redis_backend as backend
    case _:
        raise RuntimeError(f"Unsupported state backend: {CONF.state_backend}.")


__all__ = [
    "backend",
    "get_result",
    "aget_result",
    "set_result",
    "aset_result",
    "delete_result",
    "adelete_result",
    "set_shutdown_flag",
    "check_shutdown_flag",
    "acheck_shutdown_flag",
    "clear_shutdown_flag",
    "publish_log",
    "subscribe_logs",
]


def get_result(agent_id: str) -> object | None:
    """Get result for an agent (sync).

    Args:
        agent_id: Unique agent identifier

    Returns:
        Deserialized result object or None if not found
    """
    data = backend.get(backend.format_key(*KEY_RESULT, agent_id))
    return backend.deserialize(data) if data else None


def aget_result(agent_id: str) -> Coroutine[None, None, object | None]:
    """Get result for an agent (async).

    Args:
        agent_id: Unique agent identifier

    Returns:
        Coroutine that resolves to deserialized result object or None if not found
    """

    async def _get() -> object | None:
        data = await backend.aget(backend.format_key(*KEY_RESULT, agent_id))
        return backend.deserialize(data) if data else None

    return _get()


def set_result(agent_id: str, data: object, ttl_seconds: int | None = None) -> bool:
    """Set result for an agent (sync).

    Args:
        agent_id: Unique agent identifier
        data: Result data to store
        ttl_seconds: Optional time-to-live in seconds

    Returns:
        True if successful
    """
    return backend.set(
        backend.format_key(*KEY_RESULT, agent_id),
        backend.serialize(data),
        ttl_seconds=ttl_seconds,
    )


def aset_result(
    agent_id: str,
    data: object,
    ttl_seconds: int | None = None,
) -> Coroutine[None, None, bool]:
    """Set result for an agent (async).

    Args:
        agent_id: Unique agent identifier
        data: Result data to store
        ttl_seconds: Optional time-to-live in seconds

    Returns:
        Coroutine that resolves to True if successful
    """
    return backend.aset(
        backend.format_key(*KEY_RESULT, agent_id),
        backend.serialize(data),
        ttl_seconds=ttl_seconds,
    )


def delete_result(agent_id: str) -> int:
    """Delete result for an agent (sync).

    Args:
        agent_id: Unique agent identifier

    Returns:
        Number of keys deleted (0 or 1)
    """
    return backend.delete(backend.format_key(*KEY_RESULT, agent_id))


def adelete_result(agent_id: str) -> Coroutine[None, None, int]:
    """Delete result for an agent (async).

    Args:
        agent_id: Unique agent identifier

    Returns:
        Coroutine that resolves to number of keys deleted (0 or 1)
    """
    return backend.adelete(backend.format_key(*KEY_RESULT, agent_id))


def set_shutdown_flag(pool_id: str) -> bool:
    """Set shutdown flag for a worker pool (sync).

    Args:
        pool_id: Worker pool identifier

    Returns:
        True if successful
    """
    return backend.set(backend.format_key(*KEY_SHUTDOWN, pool_id), b"1")


def check_shutdown_flag(pool_id: str) -> bool:
    """Check if shutdown flag is set (sync).

    Args:
        pool_id: Worker pool identifier

    Returns:
        True if shutdown flag is set, False otherwise
    """
    return backend.get(backend.format_key(*KEY_SHUTDOWN, pool_id)) is not None


def acheck_shutdown_flag(pool_id: str) -> Coroutine[None, None, bool]:
    """Check if shutdown flag is set (async).

    Args:
        pool_id: Worker pool identifier

    Returns:
        Coroutine that resolves to True if shutdown flag is set, False otherwise
    """

    async def _check() -> bool:
        return await backend.aget(backend.format_key(*KEY_SHUTDOWN, pool_id)) is not None

    return _check()


def clear_shutdown_flag(pool_id: str) -> int:
    """Clear shutdown flag for a worker pool (sync).

    Args:
        pool_id: Worker pool identifier

    Returns:
        Number of keys deleted (0 or 1)
    """
    return backend.delete(backend.format_key(*KEY_SHUTDOWN, pool_id))


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
