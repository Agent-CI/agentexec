# cspell:ignore rpush lpush brpop RPUSH LPUSH BRPOP
from typing import TypedDict, AsyncGenerator, Coroutine, Optional
import importlib
import json

import redis
import redis.asyncio
from pydantic import BaseModel

from agentexec.config import CONF

__all__ = [
    "format_key",
    "serialize",
    "deserialize",
    "rpush",
    "lpush",
    "brpop",
    "aget",
    "get",
    "aset",
    "set",
    "adelete",
    "delete",
    "publish",
    "subscribe",
    "close",
]

_redis_client: redis.asyncio.Redis | None = None
_redis_sync_client: redis.Redis | None = None
_pubsub: redis.asyncio.client.PubSub | None = None


def format_key(*args: str) -> str:
    """Format a Redis key by joining parts with colons.

    Args:
        *args: Parts of the key

    Returns:
        Formatted key string
    """
    return ":".join(args)


class SerializeWrapper(TypedDict):
    __class__: str
    __data__: str


def serialize(obj: BaseModel) -> bytes:
    """Serialize a Pydantic BaseModel to JSON bytes with type information.

    Stores the fully qualified class name alongside the data, similar to pickle.
    This allows deserialization without needing an external type registry.

    Args:
        obj: Pydantic BaseModel instance to serialize

    Returns:
        JSON-encoded bytes containing class info and data

    Raises:
        TypeError: If obj is not a BaseModel instance
    """
    if not isinstance(obj, BaseModel):
        raise TypeError(f"Expected BaseModel, got {type(obj)}")

    cls = type(obj)
    wrapper: SerializeWrapper = {
        "__class__": f"{cls.__module__}.{cls.__qualname__}",
        "__data__": obj.model_dump_json(),
    }

    return json.dumps(wrapper).encode("utf-8")


def deserialize(data: bytes) -> BaseModel:
    """Deserialize JSON bytes back to a Pydantic BaseModel instance.

    Uses the stored class information to dynamically import and reconstruct
    the original type, similar to pickle.

    Args:
        data: JSON-encoded bytes containing class info and data

    Returns:
        Deserialized BaseModel instance

    Raises:
        ImportError: If the class module cannot be imported
        AttributeError: If the class does not exist in the module
        ValueError: If the data is invalid JSON or missing required fields
    """
    wrapper: SerializeWrapper = json.loads(data.decode("utf-8"))
    class_path = wrapper["__class__"]
    json_data = wrapper["__data__"]

    # Import the class dynamically (e.g., "myapp.models.Result" → myapp.models module)
    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)

    result: BaseModel = cls.model_validate_json(json_data)
    return result


def _get_async_client() -> redis.asyncio.Redis:
    """Get async Redis client, initializing lazily if needed.

    Returns:
        Async Redis client instance

    Raises:
        ValueError: If REDIS_URL is not configured
    """
    global _redis_client

    if _redis_client is None:
        if CONF.redis_url is None:
            raise ValueError("REDIS_URL must be configured")

        _redis_client = redis.asyncio.Redis.from_url(
            CONF.redis_url,
            max_connections=CONF.redis_pool_size,
            socket_connect_timeout=CONF.redis_pool_timeout,
            decode_responses=False,  # Handle binary data (pickled results)
        )

    return _redis_client


def _get_sync_client() -> redis.Redis:
    """Get sync Redis client, initializing lazily if needed.

    Returns:
        Sync Redis client instance

    Raises:
        ValueError: If REDIS_URL is not configured
    """
    global _redis_sync_client

    if _redis_sync_client is None:
        if CONF.redis_url is None:
            raise ValueError("REDIS_URL must be configured")

        _redis_sync_client = redis.Redis.from_url(
            CONF.redis_url,
            max_connections=CONF.redis_pool_size,
            socket_connect_timeout=CONF.redis_pool_timeout,
            decode_responses=False,
        )

    return _redis_sync_client


async def close() -> None:
    """Close all Redis connections and clean up resources."""
    global _redis_client, _redis_sync_client, _pubsub

    # Close pubsub if active
    if _pubsub is not None:
        await _pubsub.close()
        _pubsub = None

    # Close async client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None

    # Close sync client
    if _redis_sync_client is not None:
        _redis_sync_client.close()
        _redis_sync_client = None


def rpush(key: str, value: str) -> int:
    """Push value to the right (front) of the list - for high priority tasks.

    Args:
        key: Redis list key
        value: Serialized task data

    Returns:
        Length of the list after the push
    """
    client = _get_sync_client()
    return client.rpush(key, value)  # type: ignore[return-value]


def lpush(key: str, value: str) -> int:
    """Push value to the left (back) of the list - for low priority tasks.

    Args:
        key: Redis list key
        value: Serialized task data

    Returns:
        Length of the list after the push
    """
    client = _get_sync_client()
    return client.lpush(key, value)  # type: ignore[return-value]


async def brpop(key: str, timeout: int = 0) -> Optional[tuple[str, str]]:
    """Pop value from the right of the list with blocking.

    Args:
        key: Redis list key
        timeout: Timeout in seconds (0 = block forever)

    Returns:
        Tuple of (key, value) or None if timeout
    """
    client = _get_async_client()
    result = await client.brpop([key], timeout=timeout)  # type: ignore[misc]
    if result is None:
        return None
    # Redis returns bytes, decode to string
    list_key, value = result
    return (list_key.decode("utf-8"), value.decode("utf-8"))


def aget(key: str) -> Coroutine[None, None, Optional[bytes]]:
    """Get value for key asynchronously.

    Args:
        key: Key to retrieve

    Returns:
        Coroutine that resolves to value as bytes or None if not found
    """
    client = _get_async_client()
    return client.get(key)  # type: ignore[return-value]


def get(key: str) -> Optional[bytes]:
    """Get value for key synchronously.

    Args:
        key: Key to retrieve

    Returns:
        Value as bytes or None if not found
    """
    client = _get_sync_client()
    return client.get(key)  # type: ignore[return-value]


def aset(key: str, value: bytes, ttl_seconds: Optional[int] = None) -> Coroutine[None, None, bool]:
    """Set value for key asynchronously with optional TTL.

    Args:
        key: Key to set
        value: Value as bytes
        ttl_seconds: Optional time-to-live in seconds

    Returns:
        Coroutine that resolves to True if successful
    """
    client = _get_async_client()
    if ttl_seconds is not None:
        return client.set(key, value, ex=ttl_seconds)  # type: ignore[return-value]
    else:
        return client.set(key, value)  # type: ignore[return-value]


def set(key: str, value: bytes, ttl_seconds: Optional[int] = None) -> bool:
    """Set value for key synchronously with optional TTL.

    Args:
        key: Key to set
        value: Value as bytes
        ttl_seconds: Optional time-to-live in seconds

    Returns:
        True if successful
    """
    client = _get_sync_client()
    if ttl_seconds is not None:
        return client.set(key, value, ex=ttl_seconds)  # type: ignore[return-value]
    else:
        return client.set(key, value)  # type: ignore[return-value]


def adelete(key: str) -> Coroutine[None, None, int]:
    """Delete key asynchronously.

    Args:
        key: Key to delete

    Returns:
        Coroutine that resolves to number of keys deleted (0 or 1)
    """
    client = _get_async_client()
    return client.delete(key)  # type: ignore[return-value]


def delete(key: str) -> int:
    """Delete key synchronously.

    Args:
        key: Key to delete

    Returns:
        Number of keys deleted (0 or 1)
    """
    client = _get_sync_client()
    return client.delete(key)  # type: ignore[return-value]


def publish(channel: str, message: str) -> None:
    """Publish message to a channel.

    Args:
        channel: Channel name
        message: Message to publish
    """
    client = _get_sync_client()
    client.publish(channel, message)


async def subscribe(channel: str) -> AsyncGenerator[str, None]:
    """Subscribe to a channel and yield messages.

    Args:
        channel: Channel name

    Yields:
        Messages from the channel as strings
    """
    global _pubsub

    client = _get_async_client()
    _pubsub = client.pubsub()
    await _pubsub.subscribe(channel)

    try:
        async for message in _pubsub.listen():
            if message["type"] == "message":
                # Decode bytes to string
                data = message["data"]
                if isinstance(data, bytes):
                    yield data.decode("utf-8")
                else:
                    yield data
    finally:
        await _pubsub.unsubscribe(channel)
        await _pubsub.close()
        _pubsub = None
