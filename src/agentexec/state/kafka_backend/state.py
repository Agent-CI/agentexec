"""Kafka state operations: KV store, counters, pub/sub, locks, sorted index, serialization.

Uses compacted topics for persistence and in-memory caches for reads.
"""

from __future__ import annotations

import importlib
import json
from typing import Any, AsyncGenerator, Optional, TypedDict

from pydantic import BaseModel

from agentexec.config import CONF
from agentexec.state.kafka_backend.connection import (
    _cache_lock,
    client_id,
    ensure_topic,
    get_bootstrap_servers,
    kv_topic,
    logs_topic,
    produce,
    produce_sync,
)

# ---------------------------------------------------------------------------
# In-memory caches
# ---------------------------------------------------------------------------

_kv_cache: dict[str, bytes] = {}
_counter_cache: dict[str, int] = {}
_sorted_set_cache: dict[str, dict[str, float]] = {}  # key -> {member: score}


# -- KV store (compacted topic + in-memory cache) ----------------------------


async def store_get(key: str) -> Optional[bytes]:
    """Get from in-memory cache (populated from compacted state topic)."""
    with _cache_lock:
        return _kv_cache.get(key)


async def store_set(key: str, value: bytes, ttl_seconds: Optional[int] = None) -> bool:
    """Write to compacted state topic and update local cache.

    ttl_seconds is accepted for interface compatibility but not enforced —
    Kafka uses topic-level retention instead of per-key TTL.
    """
    with _cache_lock:
        _kv_cache[key] = value
    await produce(kv_topic(), value, key=key)
    return True


async def store_delete(key: str) -> int:
    """Tombstone the key in the compacted topic and remove from cache."""
    with _cache_lock:
        existed = 1 if key in _kv_cache else 0
        _kv_cache.pop(key, None)
    await produce(kv_topic(), None, key=key)  # Tombstone
    return existed


# -- Counters (in-memory + compacted topic) -----------------------------------


async def counter_incr(key: str) -> int:
    """Increment counter in local cache and persist to compacted topic."""
    with _cache_lock:
        val = _counter_cache.get(key, 0) + 1
        _counter_cache[key] = val
    await produce(kv_topic(), str(val).encode("utf-8"), key=f"counter:{key}")
    return val


async def counter_decr(key: str) -> int:
    """Decrement counter in local cache and persist to compacted topic."""
    with _cache_lock:
        val = _counter_cache.get(key, 0) - 1
        _counter_cache[key] = val
    await produce(kv_topic(), str(val).encode("utf-8"), key=f"counter:{key}")
    return val


# -- Pub/sub (log streaming via Kafka topic) ----------------------------------


def log_publish(channel: str, message: str) -> None:
    """Produce a log message to the logs topic. Sync for logging handler compatibility."""
    produce_sync(logs_topic(), message.encode("utf-8"))


async def log_subscribe(channel: str) -> AsyncGenerator[str, None]:
    """Consume log messages from the logs topic."""
    from aiokafka import AIOKafkaConsumer

    from agentexec.state.kafka_backend.queue import _discover_partitions

    topic = logs_topic()
    await ensure_topic(topic)

    consumer = AIOKafkaConsumer(
        bootstrap_servers=get_bootstrap_servers(),
        client_id=client_id("log-collector"),
        enable_auto_commit=False,
    )
    await consumer.start()

    # Manual partition assignment — no consumer group overhead
    tps = await _discover_partitions(consumer, topic)
    consumer.assign(tps)

    # Seek to end so we only see new messages
    await consumer.seek_to_end(*tps)

    try:
        async for msg in consumer:
            yield msg.value.decode("utf-8")
    finally:
        await consumer.stop()


# -- Locks — no-op with Kafka ------------------------------------------------


async def acquire_lock(key: str, value: str, ttl_seconds: int) -> bool:
    """Always returns True — partition assignment handles isolation."""
    return True


async def release_lock(key: str) -> int:
    """No-op — returns 0."""
    return 0


# -- Sorted index (in-memory + compacted topic) ------------------------------


async def index_add(key: str, mapping: dict[str, float]) -> int:
    """Add members with scores. Persists to compacted topic."""
    added = 0
    with _cache_lock:
        if key not in _sorted_set_cache:
            _sorted_set_cache[key] = {}
        for member, score in mapping.items():
            if member not in _sorted_set_cache[key]:
                added += 1
            _sorted_set_cache[key][member] = score
    data = json.dumps(_sorted_set_cache[key]).encode("utf-8")
    await produce(kv_topic(), data, key=f"zset:{key}")
    return added


async def index_range(
    key: str, min_score: float, max_score: float
) -> list[bytes]:
    """Query in-memory sorted set index by score range."""
    with _cache_lock:
        members = _sorted_set_cache.get(key, {})
        return [
            member.encode("utf-8")
            for member, score in members.items()
            if min_score <= score <= max_score
        ]


async def index_remove(key: str, *members: str) -> int:
    """Remove members from in-memory sorted set. Persists update."""
    removed = 0
    with _cache_lock:
        if key in _sorted_set_cache:
            for member in members:
                if member in _sorted_set_cache[key]:
                    del _sorted_set_cache[key][member]
                    removed += 1
    if removed > 0:
        data = json.dumps(_sorted_set_cache.get(key, {})).encode("utf-8")
        await produce(kv_topic(), data, key=f"zset:{key}")
    return removed


# -- Serialization (sync — pure CPU) -----------------------------------------


class _SerializeWrapper(TypedDict):
    __class__: str
    __data__: str


def serialize(obj: BaseModel) -> bytes:
    """Serialize a Pydantic BaseModel to JSON bytes with type information."""
    if not isinstance(obj, BaseModel):
        raise TypeError(f"Expected BaseModel, got {type(obj)}")

    cls = type(obj)
    wrapper: _SerializeWrapper = {
        "__class__": f"{cls.__module__}.{cls.__qualname__}",
        "__data__": obj.model_dump_json(),
    }
    return json.dumps(wrapper).encode("utf-8")


def deserialize(data: bytes) -> BaseModel:
    """Deserialize JSON bytes back to a typed Pydantic BaseModel instance."""
    wrapper: _SerializeWrapper = json.loads(data.decode("utf-8"))
    class_path = wrapper["__class__"]
    json_data = wrapper["__data__"]

    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)

    result: BaseModel = cls.model_validate_json(json_data)
    return result


# -- Key formatting -----------------------------------------------------------


def format_key(*args: str) -> str:
    """Join key parts with dots (Kafka convention)."""
    return ".".join(args)


# -- Cleanup ------------------------------------------------------------------


async def clear_keys() -> int:
    """Clear in-memory caches. Topic data is managed by retention policies."""
    from agentexec.state.kafka_backend.activity import _activity_cache

    with _cache_lock:
        count = (
            len(_kv_cache) + len(_counter_cache)
            + len(_sorted_set_cache) + len(_activity_cache)
        )
        _kv_cache.clear()
        _counter_cache.clear()
        _sorted_set_cache.clear()
        _activity_cache.clear()
    return count
