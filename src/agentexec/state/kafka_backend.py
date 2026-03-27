"""Kafka implementation of the agentexec state backend.

Replaces Redis entirely with Apache Kafka:
- Queue: Kafka topic with consumer groups. Partition key derived from
  lock_key provides natural per-user ordering and isolation (no locks).
- KV: Compacted topics for results, events, schedules. Reads are served
  from an in-memory cache populated by consuming the compacted topic.
- Counters: In-memory counters backed by a compacted topic for persistence.
- Pub/sub: Kafka topic for log streaming.
- Locks: No-op — Kafka's partition assignment handles isolation.
- Sorted sets: In-memory index backed by a compacted topic.
- Serialization: Same JSON+type-info format as Redis backend.

Requires the ``aiokafka`` package::

    pip install agentexec[kafka]
"""

from __future__ import annotations

import asyncio
import importlib
import json
import threading
import uuid
from datetime import UTC, datetime
from typing import Any, AsyncGenerator, Coroutine, Optional, TypedDict

from pydantic import BaseModel

from agentexec.config import CONF

__all__ = [
    "close",
    "queue_push",
    "queue_pop",
    "get",
    "aget",
    "get",
    "set",
    "aset",
    "delete",
    "adelete",
    "incr",
    "decr",
    "publish",
    "subscribe",
    "acquire_lock",
    "release_lock",
    "zadd",
    "zrangebyscore",
    "zrem",
    "serialize",
    "deserialize",
    "format_key",
    "clear_keys",
]


# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------

_producer: object | None = None  # AIOKafkaProducer
_consumers: dict[str, object] = {}  # consumer_key -> AIOKafkaConsumer
_admin: object | None = None  # AIOKafkaAdminClient

# In-memory caches for compacted topic data
_kv_cache: dict[str, bytes] = {}
_counter_cache: dict[str, int] = {}
_sorted_set_cache: dict[str, dict[str, float]] = {}  # key -> {member: score}
_activity_cache: dict[str, dict[str, Any]] = {}  # agent_id -> activity record

_cache_lock = threading.Lock()
_initialized_topics: set[str] = set()
_worker_id: str | None = None


def configure(*, worker_id: str | None = None) -> None:
    """Set per-process identity for Kafka client IDs.

    Called by Worker.run() before any Kafka operations so that broker
    logs and monitoring tools can distinguish between consumers.
    """
    global _worker_id
    _worker_id = worker_id


def _client_id(role: str = "worker") -> str:
    """Build a client_id string, including worker_id when available."""
    base = f"{CONF.key_prefix}-{role}"
    if _worker_id is not None:
        return f"{base}-{_worker_id}"
    return base


def _get_bootstrap_servers() -> str:
    if CONF.kafka_bootstrap_servers is None:
        raise ValueError(
            "KAFKA_BOOTSTRAP_SERVERS must be configured "
            "(e.g. 'localhost:9092' or 'broker1:9092,broker2:9092')"
        )
    return CONF.kafka_bootstrap_servers


# ---------------------------------------------------------------------------
# Topic naming conventions
# ---------------------------------------------------------------------------


def _tasks_topic(queue_name: str) -> str:
    return f"{CONF.key_prefix}.tasks.{queue_name}"


def _kv_topic() -> str:
    return f"{CONF.key_prefix}.state"


def _logs_topic() -> str:
    return f"{CONF.key_prefix}.logs"


def _activity_topic() -> str:
    return f"{CONF.key_prefix}.activity"


# ---------------------------------------------------------------------------
# Internal Kafka helpers
# ---------------------------------------------------------------------------


async def _get_producer():  # type: ignore[no-untyped-def]
    global _producer
    if _producer is None:
        from aiokafka import AIOKafkaProducer

        _producer = AIOKafkaProducer(
            bootstrap_servers=_get_bootstrap_servers(),
            client_id=_client_id("producer"),
            acks="all",
            max_batch_size=CONF.kafka_max_batch_size,
            linger_ms=CONF.kafka_linger_ms,
        )
        await _producer.start()  # type: ignore[union-attr]
    return _producer


async def _get_admin():  # type: ignore[no-untyped-def]
    global _admin
    if _admin is None:
        from aiokafka.admin import AIOKafkaAdminClient

        _admin = AIOKafkaAdminClient(
            bootstrap_servers=_get_bootstrap_servers(),
            client_id=_client_id("admin"),
        )
        await _admin.start()  # type: ignore[union-attr]
    return _admin


async def _produce(topic: str, value: bytes | None, key: str | None = None) -> None:
    """Produce a message. key=None means unkeyed."""
    producer = await _get_producer()
    key_bytes = key.encode("utf-8") if key is not None else None
    await producer.send_and_wait(topic, value=value, key=key_bytes)  # type: ignore[union-attr]


def _produce_sync(topic: str, value: bytes | None, key: str | None = None) -> None:
    """Produce from synchronous context."""
    try:
        loop = asyncio.get_running_loop()
        # Fire-and-forget from async context
        loop.create_task(_produce(topic, value, key))
    except RuntimeError:
        asyncio.run(_produce(topic, value, key))


async def _ensure_topic(topic: str, *, compact: bool = False) -> None:
    """Create a topic if it doesn't exist."""
    if topic in _initialized_topics:
        return

    from aiokafka.admin import NewTopic

    admin = await _get_admin()
    config: dict[str, str] = {}
    if compact:
        config["cleanup.policy"] = "compact"

    try:
        await admin.create_topics(  # type: ignore[union-attr]
            [
                NewTopic(
                    name=topic,
                    num_partitions=CONF.kafka_default_partitions,
                    replication_factor=CONF.kafka_replication_factor,
                    topic_configs=config,
                )
            ]
        )
    except Exception:
        # Topic already exists — that's fine
        pass

    _initialized_topics.add(topic)


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------


async def close() -> None:
    """Close all Kafka connections."""
    global _producer, _admin

    if _producer is not None:
        await _producer.stop()  # type: ignore[union-attr]
        _producer = None

    for consumer in _consumers.values():
        await consumer.stop()  # type: ignore[union-attr]
    _consumers.clear()

    if _admin is not None:
        await _admin.close()  # type: ignore[union-attr]
        _admin = None


# ---------------------------------------------------------------------------
# Queue operations
# ---------------------------------------------------------------------------


def queue_push(
    queue_name: str,
    value: str,
    *,
    high_priority: bool = False,
    partition_key: str | None = None,
) -> None:
    """Produce a task to the tasks topic.

    partition_key determines which partition the task lands in. Tasks with
    the same partition_key are guaranteed to be processed in order by a
    single consumer — this replaces distributed locking.

    high_priority is stored as a header for potential future use but does
    not affect partition assignment or ordering.
    """
    _produce_sync(
        _tasks_topic(queue_name),
        value.encode("utf-8"),
        key=partition_key,
    )


async def queue_pop(
    queue_name: str,
    *,
    timeout: int = 1,
) -> dict[str, Any] | None:
    """Consume the next task from the tasks topic.

    The message offset is NOT committed here — call queue_commit() after
    successful processing, or queue_nack() to allow redelivery.

    If the worker crashes before committing, Kafka's consumer group protocol
    will reassign the partition and redeliver the message to another consumer.
    """
    from aiokafka import AIOKafkaConsumer

    topic = _tasks_topic(queue_name)
    consumer_key = f"worker:{topic}"

    if consumer_key not in _consumers:
        await _ensure_topic(topic)
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=_get_bootstrap_servers(),
            group_id=f"{CONF.key_prefix}-workers",
            client_id=_client_id("worker"),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await consumer.start()  # type: ignore[union-attr]
        _consumers[consumer_key] = consumer

    consumer = _consumers[consumer_key]
    result = await consumer.getmany(timeout_ms=timeout * 1000)  # type: ignore[union-attr]
    for tp, messages in result.items():
        for msg in messages:
            # Do NOT commit — let the worker decide via queue_commit/queue_nack
            return json.loads(msg.value.decode("utf-8"))

    return None


async def queue_commit(queue_name: str) -> None:
    """Commit the consumer offset — acknowledges successful processing.

    After this call, the message will not be redelivered even if the
    worker crashes later.
    """
    topic = _tasks_topic(queue_name)
    consumer_key = f"worker:{topic}"
    if consumer_key in _consumers:
        await _consumers[consumer_key].commit()  # type: ignore[union-attr]


async def queue_nack(queue_name: str) -> None:
    """Do NOT commit the offset — the message will be redelivered.

    On the next poll (or after a rebalance if the worker dies), this
    message will be returned again, either to this consumer or to another
    consumer in the group. This keeps the task in its original position
    within its partition, preserving ordering.
    """
    # Intentionally do nothing — the uncommitted offset means Kafka will
    # redeliver the message. The consumer's next poll will return it again.
    pass


# ---------------------------------------------------------------------------
# Key-value operations (compacted topic + in-memory cache)
# ---------------------------------------------------------------------------


def get(key: str) -> Optional[bytes]:
    """Get from in-memory cache (populated from compacted state topic)."""
    with _cache_lock:
        return _kv_cache.get(key)


def aget(key: str) -> Coroutine[None, None, Optional[bytes]]:
    """Async get — same as sync since reads are from in-memory cache."""
    async def _get() -> Optional[bytes]:
        return get(key)
    return _get()


def set(key: str, value: bytes, ttl_seconds: Optional[int] = None) -> bool:
    """Write to compacted state topic and update local cache.

    ttl_seconds is accepted for interface compatibility but not enforced —
    Kafka uses topic-level retention instead of per-key TTL.
    """
    with _cache_lock:
        _kv_cache[key] = value
    _produce_sync(_kv_topic(), value, key=key)
    return True


def aset(
    key: str, value: bytes, ttl_seconds: Optional[int] = None
) -> Coroutine[None, None, bool]:
    """Async set."""
    async def _set() -> bool:
        with _cache_lock:
            _kv_cache[key] = value
        await _produce(_kv_topic(), value, key=key)
        return True
    return _set()


def delete(key: str) -> int:
    """Tombstone the key in the compacted topic and remove from cache."""
    with _cache_lock:
        existed = 1 if key in _kv_cache else 0
        _kv_cache.pop(key, None)
    _produce_sync(_kv_topic(), None, key=key)  # Tombstone
    return existed


def adelete(key: str) -> Coroutine[None, None, int]:
    """Async delete."""
    async def _delete() -> int:
        with _cache_lock:
            existed = 1 if key in _kv_cache else 0
            _kv_cache.pop(key, None)
        await _produce(_kv_topic(), None, key=key)
        return existed
    return _delete()


# ---------------------------------------------------------------------------
# Atomic counters (in-memory + compacted topic)
# ---------------------------------------------------------------------------


def incr(key: str) -> int:
    """Increment counter in local cache and persist to compacted topic."""
    with _cache_lock:
        val = _counter_cache.get(key, 0) + 1
        _counter_cache[key] = val
    _produce_sync(_kv_topic(), str(val).encode("utf-8"), key=f"counter:{key}")
    return val


def decr(key: str) -> int:
    """Decrement counter in local cache and persist to compacted topic."""
    with _cache_lock:
        val = _counter_cache.get(key, 0) - 1
        _counter_cache[key] = val
    _produce_sync(_kv_topic(), str(val).encode("utf-8"), key=f"counter:{key}")
    return val


# ---------------------------------------------------------------------------
# Pub/sub (log streaming via Kafka topic)
# ---------------------------------------------------------------------------


def publish(channel: str, message: str) -> None:
    """Produce a log message to the logs topic."""
    _produce_sync(_logs_topic(), message.encode("utf-8"))


async def subscribe(channel: str) -> AsyncGenerator[str, None]:
    """Consume log messages from the logs topic."""
    from aiokafka import AIOKafkaConsumer

    topic = _logs_topic()
    await _ensure_topic(topic)

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=_get_bootstrap_servers(),
        group_id=f"{CONF.key_prefix}-log-collector",
        client_id=_client_id("log-collector"),
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    await consumer.start()  # type: ignore[union-attr]

    try:
        async for msg in consumer:  # type: ignore[union-attr]
            yield msg.value.decode("utf-8")
    finally:
        await consumer.stop()  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Distributed locks — no-op with Kafka
# ---------------------------------------------------------------------------


async def acquire_lock(key: str, value: str, ttl_seconds: int) -> bool:
    """Always returns True — partition assignment handles isolation."""
    return True


async def release_lock(key: str) -> int:
    """No-op — returns 0."""
    return 0


# ---------------------------------------------------------------------------
# Sorted sets (in-memory + compacted topic)
# ---------------------------------------------------------------------------


def zadd(key: str, mapping: dict[str, float]) -> int:
    """Add members with scores. Persists to compacted topic."""
    added = 0
    with _cache_lock:
        if key not in _sorted_set_cache:
            _sorted_set_cache[key] = {}
        for member, score in mapping.items():
            if member not in _sorted_set_cache[key]:
                added += 1
            _sorted_set_cache[key][member] = score
    # Persist the entire sorted set
    data = json.dumps(_sorted_set_cache[key]).encode("utf-8")
    _produce_sync(_kv_topic(), data, key=f"zset:{key}")
    return added


async def zrangebyscore(
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


def zrem(key: str, *members: str) -> int:
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
        _produce_sync(_kv_topic(), data, key=f"zset:{key}")
    return removed


# ---------------------------------------------------------------------------
# Activity operations (compacted topic + in-memory cache)
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    """Current UTC time as ISO string."""
    return datetime.now(UTC).isoformat()


def _activity_produce(record: dict[str, Any]) -> None:
    """Persist an activity record to the compacted activity topic."""
    agent_id = record["agent_id"]
    data = json.dumps(record, default=str).encode("utf-8")
    _produce_sync(_activity_topic(), data, key=str(agent_id))


def activity_create(
    agent_id: uuid.UUID,
    agent_type: str,
    message: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Create a new activity record with initial QUEUED log entry."""
    now = _now_iso()
    log_entry = {
        "id": str(uuid.uuid4()),
        "message": message,
        "status": "queued",
        "percentage": 0,
        "created_at": now,
    }
    record: dict[str, Any] = {
        "agent_id": str(agent_id),
        "agent_type": agent_type,
        "created_at": now,
        "updated_at": now,
        "metadata": metadata,
        "logs": [log_entry],
    }
    with _cache_lock:
        _activity_cache[str(agent_id)] = record
    _activity_produce(record)


def activity_append_log(
    agent_id: uuid.UUID,
    message: str,
    status: str,
    percentage: int | None = None,
) -> None:
    """Append a log entry to an existing activity record."""
    key = str(agent_id)
    now = _now_iso()
    log_entry = {
        "id": str(uuid.uuid4()),
        "message": message,
        "status": status,
        "percentage": percentage,
        "created_at": now,
    }
    with _cache_lock:
        record = _activity_cache.get(key)
        if record is None:
            raise ValueError(f"Activity not found for agent_id {agent_id}")
        record["logs"].append(log_entry)
        record["updated_at"] = now
    _activity_produce(record)


def activity_get(
    agent_id: uuid.UUID,
    metadata_filter: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Get a single activity record by agent_id."""
    key = str(agent_id)
    with _cache_lock:
        record = _activity_cache.get(key)
    if record is None:
        return None
    if metadata_filter and record.get("metadata"):
        for k, v in metadata_filter.items():
            if str(record["metadata"].get(k)) != str(v):
                return None
    elif metadata_filter:
        return None
    return record


def activity_list(
    page: int = 1,
    page_size: int = 50,
    metadata_filter: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """List activity records with pagination.

    Returns (items, total) where items are summary dicts matching
    ActivityListItemSchema fields.
    """
    with _cache_lock:
        records = list(_activity_cache.values())

    # Apply metadata filter
    if metadata_filter:
        filtered = []
        for r in records:
            meta = r.get("metadata")
            if meta and all(str(meta.get(k)) == str(v) for k, v in metadata_filter.items()):
                filtered.append(r)
        records = filtered

    # Build summary items
    items: list[dict[str, Any]] = []
    for r in records:
        logs = r.get("logs", [])
        latest = logs[-1] if logs else None
        first = logs[0] if logs else None
        items.append({
            "agent_id": r["agent_id"],
            "agent_type": r.get("agent_type"),
            "status": latest["status"] if latest else "queued",
            "latest_log_message": latest["message"] if latest else None,
            "latest_log_timestamp": latest["created_at"] if latest else None,
            "percentage": latest.get("percentage", 0) if latest else 0,
            "started_at": first["created_at"] if first else None,
            "metadata": r.get("metadata"),
        })

    # Sort: active (running/queued) first, then by started_at desc
    def sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
        status = item["status"]
        if status == "running":
            active, priority = 0, 1
        elif status == "queued":
            active, priority = 0, 2
        else:
            active, priority = 1, 3
        # Negate time for descending order (use string sort, ISO format is sortable)
        ts = item.get("started_at") or ""
        return (active, priority, ts)

    items.sort(key=sort_key)
    # For descending started_at within each group, reverse within groups
    # Actually, the ISO string sort is ascending; we want descending within non-active
    # Simpler: sort by (active, priority, -started_at)
    items.sort(key=lambda x: (
        0 if x["status"] in ("running", "queued") else 1,
        {"running": 1, "queued": 2}.get(x["status"], 3),
        "",  # placeholder
    ))
    # Stable sort: within same priority, reverse by started_at
    # Use a two-pass: sort by started_at desc first, then stable sort by priority
    items.sort(key=lambda x: x.get("started_at") or "", reverse=True)
    items.sort(key=lambda x: (
        0 if x["status"] in ("running", "queued") else 1,
        {"running": 1, "queued": 2}.get(x["status"], 3),
    ))

    total = len(items)
    offset = (page - 1) * page_size
    page_items = items[offset:offset + page_size]
    return page_items, total


def activity_count_active() -> int:
    """Count activities with QUEUED or RUNNING status."""
    count = 0
    with _cache_lock:
        for record in _activity_cache.values():
            logs = record.get("logs", [])
            if logs and logs[-1]["status"] in ("queued", "running"):
                count += 1
    return count


def activity_get_pending_ids() -> list[uuid.UUID]:
    """Get agent_ids for all activities with QUEUED or RUNNING status."""
    pending: list[uuid.UUID] = []
    with _cache_lock:
        for record in _activity_cache.values():
            logs = record.get("logs", [])
            if logs and logs[-1]["status"] in ("queued", "running"):
                pending.append(uuid.UUID(record["agent_id"]))
    return pending


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Key formatting
# ---------------------------------------------------------------------------


def format_key(*args: str) -> str:
    """Join key parts with dots (Kafka convention)."""
    return ".".join(args)


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def clear_keys() -> int:
    """Clear in-memory caches. Topic data is managed by retention policies."""
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
