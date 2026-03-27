"""Kafka connection management — producer, admin, consumers, topic lifecycle."""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any

from agentexec.config import CONF

# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------

_producer: object | None = None  # AIOKafkaProducer
_consumers: dict[str, object] = {}  # consumer_key -> AIOKafkaConsumer
_admin: object | None = None  # AIOKafkaAdminClient

_cache_lock = threading.Lock()
_initialized_topics: set[str] = set()
_worker_id: str | None = None


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def configure(*, worker_id: str | None = None) -> None:
    """Set per-process identity for Kafka client IDs.

    Called by Worker.run() before any Kafka operations so that broker
    logs and monitoring tools can distinguish between consumers.
    """
    global _worker_id
    _worker_id = worker_id


def client_id(role: str = "worker") -> str:
    """Build a client_id string, including worker_id when available."""
    base = f"{CONF.key_prefix}-{role}"
    if _worker_id is not None:
        return f"{base}-{_worker_id}"
    return base


def get_bootstrap_servers() -> str:
    if CONF.kafka_bootstrap_servers is None:
        raise ValueError(
            "KAFKA_BOOTSTRAP_SERVERS must be configured "
            "(e.g. 'localhost:9092' or 'broker1:9092,broker2:9092')"
        )
    return CONF.kafka_bootstrap_servers


# ---------------------------------------------------------------------------
# Topic naming conventions
# ---------------------------------------------------------------------------


def tasks_topic(queue_name: str) -> str:
    return f"{CONF.key_prefix}.tasks.{queue_name}"


def kv_topic() -> str:
    return f"{CONF.key_prefix}.state"


def logs_topic() -> str:
    return f"{CONF.key_prefix}.logs"


def activity_topic() -> str:
    return f"{CONF.key_prefix}.activity"


# ---------------------------------------------------------------------------
# Producer / Admin helpers
# ---------------------------------------------------------------------------


async def get_producer():  # type: ignore[no-untyped-def]
    global _producer
    if _producer is None:
        from aiokafka import AIOKafkaProducer

        _producer = AIOKafkaProducer(
            bootstrap_servers=get_bootstrap_servers(),
            client_id=client_id("producer"),
            acks="all",
            max_batch_size=CONF.kafka_max_batch_size,
            linger_ms=CONF.kafka_linger_ms,
        )
        await _producer.start()  # type: ignore[union-attr]
    return _producer


async def get_admin():  # type: ignore[no-untyped-def]
    global _admin
    if _admin is None:
        from aiokafka.admin import AIOKafkaAdminClient

        _admin = AIOKafkaAdminClient(
            bootstrap_servers=get_bootstrap_servers(),
            client_id=client_id("admin"),
        )
        await _admin.start()  # type: ignore[union-attr]
    return _admin


async def produce(topic: str, value: bytes | None, key: str | bytes | None = None) -> None:
    """Produce a message. key=None means unkeyed."""
    producer = await get_producer()
    if isinstance(key, str):
        key_bytes = key.encode("utf-8")
    else:
        key_bytes = key
    await producer.send_and_wait(topic, value=value, key=key_bytes)  # type: ignore[union-attr]


def produce_sync(topic: str, value: bytes | None, key: str | None = None) -> None:
    """Produce from synchronous context."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(produce(topic, value, key))
    except RuntimeError:
        asyncio.run(produce(topic, value, key))


async def ensure_topic(topic: str, *, compact: bool = False) -> None:
    """Create a topic if it doesn't exist."""
    if topic in _initialized_topics:
        return

    from aiokafka.admin import NewTopic

    admin = await get_admin()
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
        pass  # Topic already exists

    _initialized_topics.add(topic)


async def get_topic_partitions(topic: str) -> list[int]:
    """Get partition IDs for a topic via the admin client's metadata."""
    admin = await get_admin()
    topics_meta = await admin.describe_topics([topic])  # type: ignore[union-attr]
    for t in topics_meta:
        if t.get("topic") == topic:
            parts = t.get("partitions", [])
            if parts:
                return sorted(p["partition"] for p in parts)
    return [0]


def get_consumers() -> dict[str, object]:
    """Access the consumers dict (used by queue module)."""
    return _consumers


# ---------------------------------------------------------------------------
# Connection lifecycle
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
