"""Kafka connection management — producer, admin, consumers, topic lifecycle."""

from __future__ import annotations

import asyncio
import os
import socket
import threading

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from agentexec.config import CONF

# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------

_producer: AIOKafkaProducer | None = None
_consumers: dict[str, AIOKafkaConsumer] = {}
_admin: AIOKafkaAdminClient | None = None

_cache_lock = threading.Lock()
_initialized_topics: set[str] = set()
# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


_worker_id: str | None = None


def configure(*, worker_id: str | None = None) -> None:
    """Set the worker index for this process."""
    global _worker_id
    _worker_id = worker_id


def client_id(role: str = "worker") -> str:
    """Build a globally unique client_id string."""
    base = f"{CONF.key_prefix}-{role}-{socket.gethostname()}"
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


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=get_bootstrap_servers(),
            client_id=client_id("producer"),
            acks="all",
            max_batch_size=CONF.kafka_max_batch_size,
            linger_ms=CONF.kafka_linger_ms,
        )
        await _producer.start()
    return _producer


async def get_admin() -> AIOKafkaAdminClient:
    global _admin
    if _admin is None:
        _admin = AIOKafkaAdminClient(
            bootstrap_servers=get_bootstrap_servers(),
            client_id=client_id("admin"),
        )
        await _admin.start()
    return _admin


async def produce(topic: str, value: bytes | None, key: str | bytes | None = None) -> None:
    """Produce a message. key=None means unkeyed."""
    producer = await get_producer()
    if isinstance(key, str):
        key_bytes = key.encode("utf-8")
    else:
        key_bytes = key
    await producer.send_and_wait(topic, value=value, key=key_bytes)



async def ensure_topic(topic: str, *, compact: bool = True) -> None:
    """Create a topic if it doesn't exist.

    Topics default to compacted with configurable retention so that
    state is never silently lost.
    """
    if topic in _initialized_topics:
        return

    admin = await get_admin()
    config: dict[str, str] = {}
    if compact:
        config["cleanup.policy"] = "compact"
        config["retention.ms"] = str(CONF.kafka_retention_ms)

    try:
        await admin.create_topics(
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


async def get_topic_partitions(topic: str) -> list[TopicPartition]:
    """Get partitions for a topic via the admin client's metadata."""
    admin = await get_admin()
    topics_meta = await admin.describe_topics([topic])
    for t in topics_meta:
        if t.get("topic") == topic:
            parts = t.get("partitions", [])
            if parts:
                return [TopicPartition(topic, p["partition"]) for p in sorted(parts, key=lambda p: p["partition"])]
    return [TopicPartition(topic, 0)]


def get_consumers() -> dict[str, AIOKafkaConsumer]:
    """Access the consumers dict (used by queue module)."""
    return _consumers


# ---------------------------------------------------------------------------
# Connection lifecycle
# ---------------------------------------------------------------------------


async def close() -> None:
    """Close all Kafka connections."""
    global _producer, _admin

    if _producer is not None:
        await _producer.stop()
        _producer = None

    for consumer in _consumers.values():
        await consumer.stop()
    _consumers.clear()

    if _admin is not None:
        await _admin.close()
        _admin = None
