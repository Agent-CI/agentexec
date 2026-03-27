"""Kafka implementation of the stream backend protocol.

Provides topic-based message production and consumption via Apache Kafka.
This module is loaded dynamically by the state layer based on configuration.

Requires the ``aiokafka`` package::

    pip install agentexec[kafka]
"""

from __future__ import annotations

import asyncio
import threading
from typing import AsyncGenerator

from agentexec.config import CONF
from agentexec.state.stream_backend import StreamRecord

__all__ = [
    "close",
    "produce",
    "produce_sync",
    "consume",
    "ensure_topic",
    "delete_topic",
    "put",
    "tombstone",
]

# Lazy imports — aiokafka is an optional dependency
_producer: object | None = None  # aiokafka.AIOKafkaProducer
_consumers: dict[str, object] = {}  # group_id -> aiokafka.AIOKafkaConsumer
_admin: object | None = None  # aiokafka.admin.AIOKafkaAdminClient
_sync_lock = threading.Lock()
_loop: asyncio.AbstractEventLoop | None = None


def _get_bootstrap_servers() -> str:
    """Get Kafka bootstrap servers from configuration."""
    if CONF.kafka_bootstrap_servers is None:
        raise ValueError(
            "KAFKA_BOOTSTRAP_SERVERS must be configured "
            "(e.g. 'localhost:9092' or 'broker1:9092,broker2:9092')"
        )
    return CONF.kafka_bootstrap_servers


async def _get_producer():  # type: ignore[no-untyped-def]
    """Get or create the shared Kafka producer."""
    global _producer

    if _producer is None:
        from aiokafka import AIOKafkaProducer

        _producer = AIOKafkaProducer(
            bootstrap_servers=_get_bootstrap_servers(),
            client_id=f"{CONF.key_prefix}-producer",
            acks="all",
            # Ensure ordering: only one in-flight request per connection
            max_batch_size=CONF.kafka_max_batch_size,
            linger_ms=CONF.kafka_linger_ms,
        )
        await _producer.start()  # type: ignore[union-attr]

    return _producer


async def _get_admin():  # type: ignore[no-untyped-def]
    """Get or create the shared Kafka admin client."""
    global _admin

    if _admin is None:
        from aiokafka.admin import AIOKafkaAdminClient

        _admin = AIOKafkaAdminClient(
            bootstrap_servers=_get_bootstrap_servers(),
            client_id=f"{CONF.key_prefix}-admin",
        )
        await _admin.start()  # type: ignore[union-attr]

    return _admin


# -- Connection management ----------------------------------------------------


async def close() -> None:
    """Close all Kafka connections (producer, consumers, admin)."""
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


# -- Produce ------------------------------------------------------------------


async def produce(
    topic: str,
    value: bytes,
    *,
    key: str | None = None,
    headers: dict[str, bytes] | None = None,
) -> None:
    """Produce a message to a topic.

    Args:
        topic: Target topic name.
        value: Message payload.
        key: Optional partition key. Messages with the same key are routed
            to the same partition, guaranteeing ordering for that key.
        headers: Optional message headers.
    """
    producer = await _get_producer()
    key_bytes = key.encode("utf-8") if key is not None else None
    header_list = [(k, v) for k, v in headers.items()] if headers else None

    await producer.send_and_wait(  # type: ignore[union-attr]
        topic,
        value=value,
        key=key_bytes,
        headers=header_list,
    )


def produce_sync(
    topic: str,
    value: bytes,
    *,
    key: str | None = None,
    headers: dict[str, bytes] | None = None,
) -> None:
    """Produce a message synchronously.

    Runs the async produce in the existing event loop or creates a new one.
    Used from synchronous contexts like logging handlers.
    """
    try:
        loop = asyncio.get_running_loop()
        # We're in an async context — schedule as a task
        # and use a threading event to block until done.
        import concurrent.futures

        future: concurrent.futures.Future[None] = concurrent.futures.Future()

        async def _do() -> None:
            try:
                await produce(topic, value, key=key, headers=headers)
                future.set_result(None)
            except Exception as e:
                future.set_exception(e)

        loop.create_task(_do())
        # Don't block if we're on the event loop thread — fire and forget
    except RuntimeError:
        # No running loop — safe to use asyncio.run
        asyncio.run(produce(topic, value, key=key, headers=headers))


# -- Consume ------------------------------------------------------------------


async def consume(
    topic: str,
    group_id: str,
    *,
    timeout_ms: int = 1000,
) -> AsyncGenerator[StreamRecord, None]:
    """Consume messages from a topic as an async generator.

    Each call creates or reuses a consumer for the given group_id.
    Offsets are committed after each message (at-least-once).

    Args:
        topic: Topic to consume from.
        group_id: Consumer group ID.
        timeout_ms: Poll timeout in milliseconds.

    Yields:
        StreamRecord instances.
    """
    from aiokafka import AIOKafkaConsumer

    consumer_key = f"{group_id}:{topic}"

    if consumer_key not in _consumers:
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=_get_bootstrap_servers(),
            group_id=group_id,
            client_id=f"{CONF.key_prefix}-{group_id}",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await consumer.start()
        _consumers[consumer_key] = consumer
    else:
        consumer = _consumers[consumer_key]

    try:
        while True:
            result = await consumer.getmany(timeout_ms=timeout_ms)  # type: ignore[union-attr]
            for tp, messages in result.items():
                for msg in messages:
                    yield StreamRecord(
                        topic=msg.topic,
                        key=msg.key.decode("utf-8") if msg.key else None,
                        value=msg.value,
                        headers=dict(msg.headers) if msg.headers else {},
                        partition=msg.partition,
                        offset=msg.offset,
                        timestamp=msg.timestamp,
                    )
                    await consumer.commit()  # type: ignore[union-attr]
    finally:
        await consumer.stop()  # type: ignore[union-attr]
        _consumers.pop(consumer_key, None)


# -- Topic management --------------------------------------------------------


async def ensure_topic(
    topic: str,
    *,
    num_partitions: int | None = None,
    compact: bool = False,
    retention_ms: int | None = None,
) -> None:
    """Ensure a topic exists, creating it if necessary.

    Args:
        topic: Topic name.
        num_partitions: Number of partitions (defaults to kafka_default_partitions).
        compact: If True, enable log compaction.
        retention_ms: Optional retention period in ms.
    """
    from aiokafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = await _get_admin()

    partitions = num_partitions or CONF.kafka_default_partitions

    topic_config: dict[str, str] = {}
    if compact:
        topic_config["cleanup.policy"] = "compact"
    if retention_ms is not None:
        topic_config["retention.ms"] = str(retention_ms)

    new_topic = NewTopic(
        name=topic,
        num_partitions=partitions,
        replication_factor=CONF.kafka_replication_factor,
        topic_configs=topic_config,
    )

    try:
        await admin.create_topics([new_topic])  # type: ignore[union-attr]
    except TopicAlreadyExistsError:
        pass


async def delete_topic(topic: str) -> None:
    """Delete a topic and all its data."""
    admin = await _get_admin()
    await admin.delete_topics([topic])  # type: ignore[union-attr]


# -- Compacted topic helpers --------------------------------------------------


async def put(topic: str, key: str, value: bytes) -> None:
    """Write a keyed record to a compacted topic."""
    await produce(topic, value, key=key)


async def tombstone(topic: str, key: str) -> None:
    """Write a tombstone (null value) to delete a key from a compacted topic."""
    producer = await _get_producer()
    await producer.send_and_wait(  # type: ignore[union-attr]
        topic,
        value=None,
        key=key.encode("utf-8"),
    )
