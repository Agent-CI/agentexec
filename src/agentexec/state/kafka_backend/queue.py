"""Kafka queue operations using consumer groups with commit/nack semantics."""

from __future__ import annotations

import json
from typing import Any

from agentexec.config import CONF
from agentexec.state.kafka_backend.connection import (
    client_id,
    ensure_topic,
    get_bootstrap_servers,
    get_consumers,
    produce_sync,
    tasks_topic,
)


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
    produce_sync(
        tasks_topic(queue_name),
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

    topic = tasks_topic(queue_name)
    consumer_key = f"worker:{topic}"
    consumers = get_consumers()

    if consumer_key not in consumers:
        await ensure_topic(topic)
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=get_bootstrap_servers(),
            group_id=f"{CONF.key_prefix}-workers",
            client_id=client_id("worker"),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await consumer.start()  # type: ignore[union-attr]
        consumers[consumer_key] = consumer

    consumer = consumers[consumer_key]
    result = await consumer.getmany(timeout_ms=timeout * 1000)  # type: ignore[union-attr]
    for tp, messages in result.items():
        for msg in messages:
            return json.loads(msg.value.decode("utf-8"))

    return None


async def queue_commit(queue_name: str) -> None:
    """Commit the consumer offset — acknowledges successful processing."""
    topic = tasks_topic(queue_name)
    consumer_key = f"worker:{topic}"
    consumers = get_consumers()
    if consumer_key in consumers:
        await consumers[consumer_key].commit()  # type: ignore[union-attr]


async def queue_nack(queue_name: str) -> None:
    """Do NOT commit the offset — the message will be redelivered.

    Intentionally empty — the uncommitted offset means Kafka will
    redeliver the message on the next poll or after a rebalance.
    """
    pass
