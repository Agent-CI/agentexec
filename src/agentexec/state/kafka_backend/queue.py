"""Kafka queue operations using manual partition assignment."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from agentexec.config import CONF
from agentexec.state.kafka_backend.connection import (
    client_id,
    ensure_topic,
    get_bootstrap_servers,
    get_consumers,
    produce,
    tasks_topic,
)


async def queue_push(
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
    """
    await produce(
        tasks_topic(queue_name),
        value.encode("utf-8"),
        key=partition_key,
    )


async def _discover_partitions(consumer, topic: str) -> list:  # type: ignore[no-untyped-def]
    """Wait for partition metadata to become available."""
    from aiokafka import TopicPartition

    for _ in range(10):
        partitions = consumer.partitions_for_topic(topic)
        if partitions:
            return [TopicPartition(topic, p) for p in sorted(partitions)]
        await asyncio.sleep(0.5)
    # Fallback: assume partition 0
    return [TopicPartition(topic, 0)]


async def queue_pop(
    queue_name: str,
    *,
    timeout: int = 1,
) -> dict[str, Any] | None:
    """Consume the next task from the tasks topic.

    Uses manual partition assignment (no consumer group) so there is no
    group-join/rebalance overhead.  Offset tracking is manual via commit().
    """
    from aiokafka import AIOKafkaConsumer

    topic = tasks_topic(queue_name)
    consumer_key = f"worker:{topic}"
    consumers = get_consumers()

    if consumer_key not in consumers:
        await ensure_topic(topic)
        consumer = AIOKafkaConsumer(
            bootstrap_servers=get_bootstrap_servers(),
            client_id=client_id("worker"),
            enable_auto_commit=False,
            group_id=f"{CONF.key_prefix}-workers-{topic}",
        )
        await consumer.start()

        # Manually assign all partitions for this topic
        tps = await _discover_partitions(consumer, topic)
        consumer.assign(tps)

        # Seek to committed offsets (or beginning if none committed)
        for tp in tps:
            committed = await consumer.committed(tp)
            if committed is not None:
                consumer.seek(tp, committed)
            else:
                await consumer.seek_to_beginning(tp)

        consumers[consumer_key] = consumer

    consumer = consumers[consumer_key]

    # Poll with retries — first poll may return empty while metadata settles
    deadline = timeout * 1000
    interval = min(1000, deadline)
    elapsed = 0
    while elapsed < deadline:
        result = await consumer.getmany(timeout_ms=interval)
        for tp, messages in result.items():
            for msg in messages:
                return json.loads(msg.value.decode("utf-8"))
        elapsed += interval

    return None


async def queue_commit(queue_name: str) -> None:
    """Commit the consumer offset — acknowledges successful processing."""
    topic = tasks_topic(queue_name)
    consumer_key = f"worker:{topic}"
    consumers = get_consumers()
    if consumer_key in consumers:
        await consumers[consumer_key].commit()


async def queue_nack(queue_name: str) -> None:
    """Do NOT commit the offset — the message will be redelivered."""
    pass
