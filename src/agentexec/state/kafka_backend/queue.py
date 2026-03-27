"""Kafka queue operations using consumer groups for reliable fan-out."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaConsumer

from agentexec.config import CONF
from agentexec.state.kafka_backend.connection import (
    client_id,
    ensure_topic,
    get_bootstrap_servers,
    get_consumers,
    produce,
    tasks_topic,
)


async def _get_consumer(topic: str) -> AIOKafkaConsumer:
    """Return a consumer for the given topic, creating one if needed.

    Uses a shared consumer group so Kafka assigns partitions across
    workers — each message is delivered to exactly one consumer.
    """
    active_consumers = get_consumers()

    if topic not in active_consumers:
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=get_bootstrap_servers(),
            group_id=f"{CONF.key_prefix}-workers",
            client_id=client_id("worker"),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await consumer.start()
        active_consumers[topic] = consumer

    return active_consumers[topic]


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
    topic = tasks_topic(queue_name)
    await ensure_topic(topic, compact=False)
    await produce(topic, value.encode("utf-8"), key=partition_key)


async def queue_pop(
    queue_name: str,
    *,
    timeout: int = 1,
) -> dict[str, Any] | None:
    """Consume the next task from the tasks topic.

    The message offset is committed after successful retrieval so Kafka
    tracks consumer progress. Retry logic is handled by the caller via
    requeue with an incremented retry_count.
    """
    consumer = await _get_consumer(tasks_topic(queue_name))

    try:
        msg = await asyncio.wait_for(
            consumer.getone(),
            timeout=timeout,
        )
        await consumer.commit()
        return json.loads(msg.value.decode("utf-8"))
    except asyncio.TimeoutError:
        return None
