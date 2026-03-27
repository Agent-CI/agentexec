"""Kafka queue operations using manual partition assignment (no consumer groups)."""

from __future__ import annotations

import json
from typing import Any

from agentexec.state.kafka_backend.connection import (
    client_id,
    ensure_topic,
    get_bootstrap_servers,
    get_consumers,
    get_topic_partitions,
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
    topic = tasks_topic(queue_name)
    await produce(
        topic,
        value.encode("utf-8"),
        key=partition_key,
    )
    print(f"[queue_push] produced to topic={topic}")


async def queue_pop(
    queue_name: str,
    *,
    timeout: int = 1,
) -> dict[str, Any] | None:
    """Consume the next task from the tasks topic.

    Uses manual partition assignment without consumer groups to avoid
    group-join/rebalance overhead entirely.  Partition info comes from
    the admin client metadata.
    """
    from aiokafka import AIOKafkaConsumer, TopicPartition

    topic = tasks_topic(queue_name)
    consumer_key = f"worker:{topic}"
    consumers = get_consumers()

    if consumer_key not in consumers:
        await ensure_topic(topic)

        # Get partition info from admin metadata (not consumer metadata)
        partition_ids = await get_topic_partitions(topic)
        tps = [TopicPartition(topic, p) for p in partition_ids]
        print(f"[queue_pop] topic={topic} partitions={partition_ids} tps={tps}")

        consumer = AIOKafkaConsumer(
            bootstrap_servers=get_bootstrap_servers(),
            client_id=client_id("worker"),
            enable_auto_commit=False,
        )
        await consumer.start()
        consumer.assign(tps)
        await consumer.seek_to_beginning(*tps)
        print(f"[queue_pop] assigned + seeked, assignment={consumer.assignment()}")

        consumers[consumer_key] = consumer

    consumer = consumers[consumer_key]

    # Poll with retries — first call may return empty while position settles
    deadline = timeout * 1000
    interval = min(1000, deadline)
    elapsed = 0
    while elapsed < deadline:
        result = await consumer.getmany(timeout_ms=interval)
        for tp, messages in result.items():
            for msg in messages:
                return json.loads(msg.value.decode("utf-8"))
        elapsed += interval

    # Debug: print consumer state when no messages found
    assignment = consumer.assignment()
    positions = {}
    for tp in assignment:
        try:
            positions[str(tp)] = await consumer.position(tp)
        except Exception as e:
            positions[str(tp)] = f"error: {e}"
    print(f"[queue_pop] TIMEOUT topic={topic} assignment={assignment} positions={positions}")

    return None


async def queue_commit(queue_name: str) -> None:
    """No-op — offset tracking is implicit via consumer position."""
    pass


async def queue_nack(queue_name: str) -> None:
    """Do NOT commit the offset — the message will be redelivered."""
    pass
