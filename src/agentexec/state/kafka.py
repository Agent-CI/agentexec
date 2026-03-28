from __future__ import annotations

import asyncio
import json
import os
import socket
import time
import threading
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, AsyncGenerator, Optional
from uuid import UUID

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from agentexec.config import CONF
from agentexec.state.base import BaseBackend, BaseQueueBackend, BaseScheduleBackend, BaseStateBackend



class Backend(BaseBackend):
    """Kafka implementation of the agentexec backend."""

    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None
        self._consumers: dict[str, AIOKafkaConsumer] = {}
        self._admin: AIOKafkaAdminClient | None = None

        self._cache_lock = threading.Lock()
        self._initialized_topics: set[str] = set()
        self._worker_id: str | None = None

        # In-memory caches
        self._kv_cache: dict[str, bytes] = {}
        self._counter_cache: dict[str, int] = {}
        self._sorted_set_cache: dict[str, dict[str, float]] = defaultdict(dict)

        # Sub-backends
        self.state = KafkaStateBackend(self)
        self.queue = KafkaQueueBackend(self)
        self.schedule = KafkaScheduleBackend(self)

    def format_key(self, *args: str) -> str:
        return ".".join(args)

    def configure(self, **kwargs: Any) -> None:
        self._worker_id = kwargs.get("worker_id")

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

        for consumer in self._consumers.values():
            await consumer.stop()
        self._consumers.clear()

        if self._admin is not None:
            await self._admin.close()
            self._admin = None

    def _get_bootstrap_servers(self) -> str:
        if CONF.kafka_bootstrap_servers is None:
            raise ValueError(
                "KAFKA_BOOTSTRAP_SERVERS must be configured "
                "(e.g. 'localhost:9092' or 'broker1:9092,broker2:9092')"
            )
        return CONF.kafka_bootstrap_servers

    def _client_id(self, role: str = "worker") -> str:
        base = f"{CONF.key_prefix}-{role}-{socket.gethostname()}"
        if self._worker_id is not None:
            return f"{base}-{self._worker_id}"
        return base

    async def _get_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._get_bootstrap_servers(),
                client_id=self._client_id("producer"),
                acks="all",
                max_batch_size=CONF.kafka_max_batch_size,
                linger_ms=CONF.kafka_linger_ms,
            )
            await self._producer.start()
        return self._producer

    async def _get_admin(self) -> AIOKafkaAdminClient:
        if self._admin is None:
            self._admin = AIOKafkaAdminClient(
                bootstrap_servers=self._get_bootstrap_servers(),
                client_id=self._client_id("admin"),
            )
            await self._admin.start()
        return self._admin

    async def produce(
        self,
        topic: str,
        value: bytes | None,
        key: str | bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        producer = await self._get_producer()
        if isinstance(key, str):
            key_bytes = key.encode("utf-8")
        else:
            key_bytes = key
        header_list = [(k, v.encode("utf-8")) for k, v in headers.items()] if headers else None
        await producer.send_and_wait(topic, value=value, key=key_bytes, headers=header_list)

    async def ensure_topic(self, topic: str, *, compact: bool = True) -> None:
        if topic in self._initialized_topics:
            return

        admin = await self._get_admin()
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

        self._initialized_topics.add(topic)

    async def _get_topic_partitions(self, topic: str) -> list[TopicPartition]:
        admin = await self._get_admin()
        topics_meta = await admin.describe_topics([topic])
        for t in topics_meta:
            if t.get("topic") == topic:
                parts = t.get("partitions", [])
                if parts:
                    return [
                        TopicPartition(topic, p["partition"])
                        for p in sorted(parts, key=lambda p: p["partition"])
                    ]
        return [TopicPartition(topic, 0)]

    def tasks_topic(self, queue_name: str) -> str:
        return f"{CONF.key_prefix}.tasks.{queue_name}"

    def kv_topic(self) -> str:
        return f"{CONF.key_prefix}.state"



    def schedule_topic(self) -> str:
        return f"{CONF.key_prefix}.schedules"


class KafkaStateBackend(BaseStateBackend):
    """Kafka state: compacted topics + in-memory caches."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    async def get(self, key: str) -> Optional[bytes]:
        with self.backend._cache_lock:
            return self.backend._kv_cache.get(key)

    async def set(self, key: str, value: bytes, ttl_seconds: Optional[int] = None) -> bool:
        topic = self.backend.kv_topic()
        await self.backend.ensure_topic(topic)
        with self.backend._cache_lock:
            self.backend._kv_cache[key] = value
        await self.backend.produce(topic, value, key=key)
        return True

    async def delete(self, key: str) -> int:
        topic = self.backend.kv_topic()
        await self.backend.ensure_topic(topic)
        with self.backend._cache_lock:
            existed = 1 if key in self.backend._kv_cache else 0
            self.backend._kv_cache.pop(key, None)
        await self.backend.produce(topic, None, key=key)  # Tombstone
        return existed

    async def counter_incr(self, key: str) -> int:
        topic = self.backend.kv_topic()
        await self.backend.ensure_topic(topic)
        with self.backend._cache_lock:
            val = self.backend._counter_cache.get(key, 0) + 1
            self.backend._counter_cache[key] = val
        await self.backend.produce(topic, str(val).encode("utf-8"), key=f"counter:{key}")
        return val

    async def counter_decr(self, key: str) -> int:
        topic = self.backend.kv_topic()
        await self.backend.ensure_topic(topic)
        with self.backend._cache_lock:
            val = self.backend._counter_cache.get(key, 0) - 1
            self.backend._counter_cache[key] = val
        await self.backend.produce(topic, str(val).encode("utf-8"), key=f"counter:{key}")
        return val

    async def publish(self, channel: str, message: str) -> None:
        await self.backend.ensure_topic(channel, compact=False)
        await self.backend.produce(channel, message.encode("utf-8"))

    async def subscribe(self, channel: str) -> AsyncGenerator[str, None]:
        await self.backend.ensure_topic(channel, compact=False)
        topic_partitions = await self.backend._get_topic_partitions(channel)

        consumer = AIOKafkaConsumer(
            bootstrap_servers=self.backend._get_bootstrap_servers(),
            client_id=self.backend._client_id("subscriber"),
            enable_auto_commit=False,
        )
        await consumer.start()
        consumer.assign(topic_partitions)
        await consumer.seek_to_end(*topic_partitions)

        try:
            async for msg in consumer:
                yield msg.value.decode("utf-8")
        finally:
            await consumer.stop()

    async def acquire_lock(self, lock_key: str, agent_id: UUID) -> bool:
        return True  # Partition assignment handles isolation

    async def release_lock(self, lock_key: str) -> int:
        return 0

    async def index_add(self, key: str, mapping: dict[str, float]) -> int:
        topic = self.backend.kv_topic()
        await self.backend.ensure_topic(topic)
        added = 0
        with self.backend._cache_lock:
            if key not in self.backend._sorted_set_cache:
                self.backend._sorted_set_cache[key] = {}
            for member, score in mapping.items():
                if member not in self.backend._sorted_set_cache[key]:
                    added += 1
                self.backend._sorted_set_cache[key][member] = score
        data = json.dumps(self.backend._sorted_set_cache[key]).encode("utf-8")
        await self.backend.produce(topic, data, key=f"zset:{key}")
        return added

    async def index_range(self, key: str, min_score: float, max_score: float) -> list[bytes]:
        with self.backend._cache_lock:
            members = self.backend._sorted_set_cache.get(key, {})
            return [
                member.encode("utf-8")
                for member, score in members.items()
                if min_score <= score <= max_score
            ]

    async def index_remove(self, key: str, *members: str) -> int:
        removed = 0
        with self.backend._cache_lock:
            if key in self.backend._sorted_set_cache:
                for member in members:
                    if member in self.backend._sorted_set_cache[key]:
                        del self.backend._sorted_set_cache[key][member]
                        removed += 1
        if removed > 0:
            topic = self.backend.kv_topic()
            await self.backend.ensure_topic(topic)
            data = json.dumps(self.backend._sorted_set_cache.get(key, {})).encode("utf-8")
            await self.backend.produce(topic, data, key=f"zset:{key}")
        return removed

    async def clear(self) -> int:
        with self.backend._cache_lock:
            count = (
                len(self.backend._kv_cache) + len(self.backend._counter_cache)
                + len(self.backend._sorted_set_cache)
            )
            self.backend._kv_cache.clear()
            self.backend._counter_cache.clear()
            self.backend._sorted_set_cache.clear()
        return count


class KafkaQueueBackend(BaseQueueBackend):
    """Kafka queue: consumer groups for reliable fan-out."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    async def _get_consumer(self, topic: str) -> AIOKafkaConsumer:
        consumers = self.backend._consumers

        if topic not in consumers:
            await self.backend.ensure_topic(topic, compact=False)

            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=self.backend._get_bootstrap_servers(),
                group_id=f"{CONF.key_prefix}-workers",
                client_id=self.backend._client_id("worker"),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
            )
            await consumer.start()
            consumers[topic] = consumer

        return consumers[topic]

    async def push(
        self,
        queue_name: str,
        value: str,
        *,
        high_priority: bool = False,
        partition_key: str | None = None,
    ) -> None:
        topic = self.backend.tasks_topic(queue_name)
        await self.backend.ensure_topic(topic, compact=False)

        # Extract metadata for headers without altering the payload
        task_data = json.loads(value)
        headers = {
            "ax_task_name": task_data.get("task_name", ""),
            "ax_agent_id": task_data.get("agent_id", ""),
            "ax_retry_count": str(task_data.get("retry_count", 0)),
        }
        await self.backend.produce(topic, value.encode("utf-8"), key=partition_key, headers=headers)

    async def pop(
        self,
        queue_name: str,
        *,
        timeout: int = 1,
    ) -> dict[str, Any] | None:
        consumer = await self._get_consumer(self.backend.tasks_topic(queue_name))

        try:
            msg = await asyncio.wait_for(
                consumer.getone(),
                timeout=timeout,
            )
            await consumer.commit()
            return json.loads(msg.value.decode("utf-8"))
        except asyncio.TimeoutError:
            return None

    async def release_lock(self, queue_name: str, partition_key: str) -> None:
        pass  # Kafka uses partition assignment, no explicit locks


class KafkaScheduleBackend(BaseScheduleBackend):
    """Kafka schedule: compacted topic + in-memory cache."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self._consumer: AIOKafkaConsumer | None = None
        self._tps: list[TopicPartition] = []

    async def _ensure_consumer(self) -> AIOKafkaConsumer:
        topic = self.backend.schedule_topic()
        await self.backend.ensure_topic(topic)

        if self._consumer is None:
            self._tps = await self.backend._get_topic_partitions(topic)
            self._consumer = AIOKafkaConsumer(
                bootstrap_servers=self.backend._get_bootstrap_servers(),
                client_id=self.backend._client_id("scheduler"),
                enable_auto_commit=False,
            )
            await self._consumer.start()
            self._consumer.assign(self._tps)

        return self._consumer

    async def register(self, task: ScheduledTask) -> None:
        topic = self.backend.schedule_topic()
        await self.backend.ensure_topic(topic)
        data = task.model_dump_json().encode("utf-8")
        headers = {
            "ax_task_name": task.task_name,
            "ax_cron": task.cron,
            "ax_next_run": str(task.next_run),
            "ax_repeat": str(task.repeat),
        }
        await self.backend.produce(topic, data, key=task.task_name, headers=headers)

    async def get_due(self) -> list[ScheduledTask]:
        # TODO: this replays the entire compacted topic on every poll —
        # seek, iterate, deserialize, compare for each schedule. Consider
        # caching with invalidation or using message timestamps to skip
        # schedules that aren't close to due.
        from agentexec.schedule import ScheduledTask
        from pydantic import ValidationError

        consumer = await self._ensure_consumer()
        await consumer.seek_to_beginning(*self._tps)

        now = time.time()
        due = []
        records = await consumer.getmany(*self._tps, timeout_ms=1000)
        for tp_records in records.values():
            for msg in tp_records:
                if msg.value is None:
                    continue
                try:
                    task = ScheduledTask.model_validate_json(msg.value)
                    if task.next_run <= now:
                        due.append(task)
                except ValidationError:
                    continue

        return due

    async def remove(self, task_name: str) -> None:
        topic = self.backend.schedule_topic()
        await self.backend.ensure_topic(topic)
        await self.backend.produce(topic, None, key=task_name)
