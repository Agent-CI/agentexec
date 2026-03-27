"""Kafka backend — class-based implementation."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import threading
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, AsyncGenerator, Optional
from uuid import UUID

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from agentexec.config import CONF
from agentexec.state.base import BaseActivityBackend, BaseBackend, BaseQueueBackend, BaseStateBackend


class KafkaBackend(BaseBackend):
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
        self._activity_cache: dict[str, dict[str, Any]] = {}

        # Sub-backends
        self.state = KafkaStateBackend(self)
        self.queue = KafkaQueueBackend(self)
        self.activity = KafkaActivityBackend(self)

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

    # -- Connection helpers ---------------------------------------------------

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

    async def produce(self, topic: str, value: bytes | None, key: str | bytes | None = None) -> None:
        producer = await self._get_producer()
        if isinstance(key, str):
            key_bytes = key.encode("utf-8")
        else:
            key_bytes = key
        await producer.send_and_wait(topic, value=value, key=key_bytes)

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

    # -- Topic naming ---------------------------------------------------------

    def tasks_topic(self, queue_name: str) -> str:
        return f"{CONF.key_prefix}.tasks.{queue_name}"

    def kv_topic(self) -> str:
        return f"{CONF.key_prefix}.state"

    def logs_topic(self) -> str:
        return f"{CONF.key_prefix}.logs"

    def activity_topic(self) -> str:
        return f"{CONF.key_prefix}.activity"


class KafkaStateBackend(BaseStateBackend):
    """Kafka state: compacted topics + in-memory caches."""

    def __init__(self, backend: KafkaBackend) -> None:
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

    async def log_publish(self, channel: str, message: str) -> None:
        topic = self.backend.logs_topic()
        await self.backend.ensure_topic(topic, compact=False)
        await self.backend.produce(topic, message.encode("utf-8"))

    async def log_subscribe(self, channel: str) -> AsyncGenerator[str, None]:
        topic = self.backend.logs_topic()
        tps = await self.backend._get_topic_partitions(topic)

        consumer = AIOKafkaConsumer(
            bootstrap_servers=self.backend._get_bootstrap_servers(),
            client_id=self.backend._client_id("log-collector"),
            enable_auto_commit=False,
        )
        await consumer.start()
        consumer.assign(tps)
        await consumer.seek_to_end(*tps)

        try:
            async for msg in consumer:
                yield msg.value.decode("utf-8")
        finally:
            await consumer.stop()

    async def acquire_lock(self, key: str, agent_id: UUID, ttl_seconds: int) -> bool:
        return True  # Partition assignment handles isolation

    async def release_lock(self, key: str) -> int:
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
                + len(self.backend._sorted_set_cache) + len(self.backend._activity_cache)
            )
            self.backend._kv_cache.clear()
            self.backend._counter_cache.clear()
            self.backend._sorted_set_cache.clear()
            self.backend._activity_cache.clear()
        return count


class KafkaQueueBackend(BaseQueueBackend):
    """Kafka queue: consumer groups for reliable fan-out."""

    def __init__(self, backend: KafkaBackend) -> None:
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
        await self.backend.produce(topic, value.encode("utf-8"), key=partition_key)

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


class KafkaActivityBackend(BaseActivityBackend):
    """Kafka activity: compacted topic + in-memory cache."""

    def __init__(self, backend: KafkaBackend) -> None:
        self.backend = backend

    def _now(self) -> str:
        return datetime.now(UTC).isoformat()

    async def _produce(self, record: dict[str, Any]) -> None:
        topic = self.backend.activity_topic()
        await self.backend.ensure_topic(topic)
        agent_id = record["agent_id"]
        data = json.dumps(record, default=str).encode("utf-8")
        await self.backend.produce(topic, data, key=str(agent_id))

    async def create(
        self,
        agent_id: UUID,
        agent_type: str,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = self._now()
        record = {
            "agent_id": str(agent_id),
            "agent_type": agent_type,
            "status": "queued",
            "metadata": metadata or {},
            "created_at": now,
            "updated_at": now,
            "logs": [
                {
                    "message": message,
                    "status": "queued",
                    "percentage": None,
                    "timestamp": now,
                }
            ],
        }
        with self.backend._cache_lock:
            self.backend._activity_cache[str(agent_id)] = record
        await self._produce(record)

    async def append_log(
        self,
        agent_id: UUID,
        message: str,
        status: str,
        percentage: int | None = None,
    ) -> None:
        now = self._now()
        log_entry = {
            "message": message,
            "status": status,
            "percentage": percentage,
            "timestamp": now,
        }
        with self.backend._cache_lock:
            record = self.backend._activity_cache.get(str(agent_id))
            if record is None:
                raise ValueError(f"Activity not found for agent_id {agent_id}")
            record["logs"].append(log_entry)
            record["updated_at"] = now
        await self._produce(record)

    async def get(
        self,
        agent_id: UUID,
        metadata_filter: dict[str, Any] | None = None,
    ) -> Any:
        with self.backend._cache_lock:
            record = self.backend._activity_cache.get(str(agent_id))
        if record is None:
            return None
        if metadata_filter:
            meta = record.get("metadata", {})
            if not all(meta.get(k) == v for k, v in metadata_filter.items()):
                return None
        return record

    async def list(
        self,
        page: int = 1,
        page_size: int = 50,
        metadata_filter: dict[str, Any] | None = None,
    ) -> tuple[list[Any], int]:
        with self.backend._cache_lock:
            all_records = list(self.backend._activity_cache.values())

        if metadata_filter:
            all_records = [
                r for r in all_records
                if all(r.get("metadata", {}).get(k) == v for k, v in metadata_filter.items())
            ]

        total = len(all_records)
        start = (page - 1) * page_size
        end = start + page_size
        return all_records[start:end], total

    async def count_active(self) -> int:
        with self.backend._cache_lock:
            return sum(
                1 for r in self.backend._activity_cache.values()
                if r.get("logs") and r["logs"][-1].get("status") in ("queued", "running")
            )

    async def get_pending_ids(self) -> list[UUID]:
        with self.backend._cache_lock:
            return [
                UUID(r["agent_id"])
                for r in self.backend._activity_cache.values()
                if r.get("logs") and r["logs"][-1].get("status") in ("queued", "running")
            ]
