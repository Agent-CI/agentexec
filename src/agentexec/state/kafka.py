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

from agentexec.activity.status import Status
from agentexec.config import CONF
from agentexec.state.base import BaseActivityBackend, BaseBackend, BaseQueueBackend, BaseScheduleBackend, BaseStateBackend



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
        self.activity = KafkaActivityBackend(self)
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

    def logs_topic(self) -> str:
        return f"{CONF.key_prefix}.logs"

    def activity_topic(self) -> str:
        return f"{CONF.key_prefix}.activity"

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

    async def log_publish(self, message: str) -> None:
        topic = self.backend.logs_topic()
        await self.backend.ensure_topic(topic, compact=False)
        await self.backend.produce(topic, message.encode("utf-8"))

    async def log_subscribe(self) -> AsyncGenerator[str, None]:
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


class KafkaActivityBackend(BaseActivityBackend):
    """Kafka activity: compacted topic, read from Kafka directly."""

    BATCH_SIZE = 100

    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self._consumer: AIOKafkaConsumer | None = None
        self._tps: list[TopicPartition] = []

    def _now(self) -> str:
        return datetime.now(UTC).isoformat()

    async def _ensure_consumer(self) -> AIOKafkaConsumer:
        topic = self.backend.activity_topic()
        await self.backend.ensure_topic(topic)

        if self._consumer is None:
            self._tps = await self.backend._get_topic_partitions(topic)
            self._consumer = AIOKafkaConsumer(
                bootstrap_servers=self.backend._get_bootstrap_servers(),
                client_id=self.backend._client_id("activity"),
                enable_auto_commit=False,
            )
            await self._consumer.start()
            self._consumer.assign(self._tps)

        return self._consumer

    def _current_status(self, record: dict[str, Any]) -> str:
        logs = record.get("logs", [])
        return logs[-1]["status"] if logs else "queued"

    async def _produce(self, record: dict[str, Any]) -> None:
        topic = self.backend.activity_topic()
        await self.backend.ensure_topic(topic)
        data = json.dumps(record, default=str).encode("utf-8")
        headers = {
            "ax_agent_id": str(record["agent_id"]),
            "ax_task_name": record.get("agent_type", ""),
            "ax_status": self._current_status(record),
        }
        await self.backend.produce(topic, data, key=str(record["agent_id"]), headers=headers)

    async def _find_record(self, agent_id: UUID) -> dict[str, Any] | None:
        """Scan backwards from the end of the activity topic for a specific agent_id."""
        consumer = await self._ensure_consumer()
        key = str(agent_id).encode("utf-8")

        end_offsets = await consumer.end_offsets(self._tps)

        for tp in self._tps:
            end = end_offsets[tp]
            if end == 0:
                continue

            pos = max(0, end - self.BATCH_SIZE)
            while pos >= 0:
                consumer.seek(tp, pos)
                records = await consumer.getmany(tp, timeout_ms=1000)
                for msg in reversed(records.get(tp, [])):
                    if msg.key == key and msg.value is not None:
                        return json.loads(msg.value)
                if pos == 0:
                    break
                pos = max(0, pos - self.BATCH_SIZE)

        return None

    async def _read_page(self, offset_from_end: int, count: int) -> list[dict[str, Any]]:
        """Read a page of records from the end of the activity topic."""
        consumer = await self._ensure_consumer()
        end_offsets = await consumer.end_offsets(self._tps)

        results = []
        for tp in self._tps:
            end = end_offsets[tp]
            if end == 0:
                continue
            start = max(0, end - offset_from_end - count)
            consumer.seek(tp, start)
            records = await consumer.getmany(tp, timeout_ms=1000, max_records=count + offset_from_end)
            msgs = records.get(tp, [])
            for msg in msgs:
                if msg.value is not None:
                    results.append(json.loads(msg.value))

        # Reverse so most recent is first, then slice the page
        results.reverse()
        return results[offset_from_end:offset_from_end + count]

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
            "status": Status.QUEUED,
            "metadata": metadata or {},
            "created_at": now,
            "updated_at": now,
            "logs": [
                {
                    "message": message,
                    "status": Status.QUEUED,
                    "percentage": None,
                    "timestamp": now,
                }
            ],
        }
        await self._produce(record)

    async def append_log(
        self,
        agent_id: UUID,
        message: str,
        status: Status,
        percentage: int | None = None,
    ) -> None:
        record = await self._find_record(agent_id)
        if record is None:
            raise ValueError(f"Activity not found for agent_id {agent_id}")

        now = self._now()
        record["logs"].append({
            "message": message,
            "status": status,
            "percentage": percentage,
            "timestamp": now,
        })
        record["updated_at"] = now
        await self._produce(record)

    async def get(
        self,
        agent_id: UUID,
        metadata_filter: dict[str, Any] | None = None,
    ) -> Any:
        record = await self._find_record(agent_id)
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
        # TODO: metadata_filter requires scanning all records — consider
        # a secondary index if filtered queries become common
        consumer = await self._ensure_consumer()
        end_offsets = await consumer.end_offsets(self._tps)
        total = sum(end_offsets.values())

        offset_from_end = (page - 1) * page_size
        records = await self._read_page(offset_from_end, page_size)

        if metadata_filter:
            records = [
                r for r in records
                if all(r.get("metadata", {}).get(k) == v for k, v in metadata_filter.items())
            ]

        return records, total

    def _get_header(self, msg: Any, name: str) -> str | None:
        """Extract a header value from a Kafka message."""
        if msg.headers is None:
            return None
        for key, value in msg.headers:
            if key == name:
                return value.decode("utf-8")
        return None

    async def _scan_by_status(self, *statuses: str) -> list[Any]:
        """Scan the topic using headers only — no body deserialization."""
        consumer = await self._ensure_consumer()
        await consumer.seek_to_beginning(*self._tps)

        matches = []
        records = await consumer.getmany(*self._tps, timeout_ms=1000)
        for partition_records in records.values():
            for msg in partition_records:
                if msg.value is None:
                    continue
                status = self._get_header(msg, "ax_status")
                if status in statuses:
                    matches.append(msg)

        return matches

    async def count_active(self) -> int:
        return len(await self._scan_by_status("queued", "running"))

    async def get_pending_ids(self) -> list[UUID]:
        messages = await self._scan_by_status("queued", "running")
        return [
            UUID(self._get_header(msg, "ax_agent_id"))
            for msg in messages
            if self._get_header(msg, "ax_agent_id")
        ]


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
