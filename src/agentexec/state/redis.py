from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Optional

import redis
import redis.asyncio

from agentexec.config import CONF
from agentexec.state.base import BaseBackend, BaseQueueBackend, BaseScheduleBackend, BaseStateBackend


class Backend(BaseBackend):
    """Redis implementation of the agentexec backend."""

    def __init__(self) -> None:
        self._client: redis.asyncio.Redis | None = None

        self.state = RedisStateBackend(self)
        self.queue = RedisQueueBackend(self)
        self.schedule = RedisScheduleBackend(self)

    def format_key(self, *args: str) -> str:
        return ":".join(args)

    def configure(self, **kwargs: Any) -> None:
        pass  # Redis has no per-worker configuration

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> redis.asyncio.Redis:
        if self._client is None:
            if CONF.redis_url is None:
                raise ValueError("REDIS_URL must be configured")
            self._client = redis.asyncio.Redis.from_url(
                CONF.redis_url,
                max_connections=CONF.redis_pool_size,
                socket_connect_timeout=CONF.redis_pool_timeout,
                decode_responses=False,
            )
        return self._client


class RedisStateBackend(BaseStateBackend):
    """Redis state: direct Redis commands."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    async def get(self, key: str) -> Optional[bytes]:
        return await self.backend.client.get(key)  # type: ignore[return-value]

    async def set(self, key: str, value: bytes, ttl_seconds: Optional[int] = None) -> bool:
        if ttl_seconds is not None:
            return await self.backend.client.set(key, value, ex=ttl_seconds)  # type: ignore[return-value]
        else:
            return await self.backend.client.set(key, value)  # type: ignore[return-value]

    async def delete(self, key: str) -> int:
        return await self.backend.client.delete(key)  # type: ignore[return-value]

    async def counter_incr(self, key: str) -> int:
        return await self.backend.client.incr(key)  # type: ignore[return-value]

    async def counter_decr(self, key: str) -> int:
        return await self.backend.client.decr(key)  # type: ignore[return-value]

    async def index_add(self, key: str, mapping: dict[str, float]) -> int:
        return await self.backend.client.zadd(key, mapping)  # type: ignore[return-value]

    async def index_range(self, key: str, min_score: float, max_score: float) -> list[bytes]:
        return await self.backend.client.zrangebyscore(key, min_score, max_score)  # type: ignore[return-value]

    async def index_remove(self, key: str, *members: str) -> int:
        return await self.backend.client.zrem(key, *members)  # type: ignore[return-value]

    async def clear(self) -> int:
        if CONF.redis_url is None:
            return 0
        deleted = 0
        deleted += await self.backend.client.delete(CONF.queue_prefix)
        pattern = f"{CONF.key_prefix}:*"
        cursor = 0
        while True:
            cursor, keys = await self.backend.client.scan(cursor=cursor, match=pattern, count=100)
            if keys:
                deleted += await self.backend.client.delete(*keys)
            if cursor == 0:
                break
        return deleted


class RedisQueueBackend(BaseQueueBackend):
    """Redis queue: partitioned lists with per-group locking.

    Tasks with a partition_key go to {prefix}:{partition_key} and are
    serialized by a lock. Tasks without a partition_key go to the
    default queue ({prefix}) and execute concurrently.
    """

    _lock_suffix = b":lock"

    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self._prefix = CONF.queue_prefix
        self._default_key = self._prefix.encode()

    def _queue_key(self, partition_key: str | None = None) -> str:
        if partition_key:
            return f"{self._prefix}:{partition_key}"
        return self._prefix

    def _lock_key(self, queue_key: bytes) -> bytes:
        return queue_key + self._lock_suffix

    def _needs_lock(self, queue_key: bytes) -> bool:
        return queue_key != self._default_key

    async def _acquire_lock(self, queue_key: bytes) -> bool:
        return bool(await self.backend.client.set(
            self._lock_key(queue_key), b"1", nx=True, ex=CONF.lock_ttl,
        ))

    async def push(
        self,
        value: str,
        *,
        high_priority: bool = False,
        partition_key: str | None = None,
    ) -> None:
        key = self._queue_key(partition_key)
        if high_priority:
            await self.backend.client.rpush(key, value)
        else:
            await self.backend.client.lpush(key, value)

    async def pop(self, *, timeout: int = 1) -> dict[str, Any] | None:
        import json

        locks_seen: set[bytes] = set()

        # SCAN returns keys in hash-table order (effectively random),
        # so we don't need to collect all keys before choosing.
        # We try each key eagerly and exit on the first successful pop.
        async for key in self.backend.client.scan_iter(match=self._prefix.encode() + b"*", count=100):
            if key.endswith(self._lock_suffix):
                locks_seen.add(key)
                continue

            if self._needs_lock(key):
                if self._lock_key(key) in locks_seen:
                    continue  # already locked, find another

                if not await self._acquire_lock(key):
                    continue  # another worker holds this partition, find another

            result = await self.backend.client.rpop(key)
            if result is None:
                if self._needs_lock(key):
                    # TODO this should never happen; we can improve on the ergonomics of recovery later.
                    raise RuntimeError(f"Partition queue {key!r} was empty after lock acquired")

                continue  # payload was grabbed in a race condition, find another

            return json.loads(result)

    async def complete(self, partition_key: str | None) -> None:
        if partition_key:
            await self.backend.client.delete(self._lock_key(self._queue_key(partition_key).encode()))



class RedisScheduleBackend(BaseScheduleBackend):
    """Redis schedule: sorted set index + KV store."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    def _schedule_key(self, task_name: str) -> str:
        return self.backend.format_key(CONF.key_prefix, "schedule", task_name)

    def _queue_key(self) -> str:
        return self.backend.format_key(CONF.key_prefix, "schedule_queue")

    async def register(self, task: ScheduledTask) -> None:
        await self.backend.client.set(self._schedule_key(task.task_name), task.model_dump_json().encode())
        await self.backend.client.zadd(self._queue_key(), {task.task_name: task.next_run})

    async def get_due(self) -> list[ScheduledTask]:
        import time
        from pydantic import ValidationError
        from agentexec.schedule import ScheduledTask
        raw = await self.backend.client.zrangebyscore(self._queue_key(), 0, time.time())
        tasks = []
        for name in raw:
            task_name = name.decode("utf-8")
            data = await self.backend.client.get(self._schedule_key(task_name))
            if data is None:
                continue
            try:
                tasks.append(ScheduledTask.model_validate_json(data))
            except ValidationError:
                continue
        return tasks

    async def remove(self, task_name: str) -> None:
        await self.backend.client.zrem(self._queue_key(), task_name)
        await self.backend.client.delete(self._schedule_key(task_name))
