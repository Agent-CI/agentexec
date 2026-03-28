from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, AsyncGenerator, Optional
from uuid import UUID

import redis
import redis.asyncio

from agentexec.config import CONF
from agentexec.state.base import BaseBackend, BaseQueueBackend, BaseScheduleBackend, BaseStateBackend


class Backend(BaseBackend):
    """Redis implementation of the agentexec backend."""

    def __init__(self) -> None:
        self._client: redis.asyncio.Redis | None = None
        self._pubsub: redis.asyncio.client.PubSub | None = None

        self.state = RedisStateBackend(self)
        self.queue = RedisQueueBackend(self)
        self.schedule = RedisScheduleBackend(self)

    def format_key(self, *args: str) -> str:
        return ":".join(args)

    def configure(self, **kwargs: Any) -> None:
        pass  # Redis has no per-worker configuration

    async def close(self) -> None:
        if self._pubsub is not None:
            await self._pubsub.close()
            self._pubsub = None

        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _get_client(self) -> redis.asyncio.Redis:
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
        client = self.backend._get_client()
        return await client.get(key)  # type: ignore[return-value]

    async def set(self, key: str, value: bytes, ttl_seconds: Optional[int] = None) -> bool:
        client = self.backend._get_client()
        if ttl_seconds is not None:
            return await client.set(key, value, ex=ttl_seconds)  # type: ignore[return-value]
        else:
            return await client.set(key, value)  # type: ignore[return-value]

    async def delete(self, key: str) -> int:
        client = self.backend._get_client()
        return await client.delete(key)  # type: ignore[return-value]

    async def counter_incr(self, key: str) -> int:
        client = self.backend._get_client()
        return await client.incr(key)  # type: ignore[return-value]

    async def counter_decr(self, key: str) -> int:
        client = self.backend._get_client()
        return await client.decr(key)  # type: ignore[return-value]

    async def publish(self, channel: str, message: str) -> None:
        client = self.backend._get_client()
        await client.publish(channel, message)

    async def subscribe(self, channel: str) -> AsyncGenerator[str, None]:
        client = self.backend._get_client()
        ps = client.pubsub()
        self.backend._pubsub = ps
        await ps.subscribe(channel)

        try:
            async for message in ps.listen():
                if message["type"] == "message":
                    data = message["data"]
                    if isinstance(data, bytes):
                        yield data.decode("utf-8")
                    else:
                        yield data
        finally:
            await ps.unsubscribe(channel)
            await ps.close()
            self.backend._pubsub = None

    def _lock_key(self, lock_key: str) -> str:
        return self.backend.format_key(CONF.key_prefix, "lock", lock_key)

    async def acquire_lock(self, lock_key: str, agent_id: UUID) -> bool:
        client = self.backend._get_client()
        result = await client.set(self._lock_key(lock_key), str(agent_id), nx=True, ex=CONF.lock_ttl)
        return result is not None

    async def release_lock(self, lock_key: str) -> int:
        client = self.backend._get_client()
        return await client.delete(self._lock_key(lock_key))  # type: ignore[return-value]

    async def index_add(self, key: str, mapping: dict[str, float]) -> int:
        client = self.backend._get_client()
        return await client.zadd(key, mapping)  # type: ignore[return-value]

    async def index_range(self, key: str, min_score: float, max_score: float) -> list[bytes]:
        client = self.backend._get_client()
        return await client.zrangebyscore(key, min_score, max_score)  # type: ignore[return-value]

    async def index_remove(self, key: str, *members: str) -> int:
        client = self.backend._get_client()
        return await client.zrem(key, *members)  # type: ignore[return-value]

    async def clear(self) -> int:
        if CONF.redis_url is None:
            return 0
        client = self.backend._get_client()
        deleted = 0
        deleted += await client.delete(CONF.queue_name)
        pattern = f"{CONF.key_prefix}:*"
        cursor = 0
        while True:
            cursor, keys = await client.scan(cursor=cursor, match=pattern, count=100)
            if keys:
                deleted += await client.delete(*keys)
            if cursor == 0:
                break
        return deleted


class RedisQueueBackend(BaseQueueBackend):
    """Redis queue: list-based with BRPOP."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    async def push(
        self,
        queue_name: str,
        value: str,
        *,
        high_priority: bool = False,
        partition_key: str | None = None,
    ) -> None:
        client = self.backend._get_client()
        if high_priority:
            await client.rpush(queue_name, value)
        else:
            await client.lpush(queue_name, value)

    async def pop(
        self,
        queue_name: str,
        *,
        timeout: int = 1,
    ) -> dict[str, Any] | None:
        import json
        client = self.backend._get_client()
        result = await client.brpop([queue_name], timeout=timeout)  # type: ignore[misc]
        if result is None:
            return None
        _, value = result
        return json.loads(value.decode("utf-8"))



class RedisScheduleBackend(BaseScheduleBackend):
    """Redis schedule: sorted set index + KV store."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    def _schedule_key(self, task_name: str) -> str:
        return self.backend.format_key(CONF.key_prefix, "schedule", task_name)

    def _queue_key(self) -> str:
        return self.backend.format_key(CONF.key_prefix, "schedule_queue")

    async def register(self, task: ScheduledTask) -> None:
        client = self.backend._get_client()
        await client.set(self._schedule_key(task.task_name), task.model_dump_json().encode())
        await client.zadd(self._queue_key(), {task.task_name: task.next_run})

    async def get_due(self) -> list[ScheduledTask]:
        import time
        from pydantic import ValidationError
        from agentexec.schedule import ScheduledTask
        client = self.backend._get_client()
        raw = await client.zrangebyscore(self._queue_key(), 0, time.time())
        tasks = []
        for name in raw:
            task_name = name.decode("utf-8") if isinstance(name, bytes) else name
            data = await client.get(self._schedule_key(task_name))
            if data is None:
                continue
            try:
                tasks.append(ScheduledTask.model_validate_json(data))
            except ValidationError:
                continue
        return tasks

    async def remove(self, task_name: str) -> None:
        client = self.backend._get_client()
        await client.zrem(self._queue_key(), task_name)
        await client.delete(self._schedule_key(task_name))
