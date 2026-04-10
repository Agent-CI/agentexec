"""Redis state backend.

Provides queue, state (KV/counters/sorted sets), and schedule operations
backed by Redis. The queue implementation uses a partitioned design
inspired by Kafka's consumer groups:

Queue Key Layout
~~~~~~~~~~~~~~~~

All queue keys share a common prefix (``CONF.queue_prefix``, default
``agentexec_tasks``)::

    agentexec_tasks              ← default queue (no lock, concurrent)
    agentexec_tasks:user:42      ← partition queue for lock scope "user:42"
    agentexec_tasks:user:42:lock ← lock for that partition (SET NX EX)

Tasks without a ``lock_key`` go to the default queue, where any worker can
pop them concurrently. Tasks with a ``lock_key`` (evaluated from the
``TaskDefinition.lock_key`` template against the task context) go to a
partition queue keyed by that value.

Dequeue Strategy
~~~~~~~~~~~~~~~~

Workers call ``queue.pop()`` which uses Redis SCAN to iterate all keys
matching the queue prefix. SCAN returns keys in hash-table order, which
is effectively random — providing fair distribution across partitions
without explicit shuffling.

For each key discovered:

1. If it ends with ``:lock``, record it in ``locks_seen`` and skip.
2. If it's a partition queue (not the default), check ``locks_seen`` for
   an existing lock. If found, skip. Otherwise attempt ``SET NX EX`` to
   acquire the lock. If acquisition fails, skip.
3. ``RPOP`` the queue key. If successful, return the task payload.
4. On task completion, the pool calls ``queue.complete(partition_key)``
   which deletes the lock key, allowing the next task in that partition
   to be picked up.

Redis automatically deletes list keys when they become empty, so drained
partitions disappear from future scans. Lock keys expire via TTL as a
safety net for dead worker recovery.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from agentexec.core.queue import Priority
    from agentexec.schedule import ScheduledTask
import json
import redis
import redis.asyncio

from agentexec.config import CONF
from agentexec.state.base import BaseBackend, BaseQueueBackend, BaseScheduleBackend, BaseStateBackend


class Backend(BaseBackend):
    """Redis implementation of the agentexec backend."""

    _client: redis.asyncio.Redis | None
    state: RedisStateBackend
    queue: RedisQueueBackend
    schedule: RedisScheduleBackend

    def __init__(self) -> None:
        self._client = None
        self.state = RedisStateBackend(self)
        self.queue = RedisQueueBackend(self)
        self.schedule = RedisScheduleBackend(self)

    def format_key(self, *args: str) -> str:
        return ":".join(args)

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

    backend: Backend

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


class RedisQueueBackend(BaseQueueBackend):
    """Redis queue: partitioned lists with per-group locking.

    Tasks with a partition_key go to {prefix}:{partition_key} and are
    serialized by a lock. Tasks without a partition_key go to the
    default queue ({prefix}) and execute concurrently.
    """

    backend: Backend
    _lock_suffix: bytes = b":lock"
    _prefix: str
    _default_key: bytes

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
        return bool(
            await self.backend.client.set(
                self._lock_key(queue_key),
                b"1",
                nx=True,
                ex=CONF.lock_ttl,
            )
        )

    async def push(
        self,
        value: str,
        *,
        priority: Priority | None = None,
        partition_key: str | None = None,
    ) -> None:
        """Push a task to the queue.

        Tasks with a ``partition_key`` go to a dedicated partition queue
        and are serialized by a lock. Tasks without one go to the default
        queue for concurrent processing.
        """
        from agentexec.core.queue import Priority

        key = self._queue_key(partition_key)
        if priority is Priority.HIGH:
            await self.backend.client.rpush(key, value)  # type: ignore[misc]
        else:
            await self.backend.client.lpush(key, value)  # type: ignore[misc]

    async def pop(self, *, timeout: int = 1) -> dict[str, Any] | None:
        """Pop the next eligible task from any queue.

        Scans all queue keys, skips locked partitions, acquires a lock
        for the selected partition, and pops the task. Returns ``None``
        if no eligible tasks are available.
        """
        locks_seen: set[bytes] = set()

        # SCAN returns keys in hash-table order (effectively random),
        # so we don't need to collect all keys before choosing.
        # We try each key eagerly and exit on the first successful pop.
        async for key in self.backend.client.scan_iter(match=self._prefix.encode() + b"*", count=100):
            if self._needs_lock(key):
                if key.endswith(self._lock_suffix):
                    locks_seen.add(key)
                    continue  # this is a lock record, not executable

                if self._lock_key(key) in locks_seen:
                    continue  # we already observed another worker holds this partition, find another

                if not await self._acquire_lock(key):
                    continue  # another worker holds this partition, find another

            result = await self.backend.client.rpop(key)  # type: ignore[misc]
            if result is None:
                if self._needs_lock(key):
                    # TODO this should never happen; we can improve on the ergonomics of recovery later.
                    raise RuntimeError(f"Partition queue {key!r} was empty after lock acquired")

                continue  # payload was grabbed in a race condition, find another

            return json.loads(result)

    async def complete(self, partition_key: str | None) -> None:
        """Signal that the current task for this partition is done.

        Deletes the partition lock so the next task in the same scope
        can be picked up. No-op for tasks without a partition key.
        """
        if partition_key:
            await self.backend.client.delete(self._lock_key(self._queue_key(partition_key).encode()))


class RedisScheduleBackend(BaseScheduleBackend):
    """Redis schedule: sorted set for time index + hash for payloads.

    Two Redis keys::

        agentexec:schedules       ← sorted set (schedule.key → next_run score)
        agentexec:schedules:data  ← hash (schedule.key → task JSON)

    ``get_due`` queries the sorted set for keys with score <= now,
    then batch-fetches the payloads from the hash.
    """

    backend: Backend
    _index_key: str
    _data_key: str

    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self._index_key = self.backend.format_key(CONF.key_prefix, "schedules")
        self._data_key = self.backend.format_key(CONF.key_prefix, "schedules", "data")

    async def register(self, task: ScheduledTask) -> None:
        await self.backend.client.hset(self._data_key, task.key, task.model_dump_json().encode())  # type: ignore[misc]
        await self.backend.client.zadd(self._index_key, {task.key: task.next_run})  # type: ignore[misc]

    async def get_due(self) -> list[ScheduledTask]:
        import time
        from pydantic import ValidationError
        from agentexec.schedule import ScheduledTask

        raw = await self.backend.client.zrangebyscore(self._index_key, 0, time.time())
        tasks = []
        for key in raw or []:
            data = await self.backend.client.hget(self._data_key, key)  # type: ignore[misc]
            if data is None:
                continue
            try:
                tasks.append(ScheduledTask.model_validate_json(data))
            except ValidationError:
                continue
        return tasks

    async def remove(self, key: str) -> None:
        await self.backend.client.zrem(self._index_key, key)  # type: ignore[misc]
        await self.backend.client.hdel(self._data_key, key)  # type: ignore[misc]
