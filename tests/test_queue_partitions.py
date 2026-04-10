"""Tests for the partitioned Redis queue — SCAN-based dequeue with per-partition locking."""

import json
import uuid

import pytest
from fakeredis import aioredis as fake_aioredis
from pydantic import BaseModel

import agentexec as ax
from agentexec.core.queue import Priority
from agentexec.state import backend


def _task_json(task_name: str = "test", **overrides) -> str:
    data = {
        "task_name": task_name,
        "context": {"message": "hello"},
        "agent_id": str(uuid.uuid4()),
        **overrides,
    }
    return json.dumps(data)


@pytest.fixture
def fake_redis(monkeypatch):
    fake = fake_aioredis.FakeRedis(decode_responses=False)
    monkeypatch.setattr(backend, "_client", fake)
    yield fake


class TestPartitionRouting:
    async def test_push_to_default_queue(self, fake_redis):
        await backend.queue.push(_task_json("t1"))
        assert await fake_redis.llen(ax.CONF.queue_prefix) == 1

    async def test_push_to_partition_queue(self, fake_redis):
        await backend.queue.push(_task_json("t1"), partition_key="user:42")
        partition_key = f"{ax.CONF.queue_prefix}:user:42"
        assert await fake_redis.llen(partition_key) == 1
        assert await fake_redis.llen(ax.CONF.queue_prefix) == 0

    async def test_pop_from_default_queue_no_lock(self, fake_redis):
        """Default queue tasks are popped without acquiring a lock."""
        await backend.queue.push(_task_json("t1"))
        result = await backend.queue.pop(timeout=1)

        assert result is not None
        assert result["task_name"] == "t1"

        # No lock key should exist for the default queue
        keys = [k async for k in fake_redis.scan_iter(match=b"*:lock")]
        assert len(keys) == 0


class TestPartitionLocking:
    async def test_pop_acquires_lock_for_partition(self, fake_redis):
        """Popping a partitioned task acquires its lock."""
        await backend.queue.push(_task_json("t1"), partition_key="user:42")
        result = await backend.queue.pop(timeout=1)

        assert result is not None
        lock_key = f"{ax.CONF.queue_prefix}:user:42:lock".encode()
        assert await fake_redis.exists(lock_key)

    async def test_locked_partition_is_skipped(self, fake_redis):
        """A partition with a held lock is skipped during pop."""
        await backend.queue.push(_task_json("t1"), partition_key="user:42")

        # Pre-acquire the lock
        lock_key = f"{ax.CONF.queue_prefix}:user:42:lock"
        await fake_redis.set(lock_key, b"1")

        result = await backend.queue.pop(timeout=1)
        assert result is None

    async def test_complete_releases_lock(self, fake_redis):
        """complete() deletes the partition lock."""
        await backend.queue.push(_task_json("t1"), partition_key="user:42")
        await backend.queue.pop(timeout=1)

        lock_key = f"{ax.CONF.queue_prefix}:user:42:lock".encode()
        assert await fake_redis.exists(lock_key)

        await backend.queue.complete("user:42")
        assert not await fake_redis.exists(lock_key)

    async def test_complete_noop_for_none(self, fake_redis):
        """complete(None) is a no-op for unpartitioned tasks."""
        await backend.queue.complete(None)
        # No exception, no lock keys created
        keys = [k async for k in fake_redis.scan_iter(match=b"*:lock")]
        assert len(keys) == 0


class TestMultiPartitionDequeue:
    async def test_pops_from_unlocked_partition(self, fake_redis):
        """With one locked and one unlocked partition, pop picks the unlocked one."""
        await backend.queue.push(_task_json("locked"), partition_key="user:1")
        await backend.queue.push(_task_json("unlocked"), partition_key="user:2")

        # Lock user:1
        lock_key = f"{ax.CONF.queue_prefix}:user:1:lock"
        await fake_redis.set(lock_key, b"1")

        result = await backend.queue.pop(timeout=1)
        assert result is not None
        assert result["task_name"] == "unlocked"

    async def test_pops_default_and_partition_interleaved(self, fake_redis):
        """Tasks from default and partitioned queues are both reachable."""
        await backend.queue.push(_task_json("default_task"))
        await backend.queue.push(_task_json("partitioned_task"), partition_key="org:99")

        results = []
        for _ in range(3):
            r = await backend.queue.pop(timeout=1)
            if r:
                results.append(r["task_name"])
                # Release the lock if it was a partition task
                if r["task_name"] == "partitioned_task":
                    await backend.queue.complete("org:99")

        assert sorted(results) == ["default_task", "partitioned_task"]

    async def test_serialization_within_partition(self, fake_redis):
        """Only one task per partition can be in-flight at a time."""
        await backend.queue.push(_task_json("first"), partition_key="user:1")
        await backend.queue.push(_task_json("second"), partition_key="user:1")

        # Pop first task — acquires lock
        first = await backend.queue.pop(timeout=1)
        assert first is not None
        assert first["task_name"] == "first"

        # Second pop should skip user:1 (locked) and find nothing else
        second = await backend.queue.pop(timeout=1)
        assert second is None

        # After completing, second task becomes available
        await backend.queue.complete("user:1")
        second = await backend.queue.pop(timeout=1)
        assert second is not None
        assert second["task_name"] == "second"

    async def test_independent_partitions_are_concurrent(self, fake_redis):
        """Different partitions can have tasks in-flight simultaneously."""
        await backend.queue.push(_task_json("user1_task"), partition_key="user:1")
        await backend.queue.push(_task_json("user2_task"), partition_key="user:2")

        first = await backend.queue.pop(timeout=1)
        assert first is not None

        second = await backend.queue.pop(timeout=1)
        assert second is not None

        # Both tasks popped — different partitions, different locks
        names = sorted([first["task_name"], second["task_name"]])
        assert names == ["user1_task", "user2_task"]

    async def test_empty_queue_returns_none(self, fake_redis):
        result = await backend.queue.pop(timeout=1)
        assert result is None

    async def test_high_priority_goes_to_front_of_partition(self, fake_redis):
        """High priority tasks within a partition are popped first."""
        await backend.queue.push(_task_json("low"), partition_key="user:1")
        await backend.queue.push(
            _task_json("high"), partition_key="user:1", priority=Priority.HIGH,
        )

        result = await backend.queue.pop(timeout=1)
        assert result is not None
        assert result["task_name"] == "high"
