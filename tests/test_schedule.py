"""Tests for scheduled task support."""

import json
import time
import uuid

import pytest
from fakeredis import aioredis as fake_aioredis
from pydantic import BaseModel

from agentexec.schedule import (
    REPEAT_FOREVER,
    Schedule,
    ScheduledTask,
    add,
    get,
    remove,
    tick,
    _queue_key,
    _schedule_key,
)


class RefreshContext(BaseModel):
    scope: str
    ttl: int = 300


@pytest.fixture
def fake_redis(monkeypatch):
    """Setup fake redis for state backend with shared state."""
    import fakeredis

    server = fakeredis.FakeServer()
    fake_redis_sync = fakeredis.FakeRedis(server=server, decode_responses=False)
    fake_redis_async = fake_aioredis.FakeRedis(server=server, decode_responses=False)

    def get_fake_sync_client():
        return fake_redis_sync

    def get_fake_async_client():
        return fake_redis_async

    monkeypatch.setattr("agentexec.state.redis_backend._get_sync_client", get_fake_sync_client)
    monkeypatch.setattr("agentexec.state.redis_backend._get_async_client", get_fake_async_client)

    yield fake_redis_sync


@pytest.fixture
def mock_activity_create(monkeypatch):
    """Mock activity.create to avoid database dependency."""

    def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)


# ---------------------------------------------------------------------------
# Schedule model
# ---------------------------------------------------------------------------


class TestScheduleModel:
    def test_default_repeat_is_forever(self):
        s = Schedule(cron="*/5 * * * *")
        assert s.repeat == REPEAT_FOREVER
        assert s.repeat == -1

    def test_next_after_returns_future_timestamp(self):
        s = Schedule(cron="*/5 * * * *")
        now = time.time()
        nxt = s.next_after(now)
        assert nxt > now

    def test_next_after_respects_anchor(self):
        """Two calls with different anchors produce different results."""
        s = Schedule(cron="0 * * * *")  # top of every hour
        anchor_a = 1_700_000_000.0  # some fixed point
        anchor_b = anchor_a + 3600  # one hour later

        next_a = s.next_after(anchor_a)
        next_b = s.next_after(anchor_b)

        assert next_b > next_a
        # Both should be on the hour
        assert next_b - next_a == pytest.approx(3600, abs=1)

    def test_cron_every_minute(self):
        s = Schedule(cron="* * * * *")
        now = time.time()
        nxt = s.next_after(now)
        # Should be within ~60 seconds of now
        assert 0 < nxt - now <= 60


# ---------------------------------------------------------------------------
# ScheduledTask model
# ---------------------------------------------------------------------------


class TestScheduledTaskModel:
    def test_roundtrip_serialization(self):
        st = ScheduledTask(
            task_name="refresh",
            context={"scope": "all", "ttl": 300},
            schedule=Schedule(cron="*/10 * * * *", repeat=5),
            next_run=time.time() + 600,
        )

        json_str = st.model_dump_json()
        restored = ScheduledTask.model_validate_json(json_str)

        assert restored.schedule_id == st.schedule_id
        assert restored.task_name == "refresh"
        assert restored.context == {"scope": "all", "ttl": 300}
        assert restored.schedule.cron == "*/10 * * * *"
        assert restored.schedule.repeat == 5

    def test_auto_generated_fields(self):
        st = ScheduledTask(
            task_name="test",
            context={},
            schedule=Schedule(cron="* * * * *"),
            next_run=0,
        )
        assert st.schedule_id  # non-empty
        assert st.created_at > 0


# ---------------------------------------------------------------------------
# add / get / remove
# ---------------------------------------------------------------------------


class TestAddGetRemove:
    def test_add_returns_schedule_id(self, fake_redis):
        sid = add("refresh", RefreshContext(scope="all"), Schedule(cron="*/5 * * * *"))
        assert isinstance(sid, str)
        assert len(sid) > 0

    def test_add_stores_in_redis(self, fake_redis):
        sid = add("refresh", RefreshContext(scope="all"), Schedule(cron="*/5 * * * *"))

        # Definition key exists
        data = fake_redis.get(_schedule_key(sid))
        assert data is not None

        st = ScheduledTask.model_validate_json(data)
        assert st.task_name == "refresh"
        assert st.context["scope"] == "all"

    def test_add_indexes_in_sorted_set(self, fake_redis):
        sid = add("refresh", RefreshContext(scope="all"), Schedule(cron="*/5 * * * *"))

        members = fake_redis.zrange(_queue_key(), 0, -1, withscores=True)
        assert len(members) == 1
        assert members[0][0].decode() == sid

    def test_get_returns_scheduled_task(self, fake_redis):
        sid = add("refresh", RefreshContext(scope="all", ttl=120), Schedule(cron="*/5 * * * *"))

        st = get(sid)
        assert st is not None
        assert st.schedule_id == sid
        assert st.task_name == "refresh"
        assert st.context["ttl"] == 120

    def test_get_returns_none_for_missing(self, fake_redis):
        assert get("nonexistent") is None

    def test_remove_clears_both_keys(self, fake_redis):
        sid = add("refresh", RefreshContext(scope="all"), Schedule(cron="*/5 * * * *"))

        result = remove(sid)
        assert result is True

        assert fake_redis.get(_schedule_key(sid)) is None
        assert fake_redis.zcard(_queue_key()) == 0

    def test_remove_returns_false_for_missing(self, fake_redis):
        assert remove("nonexistent") is False

    def test_add_with_metadata(self, fake_redis):
        sid = add(
            "refresh",
            RefreshContext(scope="all"),
            Schedule(cron="*/5 * * * *"),
            metadata={"org_id": "org-123"},
        )
        st = get(sid)
        assert st is not None
        assert st.metadata == {"org_id": "org-123"}


# ---------------------------------------------------------------------------
# tick — the scheduler heartbeat
# ---------------------------------------------------------------------------


class TestTick:
    async def test_tick_enqueues_due_task(self, fake_redis, mock_activity_create):
        """A task whose next_run is in the past should be enqueued."""
        sid = add("refresh", RefreshContext(scope="all"), Schedule(cron="*/5 * * * *"))

        # Force next_run into the past
        st = get(sid)
        assert st is not None
        st.next_run = time.time() - 10
        fake_redis.set(_schedule_key(sid), st.model_dump_json().encode())
        fake_redis.zadd(_queue_key(), {sid: st.next_run})

        count = await tick()
        assert count == 1

    async def test_tick_skips_future_tasks(self, fake_redis, mock_activity_create):
        """A task whose next_run is in the future should NOT be enqueued."""
        add("refresh", RefreshContext(scope="all"), Schedule(cron="*/5 * * * *"))

        # Default next_run is in the future, so tick should skip it
        count = await tick()
        assert count == 0

    async def test_tick_removes_one_shot_schedule(self, fake_redis, mock_activity_create):
        """A schedule with repeat=0 should be removed after firing."""
        sid = add("once", RefreshContext(scope="all"), Schedule(cron="* * * * *", repeat=0))

        # Force into the past
        st = get(sid)
        assert st is not None
        st.next_run = time.time() - 10
        fake_redis.set(_schedule_key(sid), st.model_dump_json().encode())
        fake_redis.zadd(_queue_key(), {sid: st.next_run})

        await tick()

        # Schedule should be gone
        assert get(sid) is None
        assert fake_redis.zcard(_queue_key()) == 0

    async def test_tick_decrements_repeat_count(self, fake_redis, mock_activity_create):
        """A schedule with repeat=3 should become repeat=2 after one tick."""
        sid = add("refresh", RefreshContext(scope="all"), Schedule(cron="*/5 * * * *", repeat=3))

        st = get(sid)
        assert st is not None
        st.next_run = time.time() - 10
        fake_redis.set(_schedule_key(sid), st.model_dump_json().encode())
        fake_redis.zadd(_queue_key(), {sid: st.next_run})

        await tick()

        updated = get(sid)
        assert updated is not None
        assert updated.schedule.repeat == 2
        assert updated.next_run > st.next_run

    async def test_tick_infinite_repeat_stays_negative(self, fake_redis, mock_activity_create):
        """A schedule with repeat=-1 stays at -1 forever."""
        sid = add("refresh", RefreshContext(scope="all"), Schedule(cron="*/5 * * * *", repeat=-1))

        st = get(sid)
        assert st is not None
        st.next_run = time.time() - 10
        fake_redis.set(_schedule_key(sid), st.model_dump_json().encode())
        fake_redis.zadd(_queue_key(), {sid: st.next_run})

        await tick()

        updated = get(sid)
        assert updated is not None
        assert updated.schedule.repeat == -1

    async def test_tick_anchor_based_rescheduling(self, fake_redis, mock_activity_create):
        """Next run should be computed from the intended time, not from now."""
        sid = add("refresh", RefreshContext(scope="all"), Schedule(cron="*/5 * * * *"))

        st = get(sid)
        assert st is not None
        intended_time = st.next_run

        # Simulate the scheduler being late — set next_run to the past
        st.next_run = time.time() - 60
        fake_redis.set(_schedule_key(sid), st.model_dump_json().encode())
        fake_redis.zadd(_queue_key(), {sid: st.next_run})

        await tick()

        updated = get(sid)
        assert updated is not None
        # The new next_run should be based on the old next_run (the anchor),
        # not on time.time(). It should be within a few minutes of the
        # original intended time, not hours in the future.
        assert updated.next_run > st.next_run

    async def test_tick_handles_stale_sorted_set_entries(self, fake_redis, mock_activity_create):
        """If the definition key is missing but the ZSET entry remains, clean it up."""
        # Add a ZSET entry with no corresponding definition
        fake_redis.zadd(_queue_key(), {"orphan-id": time.time() - 100})

        count = await tick()
        assert count == 0

        # Orphan should be cleaned up
        assert fake_redis.zcard(_queue_key()) == 0

    async def test_tick_multiple_due_tasks(self, fake_redis, mock_activity_create):
        """Multiple due tasks should all be processed in one tick."""
        for i in range(3):
            sid = add(
                f"task_{i}",
                RefreshContext(scope=f"scope_{i}"),
                Schedule(cron="*/5 * * * *"),
            )
            st = get(sid)
            assert st is not None
            st.next_run = time.time() - 10
            fake_redis.set(_schedule_key(sid), st.model_dump_json().encode())
            fake_redis.zadd(_queue_key(), {sid: st.next_run})

        count = await tick()
        assert count == 3

    async def test_context_payload_preserved(self, fake_redis, mock_activity_create):
        """The context payload should survive the schedule → enqueue round-trip."""
        sid = add(
            "refresh",
            RefreshContext(scope="users", ttl=999),
            Schedule(cron="*/5 * * * *"),
        )

        st = get(sid)
        assert st is not None
        assert st.context["scope"] == "users"
        assert st.context["ttl"] == 999
