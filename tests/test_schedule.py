"""Tests for scheduled task support."""

import time
import uuid
from datetime import datetime
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest
from fakeredis import aioredis as fake_aioredis
from pydantic import BaseModel

import agentexec as ax
from agentexec import state, schedule
from agentexec.schedule import (
    REPEAT_FOREVER,
    ScheduledTask,
    register,
    tick,
)
from agentexec.state import ops


class RefreshContext(BaseModel):
    scope: str
    ttl: int = 300


def _schedule_key(task_name: str) -> str:
    """Build the Redis key for a schedule definition."""
    return ops.format_key(ax.CONF.key_prefix, "schedule", task_name)


def _queue_key() -> str:
    """Build the Redis key for the schedule sorted-set index."""
    return ops.format_key(ax.CONF.key_prefix, "schedule_queue")


@pytest.fixture
def fake_redis(monkeypatch):
    """Setup fake redis for state backend."""
    import fakeredis

    server = fakeredis.FakeServer()
    fake_redis_async = fake_aioredis.FakeRedis(server=server, decode_responses=False)

    def get_fake_async_client():
        return fake_redis_async

    monkeypatch.setattr(
        "agentexec.state.redis_backend.state.get_async_client", get_fake_async_client
    )
    monkeypatch.setattr(
        "agentexec.state.redis_backend.queue.get_async_client", get_fake_async_client
    )

    yield fake_redis_async


@pytest.fixture
def mock_activity_create(monkeypatch):
    """Mock activity.create to avoid database dependency."""

    async def mock_create(*args, **kwargs):
        return uuid.uuid4()

    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)


@pytest.fixture
def pool():
    """Create a Pool with a registered task for scheduling tests."""
    p = ax.Pool(database_url="sqlite:///")

    @p.task("refresh_cache")
    async def refresh(agent_id: UUID, context: RefreshContext):
        pass

    return p


async def _force_due(fake_redis, task_name):
    """Helper: set a schedule's next_run to the past so tick() picks it up."""
    data = await fake_redis.get(_schedule_key(task_name))
    st = ScheduledTask.model_validate_json(data)
    st.next_run = time.time() - 10
    await fake_redis.set(_schedule_key(task_name), st.model_dump_json().encode())
    await fake_redis.zadd(_queue_key(), {task_name: st.next_run})
    return st


# ---------------------------------------------------------------------------
# ScheduledTask model
# ---------------------------------------------------------------------------


class TestScheduledTaskModel:
    def test_default_repeat_is_forever(self):
        ctx = RefreshContext(scope="test")
        st = ScheduledTask(
            task_name="test",
            context=state.backend.serialize(ctx),
            cron="*/5 * * * *",

        )
        assert st.repeat == REPEAT_FOREVER
        assert st.repeat == -1

    def test_next_run_returns_future_timestamp(self):
        ctx = RefreshContext(scope="test")
        st = ScheduledTask(
            task_name="test",
            context=state.backend.serialize(ctx),
            cron="*/5 * * * *",

        )
        now = time.time()
        nxt = st._next_after(now)
        assert nxt > now

    def test_next_run_respects_anchor(self):
        """Two calls with different anchors produce different results."""
        ctx = RefreshContext(scope="test")
        st = ScheduledTask(
            task_name="test",
            context=state.backend.serialize(ctx),
            cron="0 * * * *",  # top of every hour

        )
        anchor_a = 1_700_000_000.0
        anchor_b = anchor_a + 3600

        next_a = st._next_after(anchor_a)
        next_b = st._next_after(anchor_b)

        assert next_b > next_a
        assert next_b - next_a == pytest.approx(3600, abs=1)

    def test_cron_every_minute(self):
        ctx = RefreshContext(scope="test")
        st = ScheduledTask(
            task_name="test",
            context=state.backend.serialize(ctx),
            cron="* * * * *",

        )
        now = time.time()
        nxt = st._next_after(now)
        assert 0 < nxt - now <= 60

    def test_roundtrip_serialization(self):
        ctx = RefreshContext(scope="all", ttl=300)
        st = ScheduledTask(
            task_name="refresh",
            context=state.backend.serialize(ctx),
            cron="*/10 * * * *",
            repeat=5,
            next_run=time.time() + 600,
        )

        json_str = st.model_dump_json()
        restored = ScheduledTask.model_validate_json(json_str)

        assert restored.task_name == "refresh"
        restored_ctx = state.backend.deserialize(restored.context)
        assert isinstance(restored_ctx, RefreshContext)
        assert restored_ctx.scope == "all"
        assert restored_ctx.ttl == 300
        assert restored.cron == "*/10 * * * *"
        assert restored.repeat == 5

    def test_auto_generated_fields(self):
        ctx = RefreshContext(scope="test")
        st = ScheduledTask(
            task_name="test",
            context=state.backend.serialize(ctx),
            cron="* * * * *",
        )
        assert st.created_at > 0
        assert st.next_run > 0


# ---------------------------------------------------------------------------
# pool.add_schedule() — deferred registration
# ---------------------------------------------------------------------------


class TestPoolAddSchedule:
    def test_schedule_defers_registration(self, pool):
        """add_schedule stores config in _pending_schedules, not Redis."""
        pool.add_schedule("refresh_cache", "*/5 * * * *", RefreshContext(scope="all"))

        assert len(pool._pending_schedules) == 1
        sched = pool._pending_schedules[0]
        assert sched["task_name"] == "refresh_cache"
        assert sched["every"] == "*/5 * * * *"

    def test_schedule_rejects_unregistered_task(self, pool):
        with pytest.raises(ValueError, match="not registered"):
            pool.add_schedule("nonexistent_task", "*/5 * * * *", RefreshContext(scope="all"))

    def test_schedule_with_metadata(self, pool):
        pool.add_schedule(
            "refresh_cache", "*/5 * * * *", RefreshContext(scope="all"),
            metadata={"org_id": "org-123"},
        )
        assert pool._pending_schedules[0]["metadata"] == {"org_id": "org-123"}

    def test_schedule_with_repeat(self, pool):
        pool.add_schedule(
            "refresh_cache", "*/5 * * * *", RefreshContext(scope="all"), repeat=3,
        )
        assert pool._pending_schedules[0]["repeat"] == 3


# ---------------------------------------------------------------------------
# schedule.register() — direct registration to backend
# ---------------------------------------------------------------------------


class TestScheduleRegister:
    async def test_register_stores_in_redis(self, fake_redis):
        await register(
            task_name="refresh_cache",
            every="*/5 * * * *",
            context=RefreshContext(scope="all"),
        )

        data = await fake_redis.get(_schedule_key("refresh_cache"))
        assert data is not None

        st = ScheduledTask.model_validate_json(data)
        assert st.task_name == "refresh_cache"
        ctx = state.backend.deserialize(st.context)
        assert isinstance(ctx, RefreshContext)
        assert ctx.scope == "all"

    async def test_register_indexes_in_sorted_set(self, fake_redis):
        await register(
            task_name="refresh_cache",
            every="*/5 * * * *",
            context=RefreshContext(scope="all"),
        )

        members = await fake_redis.zrange(_queue_key(), 0, -1, withscores=True)
        assert len(members) == 1


# ---------------------------------------------------------------------------
# @pool.schedule() decorator
# ---------------------------------------------------------------------------


class TestPoolScheduleDecorator:
    def test_decorator_registers_task_and_defers_schedule(self):
        """@pool.schedule registers the task and defers the schedule."""
        p = ax.Pool(database_url="sqlite:///")

        @p.schedule("refresh_cache", "*/5 * * * *", context=RefreshContext(scope="all"))
        async def refresh(agent_id: uuid.UUID, context: RefreshContext):
            pass

        # Task is registered
        assert "refresh_cache" in p._context.tasks
        # Schedule is deferred
        assert len(p._pending_schedules) == 1

    def test_decorator_with_lock_key(self):
        """@pool.schedule passes lock_key to the task registration."""
        p = ax.Pool(database_url="sqlite:///")

        @p.schedule("locked_task", "*/5 * * * *", lock_key="user:{user_id}")
        async def locked(agent_id: uuid.UUID, context: RefreshContext):
            pass

        defn = p._context.tasks["locked_task"]
        assert defn.lock_key == "user:{user_id}"

    def test_decorator_returns_handler(self):
        """@pool.schedule returns the original handler function."""
        p = ax.Pool(database_url="sqlite:///")

        @p.schedule("my_task", "*/5 * * * *")
        async def my_handler(agent_id: uuid.UUID, context: BaseModel):
            pass

        assert callable(my_handler)
        assert my_handler.__name__ == "my_handler"


# ---------------------------------------------------------------------------
# tick — the scheduler heartbeat
# ---------------------------------------------------------------------------


class TestTick:
    async def test_tick_enqueues_due_task(self, fake_redis, mock_activity_create):
        await register("refresh_cache", "*/5 * * * *", RefreshContext(scope="all"))
        await _force_due(fake_redis, "refresh_cache")

        await tick()

        assert await fake_redis.llen(ax.CONF.queue_name) == 1

    async def test_tick_skips_future_tasks(self, fake_redis, mock_activity_create):
        await register("refresh_cache", "*/5 * * * *", RefreshContext(scope="all"))
        await tick()

        assert await fake_redis.llen(ax.CONF.queue_name) == 0

    async def test_tick_removes_one_shot_schedule(self, fake_redis, mock_activity_create):
        await register("refresh_cache", "* * * * *", RefreshContext(scope="all"), repeat=0)
        await _force_due(fake_redis, "refresh_cache")

        await tick()

        assert await fake_redis.get(_schedule_key("refresh_cache")) is None
        assert await fake_redis.zcard(_queue_key()) == 0

    async def test_tick_decrements_repeat_count(self, fake_redis, mock_activity_create):
        await register("refresh_cache", "*/5 * * * *", RefreshContext(scope="all"), repeat=3)
        old_st = await _force_due(fake_redis, "refresh_cache")

        await tick()

        data = await fake_redis.get(_schedule_key("refresh_cache"))
        updated = ScheduledTask.model_validate_json(data)
        assert updated.repeat == 2
        assert updated.next_run > old_st.next_run

    async def test_tick_infinite_repeat_stays_negative(self, fake_redis, mock_activity_create):
        await register("refresh_cache", "*/5 * * * *", RefreshContext(scope="all"))
        await _force_due(fake_redis, "refresh_cache")

        await tick()

        data = await fake_redis.get(_schedule_key("refresh_cache"))
        updated = ScheduledTask.model_validate_json(data)
        assert updated.repeat == -1

    async def test_tick_anchor_based_rescheduling(self, fake_redis, mock_activity_create):
        await register("refresh_cache", "*/5 * * * *", RefreshContext(scope="all"))
        old_st = await _force_due(fake_redis, "refresh_cache")

        await tick()

        data = await fake_redis.get(_schedule_key("refresh_cache"))
        updated = ScheduledTask.model_validate_json(data)
        assert updated.next_run > old_st.next_run

    async def test_tick_skips_orphaned_entries(self, fake_redis, mock_activity_create):
        """Orphaned queue entries are skipped (not deleted) with a warning."""
        await fake_redis.zadd(_queue_key(), {"orphan-id": time.time() - 100})

        await tick()

        assert await fake_redis.zcard(_queue_key()) == 1
        assert await fake_redis.llen(ax.CONF.queue_name) == 0

    async def test_tick_skips_missed_intervals(self, fake_redis, mock_activity_create):
        """After downtime, advance() skips to the next future run — no burst of catch-up tasks."""
        await register("refresh_cache", "*/1 * * * *", RefreshContext(scope="all"))

        # Simulate 10 minutes of downtime
        data = await fake_redis.get(_schedule_key("refresh_cache"))
        st = ScheduledTask.model_validate_json(data)
        st.next_run = time.time() - 600
        await fake_redis.set(_schedule_key("refresh_cache"), st.model_dump_json().encode())
        await fake_redis.zadd(_queue_key(), {"refresh_cache": st.next_run})

        await tick()
        assert await fake_redis.llen(ax.CONF.queue_name) == 1

        # Second tick should not enqueue again (next_run is in the future now)
        await tick()
        assert await fake_redis.llen(ax.CONF.queue_name) == 1

    async def test_context_payload_preserved(self, fake_redis):
        await register("refresh_cache", "*/5 * * * *", RefreshContext(scope="users", ttl=999))

        data = await fake_redis.get(_schedule_key("refresh_cache"))
        st = ScheduledTask.model_validate_json(data)
        ctx = state.backend.deserialize(st.context)
        assert isinstance(ctx, RefreshContext)
        assert ctx.scope == "users"
        assert ctx.ttl == 999


# ---------------------------------------------------------------------------
# Timezone configuration
# ---------------------------------------------------------------------------


class TestTimezone:
    def test_default_timezone_is_utc(self):
        """Default should be UTC."""
        from agentexec.config import CONF

        assert CONF.scheduler_timezone == "UTC"

    def test_scheduler_tz_returns_zoneinfo(self):
        from agentexec.config import CONF

        tz = CONF.scheduler_tz
        assert isinstance(tz, ZoneInfo)

    def test_cron_respects_configured_timezone(self, monkeypatch):
        """Cron evaluation should use the configured timezone."""
        from agentexec.config import CONF

        monkeypatch.setattr(CONF, "scheduler_timezone", "America/New_York")

        ctx = RefreshContext(scope="test")
        st = ScheduledTask(
            task_name="test",
            context=state.backend.serialize(ctx),
            cron="0 9 * * *",  # 9 AM

        )
        # Use a known timestamp: 2024-01-15 9:00 AM ET
        anchor = datetime(2024, 1, 15, 9, 0, 0, tzinfo=ZoneInfo("America/New_York")).timestamp()
        nxt = st._next_after(anchor)

        # Next 9 AM ET should be ~24h later
        next_dt = datetime.fromtimestamp(nxt, tz=ZoneInfo("America/New_York"))
        assert next_dt.hour == 9
        assert next_dt.day == 16

    def test_timezone_env_override(self, monkeypatch):
        """AGENTEXEC_SCHEDULER_TIMEZONE env var should override default."""
        monkeypatch.setenv("AGENTEXEC_SCHEDULER_TIMEZONE", "Asia/Tokyo")
        from agentexec.config import Config

        conf = Config()
        assert conf.scheduler_timezone == "Asia/Tokyo"
        assert conf.scheduler_tz == ZoneInfo("Asia/Tokyo")
