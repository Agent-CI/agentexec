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
from agentexec.core.queue import enqueue
from agentexec.schedule import (
    REPEAT_FOREVER,
    ScheduledTask,
    register,
)
from agentexec.state import backend


async def tick():
    """Test helper — replicates the pool's schedule tick logic."""
    for task in await backend.schedule.get_due():
        await enqueue(
            task.task_name,
            context=backend.deserialize(task.context),
            metadata=task.metadata,
        )
        if task.repeat == 0:
            await backend.schedule.remove(task.key)
        else:
            task.advance()
            await backend.schedule.register(task)


class RefreshContext(BaseModel):
    scope: str
    ttl: int = 300


def _index_key() -> str:
    return backend.format_key(ax.CONF.key_prefix, "schedules")


def _data_key() -> str:
    return backend.format_key(ax.CONF.key_prefix, "schedules", "data")


async def _get_schedule(fake_redis, task_name: str) -> ScheduledTask | None:
    """Find a schedule by task_name in the hash."""
    all_data = await fake_redis.hgetall(_data_key())
    for key, data in all_data.items():
        st = ScheduledTask.model_validate_json(data)
        if st.task_name == task_name:
            return st
    return None


async def _force_due(fake_redis, task_name: str) -> ScheduledTask:
    """Set a schedule's next_run to the past so tick() picks it up."""
    st = await _get_schedule(fake_redis, task_name)
    if st is None:
        raise ValueError(f"No schedule found for {task_name}")
    st.next_run = time.time() - 10
    await fake_redis.hset(_data_key(), st.key, st.model_dump_json().encode())
    await fake_redis.zadd(_index_key(), {st.key: st.next_run})
    return st


@pytest.fixture
def fake_redis(monkeypatch):
    fake = fake_aioredis.FakeRedis(decode_responses=False)
    monkeypatch.setattr(backend, "_client", fake)
    yield fake


@pytest.fixture
def mock_activity_create(monkeypatch):
    async def mock_create(*args, **kwargs):
        return uuid.uuid4()
    monkeypatch.setattr("agentexec.core.task.activity.create", mock_create)


@pytest.fixture
def pool():
    p = ax.Pool(database_url="sqlite+aiosqlite:///")

    @p.task("refresh_cache")
    async def refresh(agent_id: UUID, context: RefreshContext):
        pass

    return p


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
        ctx = RefreshContext(scope="test")
        st = ScheduledTask(
            task_name="test",
            context=state.backend.serialize(ctx),
            cron="0 * * * *",
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

    def test_key_includes_task_name_and_cron(self):
        ctx = RefreshContext(scope="test")
        st = ScheduledTask(
            task_name="research",
            context=state.backend.serialize(ctx),
            cron="*/5 * * * *",
        )
        assert st.key.startswith("research:*/5 * * * *:")


class TestPoolAddSchedule:
    def test_schedule_defers_registration(self, pool):
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


class TestScheduleRegister:
    async def test_register_stores_in_redis(self, fake_redis):
        await register(
            task_name="refresh_cache",
            every="*/5 * * * *",
            context=RefreshContext(scope="all"),
        )

        st = await _get_schedule(fake_redis, "refresh_cache")
        assert st is not None
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

        members = await fake_redis.zrange(_index_key(), 0, -1, withscores=True)
        assert len(members) == 1


class TestPoolScheduleDecorator:
    def test_decorator_registers_task_and_defers_schedule(self):
        p = ax.Pool(database_url="sqlite+aiosqlite:///")

        @p.schedule("refresh_cache", "*/5 * * * *", context=RefreshContext(scope="all"))
        async def refresh(agent_id: uuid.UUID, context: RefreshContext):
            pass

        assert "refresh_cache" in p._context.tasks
        assert len(p._pending_schedules) == 1

    def test_decorator_with_lock_key(self):
        p = ax.Pool(database_url="sqlite+aiosqlite:///")

        @p.schedule("locked_task", "*/5 * * * *", lock_key="user:{user_id}")
        async def locked(agent_id: uuid.UUID, context: RefreshContext):
            pass

        defn = p._context.tasks["locked_task"]
        assert defn.lock_key == "user:{user_id}"

    def test_decorator_returns_handler(self):
        p = ax.Pool(database_url="sqlite+aiosqlite:///")

        @p.schedule("my_task", "*/5 * * * *")
        async def my_handler(agent_id: uuid.UUID, context: BaseModel):
            pass

        assert callable(my_handler)
        assert my_handler.__name__ == "my_handler"


class TestTick:
    async def test_tick_enqueues_due_task(self, fake_redis, mock_activity_create):
        await register("refresh_cache", "*/5 * * * *", RefreshContext(scope="all"))
        await _force_due(fake_redis, "refresh_cache")

        await tick()

        assert await fake_redis.llen(ax.CONF.queue_prefix) == 1

    async def test_tick_skips_future_tasks(self, fake_redis, mock_activity_create):
        await register("refresh_cache", "*/5 * * * *", RefreshContext(scope="all"))
        await tick()

        assert await fake_redis.llen(ax.CONF.queue_prefix) == 0

    async def test_tick_removes_one_shot_schedule(self, fake_redis, mock_activity_create):
        await register("refresh_cache", "* * * * *", RefreshContext(scope="all"), repeat=0)
        await _force_due(fake_redis, "refresh_cache")

        await tick()

        assert await _get_schedule(fake_redis, "refresh_cache") is None
        assert await fake_redis.zcard(_index_key()) == 0

    async def test_tick_decrements_repeat_count(self, fake_redis, mock_activity_create):
        await register("refresh_cache", "*/5 * * * *", RefreshContext(scope="all"), repeat=3)
        old_st = await _force_due(fake_redis, "refresh_cache")

        await tick()

        updated = await _get_schedule(fake_redis, "refresh_cache")
        assert updated is not None
        assert updated.repeat < 3
        assert updated.next_run > old_st.next_run

    async def test_tick_infinite_repeat_stays_negative(self, fake_redis, mock_activity_create):
        await register("refresh_cache", "*/5 * * * *", RefreshContext(scope="all"))
        await _force_due(fake_redis, "refresh_cache")

        await tick()

        updated = await _get_schedule(fake_redis, "refresh_cache")
        assert updated is not None
        assert updated.repeat == -1

    async def test_tick_anchor_based_rescheduling(self, fake_redis, mock_activity_create):
        await register("refresh_cache", "*/5 * * * *", RefreshContext(scope="all"))
        old_st = await _force_due(fake_redis, "refresh_cache")

        await tick()

        updated = await _get_schedule(fake_redis, "refresh_cache")
        assert updated is not None
        assert updated.next_run > old_st.next_run

    async def test_tick_skips_orphaned_entries(self, fake_redis, mock_activity_create):
        """Orphaned index entries are skipped with a warning."""
        await fake_redis.zadd(_index_key(), {"orphan-id": time.time() - 100})

        await tick()

        assert await fake_redis.zcard(_index_key()) == 1
        assert await fake_redis.llen(ax.CONF.queue_prefix) == 0

    async def test_tick_skips_missed_intervals(self, fake_redis, mock_activity_create):
        """After downtime, advance() skips to the next future run."""
        await register("refresh_cache", "*/1 * * * *", RefreshContext(scope="all"))

        st = await _get_schedule(fake_redis, "refresh_cache")
        assert st is not None
        st.next_run = time.time() - 600
        await fake_redis.hset(_data_key(), st.key, st.model_dump_json().encode())
        await fake_redis.zadd(_index_key(), {st.key: st.next_run})

        await tick()
        assert await fake_redis.llen(ax.CONF.queue_prefix) == 1

        await tick()
        assert await fake_redis.llen(ax.CONF.queue_prefix) == 1

    async def test_context_payload_preserved(self, fake_redis):
        await register("refresh_cache", "*/5 * * * *", RefreshContext(scope="users", ttl=999))

        st = await _get_schedule(fake_redis, "refresh_cache")
        assert st is not None
        ctx = state.backend.deserialize(st.context)
        assert isinstance(ctx, RefreshContext)
        assert ctx.scope == "users"
        assert ctx.ttl == 999


class TestTimezone:
    def test_default_timezone_is_utc(self):
        from agentexec.config import CONF
        assert CONF.scheduler_timezone == "UTC"

    def test_scheduler_tz_returns_zoneinfo(self):
        from agentexec.config import CONF
        tz = CONF.scheduler_tz
        assert isinstance(tz, ZoneInfo)

    def test_cron_respects_configured_timezone(self, monkeypatch):
        from agentexec.config import CONF
        monkeypatch.setattr(CONF, "scheduler_timezone", "America/New_York")

        ctx = RefreshContext(scope="test")
        st = ScheduledTask(
            task_name="test",
            context=state.backend.serialize(ctx),
            cron="0 9 * * *",
        )
        anchor = datetime(2024, 1, 15, 9, 0, 0, tzinfo=ZoneInfo("America/New_York")).timestamp()
        nxt = st._next_after(anchor)

        next_dt = datetime.fromtimestamp(nxt, tz=ZoneInfo("America/New_York"))
        assert next_dt.hour == 9
        assert next_dt.day == 16

    def test_timezone_env_override(self, monkeypatch):
        monkeypatch.setenv("AGENTEXEC_SCHEDULER_TIMEZONE", "Asia/Tokyo")
        from agentexec.config import Config

        conf = Config()
        assert conf.scheduler_timezone == "Asia/Tokyo"
        assert conf.scheduler_tz == ZoneInfo("Asia/Tokyo")
