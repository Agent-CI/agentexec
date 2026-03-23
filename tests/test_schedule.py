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
from agentexec.schedule import (
    REPEAT_FOREVER,
    Schedule,
    ScheduledTask,
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


@pytest.fixture
def pool():
    """Create a Pool with a registered task for scheduling tests."""
    p = ax.Pool(database_url="sqlite:///")

    @p.task("refresh_cache")
    async def refresh(agent_id: UUID, context: RefreshContext):
        pass

    return p


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
        anchor_a = 1_700_000_000.0
        anchor_b = anchor_a + 3600

        next_a = s.next_after(anchor_a)
        next_b = s.next_after(anchor_b)

        assert next_b > next_a
        assert next_b - next_a == pytest.approx(3600, abs=1)

    def test_cron_every_minute(self):
        s = Schedule(cron="* * * * *")
        now = time.time()
        nxt = s.next_after(now)
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
        assert st.schedule_id
        assert st.created_at > 0


# ---------------------------------------------------------------------------
# pool.schedule()
# ---------------------------------------------------------------------------


class TestPoolSchedule:
    def test_schedule_returns_schedule_id(self, fake_redis, pool):
        sid = pool.schedule(
            "refresh_cache",
            RefreshContext(scope="all"),
            Schedule(cron="*/5 * * * *"),
        )
        assert isinstance(sid, str)
        assert len(sid) > 0

    def test_schedule_stores_in_redis(self, fake_redis, pool):
        sid = pool.schedule(
            "refresh_cache",
            RefreshContext(scope="all"),
            Schedule(cron="*/5 * * * *"),
        )

        data = fake_redis.get(_schedule_key(sid))
        assert data is not None

        st = ScheduledTask.model_validate_json(data)
        assert st.task_name == "refresh_cache"
        assert st.context["scope"] == "all"

    def test_schedule_indexes_in_sorted_set(self, fake_redis, pool):
        sid = pool.schedule(
            "refresh_cache",
            RefreshContext(scope="all"),
            Schedule(cron="*/5 * * * *"),
        )

        members = fake_redis.zrange(_queue_key(), 0, -1, withscores=True)
        assert len(members) == 1
        assert members[0][0].decode() == sid

    def test_schedule_rejects_unregistered_task(self, fake_redis, pool):
        with pytest.raises(ValueError, match="not registered"):
            pool.schedule(
                "nonexistent_task",
                RefreshContext(scope="all"),
                Schedule(cron="*/5 * * * *"),
            )

    def test_schedule_with_metadata(self, fake_redis, pool):
        sid = pool.schedule(
            "refresh_cache",
            RefreshContext(scope="all"),
            Schedule(cron="*/5 * * * *"),
            metadata={"org_id": "org-123"},
        )
        data = fake_redis.get(_schedule_key(sid))
        st = ScheduledTask.model_validate_json(data)
        assert st.metadata == {"org_id": "org-123"}

    def test_schedule_with_repeat(self, fake_redis, pool):
        sid = pool.schedule(
            "refresh_cache",
            RefreshContext(scope="all"),
            Schedule(cron="*/5 * * * *", repeat=3),
        )
        data = fake_redis.get(_schedule_key(sid))
        st = ScheduledTask.model_validate_json(data)
        assert st.schedule.repeat == 3

    def test_schedule_available_via_ax(self):
        """Schedule class should be importable from the top-level package."""
        assert hasattr(ax, "Schedule")
        assert ax.Schedule is Schedule


# ---------------------------------------------------------------------------
# tick — the scheduler heartbeat
# ---------------------------------------------------------------------------


def _force_due(fake_redis, sid):
    """Helper: set a schedule's next_run to the past so tick() picks it up."""
    data = fake_redis.get(_schedule_key(sid))
    st = ScheduledTask.model_validate_json(data)
    st.next_run = time.time() - 10
    fake_redis.set(_schedule_key(sid), st.model_dump_json().encode())
    fake_redis.zadd(_queue_key(), {sid: st.next_run})
    return st


class TestTick:
    async def test_tick_enqueues_due_task(self, fake_redis, pool, mock_activity_create):
        sid = pool.schedule(
            "refresh_cache",
            RefreshContext(scope="all"),
            Schedule(cron="*/5 * * * *"),
        )
        _force_due(fake_redis, sid)

        count = await tick()
        assert count == 1

    async def test_tick_skips_future_tasks(self, fake_redis, pool, mock_activity_create):
        pool.schedule(
            "refresh_cache",
            RefreshContext(scope="all"),
            Schedule(cron="*/5 * * * *"),
        )
        # Default next_run is in the future
        count = await tick()
        assert count == 0

    async def test_tick_removes_one_shot_schedule(self, fake_redis, pool, mock_activity_create):
        sid = pool.schedule(
            "refresh_cache",
            RefreshContext(scope="all"),
            Schedule(cron="* * * * *", repeat=0),
        )
        _force_due(fake_redis, sid)

        await tick()

        assert fake_redis.get(_schedule_key(sid)) is None
        assert fake_redis.zcard(_queue_key()) == 0

    async def test_tick_decrements_repeat_count(self, fake_redis, pool, mock_activity_create):
        sid = pool.schedule(
            "refresh_cache",
            RefreshContext(scope="all"),
            Schedule(cron="*/5 * * * *", repeat=3),
        )
        old_st = _force_due(fake_redis, sid)

        await tick()

        data = fake_redis.get(_schedule_key(sid))
        updated = ScheduledTask.model_validate_json(data)
        assert updated.schedule.repeat == 2
        assert updated.next_run > old_st.next_run

    async def test_tick_infinite_repeat_stays_negative(self, fake_redis, pool, mock_activity_create):
        sid = pool.schedule(
            "refresh_cache",
            RefreshContext(scope="all"),
            Schedule(cron="*/5 * * * *", repeat=-1),
        )
        _force_due(fake_redis, sid)

        await tick()

        data = fake_redis.get(_schedule_key(sid))
        updated = ScheduledTask.model_validate_json(data)
        assert updated.schedule.repeat == -1

    async def test_tick_anchor_based_rescheduling(self, fake_redis, pool, mock_activity_create):
        sid = pool.schedule(
            "refresh_cache",
            RefreshContext(scope="all"),
            Schedule(cron="*/5 * * * *"),
        )
        old_st = _force_due(fake_redis, sid)

        await tick()

        data = fake_redis.get(_schedule_key(sid))
        updated = ScheduledTask.model_validate_json(data)
        # Next run computed from the anchor (old next_run), not from now
        assert updated.next_run > old_st.next_run

    async def test_tick_handles_stale_sorted_set_entries(self, fake_redis, pool, mock_activity_create):
        fake_redis.zadd(_queue_key(), {"orphan-id": time.time() - 100})

        count = await tick()
        assert count == 0
        assert fake_redis.zcard(_queue_key()) == 0

    async def test_tick_multiple_due_tasks(self, fake_redis, pool, mock_activity_create):
        for i in range(3):
            sid = pool.schedule(
                "refresh_cache",
                RefreshContext(scope=f"scope_{i}"),
                Schedule(cron="*/5 * * * *"),
            )
            _force_due(fake_redis, sid)

        count = await tick()
        assert count == 3

    async def test_context_payload_preserved(self, fake_redis, pool, mock_activity_create):
        sid = pool.schedule(
            "refresh_cache",
            RefreshContext(scope="users", ttl=999),
            Schedule(cron="*/5 * * * *"),
        )

        data = fake_redis.get(_schedule_key(sid))
        st = ScheduledTask.model_validate_json(data)
        assert st.context["scope"] == "users"
        assert st.context["ttl"] == 999


# ---------------------------------------------------------------------------
# Timezone configuration
# ---------------------------------------------------------------------------


class TestTimezone:
    def test_default_timezone_is_server_local(self):
        """Default should be the server's local timezone, not hardcoded UTC."""
        from agentexec.config import CONF, _detect_local_timezone

        assert CONF.scheduler_timezone == _detect_local_timezone()

    def test_scheduler_tz_returns_zoneinfo(self):
        from agentexec.config import CONF

        tz = CONF.scheduler_tz
        assert isinstance(tz, ZoneInfo)

    def test_cron_respects_configured_timezone(self, monkeypatch):
        """Cron evaluation should use the configured timezone."""
        from agentexec.config import CONF

        monkeypatch.setattr(CONF, "scheduler_timezone", "America/New_York")

        s = Schedule(cron="0 9 * * *")  # 9 AM
        # Use a known timestamp: 2024-01-15 14:00:00 UTC = 9:00 AM ET
        anchor = datetime(2024, 1, 15, 9, 0, 0, tzinfo=ZoneInfo("America/New_York")).timestamp()
        nxt = s.next_after(anchor)

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
