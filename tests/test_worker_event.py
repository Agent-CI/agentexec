import pytest
from fakeredis import aioredis as fake_aioredis

from agentexec.state import backend
from agentexec.worker.event import StateEvent


@pytest.fixture
def fake_redis(monkeypatch):
    """Inject fake redis into the backend."""
    fake = fake_aioredis.FakeRedis(decode_responses=False)
    monkeypatch.setattr(backend, "_client", fake)
    yield fake


def test_state_event_initialization():
    event = StateEvent("test", "event123")
    assert event.name == "test"
    assert event.id == "event123"


async def test_redis_event_set(fake_redis):
    event = StateEvent("shutdown", "pool1")
    await event.set()
    value = await fake_redis.get("agentexec:event:shutdown:pool1")
    assert value == b"1"


async def test_redis_event_clear(fake_redis):
    event = StateEvent("shutdown", "pool2")
    await fake_redis.set("agentexec:event:shutdown:pool2", "1")
    await event.clear()
    value = await fake_redis.get("agentexec:event:shutdown:pool2")
    assert value is None


async def test_redis_event_clear_nonexistent(fake_redis):
    event = StateEvent("nonexistent", "id123")
    await event.clear()


async def test_redis_event_is_set_true(fake_redis):
    event = StateEvent("shutdown", "pool3")
    await fake_redis.set("agentexec:event:shutdown:pool3", "1")
    assert await event.is_set() is True


async def test_redis_event_is_set_false(fake_redis):
    event = StateEvent("shutdown", "pool4")
    assert await event.is_set() is False


async def test_redis_event_is_set_after_clear(fake_redis):
    event = StateEvent("shutdown", "pool5")
    await event.set()
    await event.clear()
    assert await event.is_set() is False


def test_redis_event_picklable():
    import pickle
    event = StateEvent("shutdown", "pickle123")
    unpickled = pickle.loads(pickle.dumps(event))
    assert unpickled.name == "shutdown"
    assert unpickled.id == "pickle123"


def test_redis_event_multiple_events():
    event1 = StateEvent("event", "id1")
    event2 = StateEvent("event", "id2")
    assert event1.id != event2.id
