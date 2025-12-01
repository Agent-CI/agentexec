"""Test Redis-backed event for cross-process coordination."""

import pytest
from fakeredis import aioredis as fake_aioredis
import fakeredis

from agentexec.worker.event import RedisEvent


@pytest.fixture
def fake_redis_sync(monkeypatch):
    """Setup fake sync redis."""
    fake_redis = fakeredis.FakeRedis(decode_responses=False)

    def get_fake_redis_sync():
        return fake_redis

    monkeypatch.setattr("agentexec.worker.event.get_redis_sync", get_fake_redis_sync)

    yield fake_redis


@pytest.fixture
def fake_redis_async(monkeypatch):
    """Setup fake async redis."""
    fake_redis = fake_aioredis.FakeRedis(decode_responses=False)

    def get_fake_redis():
        return fake_redis

    monkeypatch.setattr("agentexec.worker.event.get_redis", get_fake_redis)

    yield fake_redis


def test_redis_event_initialization():
    """Test RedisEvent can be initialized with a key."""
    event = RedisEvent(key="test:event:key")

    assert event.key == "test:event:key"


def test_redis_event_set(fake_redis_sync):
    """Test RedisEvent.set() sets the key in Redis."""
    event = RedisEvent(key="test:shutdown")

    event.set()

    # Verify the key was set
    value = fake_redis_sync.get("test:shutdown")
    assert value == b"1"


def test_redis_event_clear(fake_redis_sync):
    """Test RedisEvent.clear() removes the key from Redis."""
    event = RedisEvent(key="test:shutdown")

    # Set then clear
    fake_redis_sync.set("test:shutdown", "1")
    event.clear()

    # Verify the key was removed
    value = fake_redis_sync.get("test:shutdown")
    assert value is None


def test_redis_event_clear_nonexistent(fake_redis_sync):
    """Test RedisEvent.clear() handles non-existent keys gracefully."""
    event = RedisEvent(key="nonexistent:key")

    # Should not raise an error
    event.clear()


async def test_redis_event_is_set_true(fake_redis_async):
    """Test RedisEvent.is_set() returns True when key exists."""
    event = RedisEvent(key="test:shutdown")

    # Set the key
    await fake_redis_async.set("test:shutdown", "1")

    # Check is_set
    result = await event.is_set()
    assert result is True


async def test_redis_event_is_set_false(fake_redis_async):
    """Test RedisEvent.is_set() returns False when key doesn't exist."""
    event = RedisEvent(key="test:shutdown")

    # Don't set the key
    result = await event.is_set()
    assert result is False


async def test_redis_event_is_set_after_clear(fake_redis_sync, fake_redis_async):
    """Test RedisEvent.is_set() returns False after clear()."""
    event = RedisEvent(key="test:shutdown")

    # Set then clear
    event.set()
    event.clear()

    # Check is_set
    result = await event.is_set()
    assert result is False


def test_redis_event_picklable():
    """Test RedisEvent is picklable (for multiprocessing)."""
    import pickle

    event = RedisEvent(key="test:shutdown:pickle")

    # Pickle and unpickle
    pickled = pickle.dumps(event)
    unpickled = pickle.loads(pickled)

    assert unpickled.key == "test:shutdown:pickle"


def test_redis_event_multiple_events():
    """Test multiple RedisEvent instances with different keys."""
    event1 = RedisEvent(key="event:1")
    event2 = RedisEvent(key="event:2")

    assert event1.key != event2.key
    assert event1.key == "event:1"
    assert event2.key == "event:2"
