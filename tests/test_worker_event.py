"""Test state-backed event for cross-process coordination."""

import pytest
from fakeredis import aioredis as fake_aioredis
import fakeredis

from agentexec.worker.event import StateEvent


@pytest.fixture
def fake_redis_sync(monkeypatch):
    """Setup fake sync redis for state backend."""
    fake_redis = fakeredis.FakeRedis(decode_responses=False)

    def get_fake_sync_client():
        return fake_redis

    monkeypatch.setattr("agentexec.state.redis_backend._get_sync_client", get_fake_sync_client)

    yield fake_redis


@pytest.fixture
def fake_redis_async(monkeypatch):
    """Setup fake async redis for state backend."""
    fake_redis = fake_aioredis.FakeRedis(decode_responses=False)

    def get_fake_async_client():
        return fake_redis

    monkeypatch.setattr("agentexec.state.redis_backend._get_async_client", get_fake_async_client)

    yield fake_redis


def test_state_event_initialization():
    """Test StateEvent can be initialized with name and id."""
    event = StateEvent("test", "event123")

    assert event.name == "test"
    assert event.id == "event123"


def test_redis_event_set(fake_redis_sync):
    """Test StateEvent.set() sets the key in Redis."""
    event = StateEvent("shutdown", "pool1")

    event.set()

    # Verify the key was set (with event prefix and formatted name:id)
    value = fake_redis_sync.get("agentexec:event:shutdown:pool1")
    assert value == b"1"


def test_redis_event_clear(fake_redis_sync):
    """Test StateEvent.clear() removes the key from Redis."""
    event = StateEvent("shutdown", "pool2")

    # Set then clear
    fake_redis_sync.set("agentexec:event:shutdown:pool2", "1")
    event.clear()

    # Verify the key was removed
    value = fake_redis_sync.get("agentexec:event:shutdown:pool2")
    assert value is None


def test_redis_event_clear_nonexistent(fake_redis_sync):
    """Test StateEvent.clear() handles non-existent keys gracefully."""
    event = StateEvent("nonexistent", "id123")

    # Should not raise an error
    event.clear()


async def test_redis_event_is_set_true(fake_redis_async):
    """Test StateEvent.is_set() returns True when key exists."""
    event = StateEvent("shutdown", "pool3")

    # Set the key
    await fake_redis_async.set("agentexec:event:shutdown:pool3", "1")

    # Check is_set
    result = await event.is_set()
    assert result is True


async def test_redis_event_is_set_false(fake_redis_async):
    """Test StateEvent.is_set() returns False when key doesn't exist."""
    event = StateEvent("shutdown", "pool4")

    # Don't set the key
    result = await event.is_set()
    assert result is False


async def test_redis_event_is_set_after_clear(fake_redis_sync, fake_redis_async):
    """Test StateEvent.is_set() returns False after clear()."""
    event = StateEvent("shutdown", "pool5")

    # Set then clear
    event.set()
    event.clear()

    # Check is_set
    result = await event.is_set()
    assert result is False


def test_redis_event_picklable():
    """Test StateEvent is picklable (for multiprocessing)."""
    import pickle

    event = StateEvent("shutdown", "pickle123")

    # Pickle and unpickle
    pickled = pickle.dumps(event)
    unpickled = pickle.loads(pickled)

    assert unpickled.name == "shutdown"
    assert unpickled.id == "pickle123"


def test_redis_event_multiple_events():
    """Test multiple StateEvent instances with different names."""
    event1 = StateEvent("event", "id1")
    event2 = StateEvent("event", "id2")

    assert event1.id != event2.id
    assert event1.name == "event"
    assert event2.name == "event"
    assert event1.id == "id1"
    assert event2.id == "id2"
