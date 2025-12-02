"""Test task result storage and retrieval."""

import asyncio
import pickle
import uuid

import pytest
from fakeredis import aioredis as fake_aioredis
from pydantic import BaseModel

import agentexec as ax
from agentexec.core.results import gather, get_result


class SampleContext(BaseModel):
    """Sample context for result tests."""

    message: str


@pytest.fixture
def fake_redis(monkeypatch):
    """Setup fake async redis."""
    fake_redis = fake_aioredis.FakeRedis(decode_responses=False)

    def get_fake_redis():
        return fake_redis

    monkeypatch.setattr("agentexec.core.results.get_redis", get_fake_redis)

    yield fake_redis


async def test_get_result_returns_pickled_data(fake_redis) -> None:
    """Test that get_result retrieves and unpickles data from Redis."""
    task = ax.Task(
        task_name="test_task",
        context=SampleContext(message="test"),
        agent_id=uuid.uuid4(),
    )
    expected_result = {"status": "success", "data": [1, 2, 3]}

    # Store pickled result in Redis
    await fake_redis.set(f"result:{task.agent_id}", pickle.dumps(expected_result))

    # Retrieve the result
    result = await get_result(task, timeout=1)

    assert result == expected_result


async def test_get_result_polls_until_available(fake_redis) -> None:
    """Test that get_result polls Redis until result is available."""
    task = ax.Task(
        task_name="test_task",
        context=SampleContext(message="test"),
        agent_id=uuid.uuid4(),
    )
    expected_result = "delayed_result"

    async def set_result_after_delay():
        await asyncio.sleep(0.6)  # Wait a bit more than one poll interval
        await fake_redis.set(f"result:{task.agent_id}", pickle.dumps(expected_result))

    # Start the delayed setter
    asyncio.create_task(set_result_after_delay())

    # get_result should wait and eventually get the result
    result = await get_result(task, timeout=2)

    assert result == expected_result


async def test_get_result_timeout(fake_redis) -> None:
    """Test that get_result raises TimeoutError if result not available."""
    task = ax.Task(
        task_name="test_task",
        context=SampleContext(message="test"),
        agent_id=uuid.uuid4(),
    )

    with pytest.raises(TimeoutError, match=f"Result for {task.agent_id} not available"):
        await get_result(task, timeout=0.5)


async def test_gather_multiple_tasks(fake_redis) -> None:
    """Test that gather waits for multiple tasks and returns results."""
    # Create test tasks
    task1 = ax.Task(
        task_name="task1",
        context=SampleContext(message="test1"),
        agent_id=uuid.uuid4(),
    )
    task2 = ax.Task(
        task_name="task2",
        context=SampleContext(message="test2"),
        agent_id=uuid.uuid4(),
    )

    result1 = {"task": "task1", "value": 100}
    result2 = {"task": "task2", "value": 200}

    # Store results
    await fake_redis.set(f"result:{task1.agent_id}", pickle.dumps(result1))
    await fake_redis.set(f"result:{task2.agent_id}", pickle.dumps(result2))

    # Gather results
    results = await gather(task1, task2)

    assert results == (result1, result2)
    assert len(results) == 2


async def test_gather_single_task(fake_redis) -> None:
    """Test that gather works with a single task."""
    task = ax.Task(
        task_name="single_task",
        context=SampleContext(message="test"),
        agent_id=uuid.uuid4(),
    )

    expected = "single_result"
    await fake_redis.set(f"result:{task.agent_id}", pickle.dumps(expected))

    results = await gather(task)

    assert results == (expected,)


async def test_gather_preserves_order(fake_redis) -> None:
    """Test that gather returns results in the same order as input tasks."""
    tasks = [
        ax.Task(
            task_name=f"task{i}",
            context=SampleContext(message=f"msg{i}"),
            agent_id=uuid.uuid4(),
        )
        for i in range(5)
    ]

    # Store results in reverse order to test ordering
    for i, task in enumerate(reversed(tasks)):
        await fake_redis.set(f"result:{task.agent_id}", pickle.dumps(f"result_{4-i}"))

    results = await gather(*tasks)

    # Results should be in task order, not storage order
    assert results == ("result_0", "result_1", "result_2", "result_3", "result_4")


async def test_get_result_with_complex_object(fake_redis) -> None:
    """Test that get_result handles complex pickled objects."""
    task = ax.Task(
        task_name="test_task",
        context=SampleContext(message="test"),
        agent_id=uuid.uuid4(),
    )

    # Use a dict with complex structure instead of a custom class
    # (local classes can't be pickled)
    expected = {
        "value": 42,
        "nested": {"key": [1, 2, 3]},
        "items": [{"a": 1}, {"b": 2}],
    }
    await fake_redis.set(f"result:{task.agent_id}", pickle.dumps(expected))

    result = await get_result(task, timeout=1)

    assert result["value"] == 42
    assert result["nested"] == {"key": [1, 2, 3]}
    assert result["items"] == [{"a": 1}, {"b": 2}]
