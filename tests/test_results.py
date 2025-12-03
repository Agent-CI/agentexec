"""Test task result storage and retrieval."""

import asyncio
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel

import agentexec as ax
from agentexec.core.results import gather, get_result


class SampleContext(BaseModel):
    """Sample context for result tests."""

    message: str


class SampleResult(BaseModel):
    """Sample result model for tests."""

    status: str
    value: int


class ComplexResult(BaseModel):
    """Complex result model with nested data."""

    items: list[dict[str, int]]
    nested: dict[str, list[int]]


@pytest.fixture
def mock_state():
    """Mock the state module's aget_result function."""
    with patch("agentexec.core.results.state") as mock:
        yield mock


async def test_get_result_returns_deserialized_data(mock_state) -> None:
    """Test that get_result retrieves data from state."""
    task = ax.Task(
        task_name="test_task",
        context=SampleContext(message="test"),
        agent_id=uuid.uuid4(),
    )
    expected_result = SampleResult(status="success", value=42)

    # Mock aget_result to return the expected result
    mock_state.aget_result = AsyncMock(return_value=expected_result)

    result = await get_result(task, timeout=1)

    assert result == expected_result
    mock_state.aget_result.assert_called_once_with(task.agent_id)


async def test_get_result_polls_until_available(mock_state) -> None:
    """Test that get_result polls until result is available."""
    task = ax.Task(
        task_name="test_task",
        context=SampleContext(message="test"),
        agent_id=uuid.uuid4(),
    )
    expected_result = SampleResult(status="delayed", value=100)

    # Return None first, then the result
    call_count = 0

    async def delayed_result(agent_id):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            return None
        return expected_result

    mock_state.aget_result = delayed_result

    result = await get_result(task, timeout=5)

    assert result == expected_result
    assert call_count == 3


async def test_get_result_timeout(mock_state) -> None:
    """Test that get_result raises TimeoutError if result not available."""
    task = ax.Task(
        task_name="test_task",
        context=SampleContext(message="test"),
        agent_id=uuid.uuid4(),
    )

    # Always return None to trigger timeout
    mock_state.aget_result = AsyncMock(return_value=None)

    with pytest.raises(TimeoutError, match=f"Result for {task.agent_id} not available"):
        await get_result(task, timeout=0.5)


async def test_gather_multiple_tasks(mock_state) -> None:
    """Test that gather waits for multiple tasks and returns results."""
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

    result1 = SampleResult(status="task1", value=100)
    result2 = SampleResult(status="task2", value=200)

    # Mock to return different results for different agent_ids
    async def mock_aget_result(agent_id):
        if agent_id == task1.agent_id:
            return result1
        elif agent_id == task2.agent_id:
            return result2
        return None

    mock_state.aget_result = mock_aget_result

    results = await gather(task1, task2)

    assert results == (result1, result2)
    assert len(results) == 2


async def test_gather_single_task(mock_state) -> None:
    """Test that gather works with a single task."""
    task = ax.Task(
        task_name="single_task",
        context=SampleContext(message="test"),
        agent_id=uuid.uuid4(),
    )

    expected = SampleResult(status="single", value=1)
    mock_state.aget_result = AsyncMock(return_value=expected)

    results = await gather(task)

    assert results == (expected,)


async def test_gather_preserves_order(mock_state) -> None:
    """Test that gather returns results in the same order as input tasks."""
    tasks = [
        ax.Task(
            task_name=f"task{i}",
            context=SampleContext(message=f"msg{i}"),
            agent_id=uuid.uuid4(),
        )
        for i in range(5)
    ]

    # Create results mapped to task agent_ids
    results_map = {task.agent_id: SampleResult(status=f"result_{i}", value=i) for i, task in enumerate(tasks)}

    async def mock_aget_result(agent_id):
        return results_map.get(agent_id)

    mock_state.aget_result = mock_aget_result

    results = await gather(*tasks)

    # Results should be in task order
    expected = tuple(SampleResult(status=f"result_{i}", value=i) for i in range(5))
    assert results == expected


async def test_get_result_with_complex_object(mock_state) -> None:
    """Test that get_result handles complex BaseModel objects."""
    task = ax.Task(
        task_name="test_task",
        context=SampleContext(message="test"),
        agent_id=uuid.uuid4(),
    )

    expected = ComplexResult(
        items=[{"a": 1}, {"b": 2}],
        nested={"key": [1, 2, 3]},
    )
    mock_state.aget_result = AsyncMock(return_value=expected)

    result = await get_result(task, timeout=1)

    assert isinstance(result, ComplexResult)
    assert result.items == [{"a": 1}, {"b": 2}]
    assert result.nested == {"key": [1, 2, 3]}
