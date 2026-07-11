import asyncio
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel

import agentexec as ax
from agentexec.core.results import TaskFailedError, TaskFailure, gather, get_result, _get_result


class SampleContext(BaseModel):
    message: str


class SampleResult(BaseModel):
    status: str
    value: int


class ComplexResult(BaseModel):
    items: list[dict[str, int]]
    nested: dict[str, list[int]]


@pytest.fixture
def mock_get_result():
    """Mock the internal _get_result function."""
    with patch("agentexec.core.results._get_result") as mock:
        yield mock


async def test_get_result_returns_deserialized_data(mock_get_result) -> None:
    task = ax.Task(
        task_name="test_task",
        context={"message": "test"},
        agent_id=uuid.uuid4(),
    )
    expected_result = SampleResult(status="success", value=42)
    mock_get_result.return_value = expected_result

    result = await get_result(task, timeout=1)

    assert result == expected_result
    mock_get_result.assert_called_once_with(task.agent_id)


async def test_get_result_polls_until_available(mock_get_result) -> None:
    task = ax.Task(
        task_name="test_task",
        context={"message": "test"},
        agent_id=uuid.uuid4(),
    )
    expected_result = SampleResult(status="delayed", value=100)

    call_count = 0

    async def delayed_result(agent_id):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            return None
        return expected_result

    mock_get_result.side_effect = delayed_result

    result = await get_result(task, timeout=5)

    assert result == expected_result
    assert call_count == 3


async def test_get_result_timeout(mock_get_result) -> None:
    task = ax.Task(
        task_name="test_task",
        context={"message": "test"},
        agent_id=uuid.uuid4(),
    )
    mock_get_result.return_value = None

    with pytest.raises(TimeoutError, match=f"Result for {task.agent_id} not available"):
        await get_result(task, timeout=1)


async def test_gather_multiple_tasks(mock_get_result) -> None:
    task1 = ax.Task(
        task_name="task1",
        context={"message": "test1"},
        agent_id=uuid.uuid4(),
    )
    task2 = ax.Task(
        task_name="task2",
        context={"message": "test2"},
        agent_id=uuid.uuid4(),
    )

    result1 = SampleResult(status="task1", value=100)
    result2 = SampleResult(status="task2", value=200)

    async def mock_result(agent_id):
        if agent_id == task1.agent_id:
            return result1
        elif agent_id == task2.agent_id:
            return result2
        return None

    mock_get_result.side_effect = mock_result

    results = await gather(task1, task2)

    assert results == (result1, result2)
    assert len(results) == 2


async def test_gather_single_task(mock_get_result) -> None:
    task = ax.Task(
        task_name="single_task",
        context={"message": "test"},
        agent_id=uuid.uuid4(),
    )

    expected = SampleResult(status="single", value=1)
    mock_get_result.return_value = expected

    results = await gather(task)

    assert results == (expected,)


async def test_gather_preserves_order(mock_get_result) -> None:
    tasks = [
        ax.Task(
            task_name=f"task{i}",
            context={"message": f"msg{i}"},
            agent_id=uuid.uuid4(),
        )
        for i in range(5)
    ]

    results_map = {task.agent_id: SampleResult(status=f"result_{i}", value=i) for i, task in enumerate(tasks)}

    async def mock_result(agent_id):
        return results_map.get(agent_id)

    mock_get_result.side_effect = mock_result

    results = await gather(*tasks)

    expected = tuple(SampleResult(status=f"result_{i}", value=i) for i in range(5))
    assert results == expected


async def test_get_result_raises_on_task_failure(mock_get_result) -> None:
    """A stored TaskFailure record raises TaskFailedError immediately."""
    task = ax.Task(
        task_name="doomed_task",
        context={"message": "test"},
        agent_id=uuid.uuid4(),
    )
    mock_get_result.return_value = TaskFailure(
        task_name="doomed_task",
        agent_id=task.agent_id,
        error="boom",
        attempts=4,
    )

    with pytest.raises(TaskFailedError, match="failed after 4 attempts: boom") as exc_info:
        await get_result(task, timeout=30)

    assert exc_info.value.task_name == "doomed_task"
    assert exc_info.value.agent_id == task.agent_id
    assert exc_info.value.error == "boom"
    assert exc_info.value.attempts == 4
    # Raised on the first poll, not after the timeout.
    mock_get_result.assert_called_once_with(task.agent_id)


async def test_gather_propagates_task_failure(mock_get_result) -> None:
    """gather raises when any task has permanently failed."""
    ok_task = ax.Task(
        task_name="ok_task",
        context={"message": "ok"},
        agent_id=uuid.uuid4(),
    )
    doomed_task = ax.Task(
        task_name="doomed_task",
        context={"message": "doom"},
        agent_id=uuid.uuid4(),
    )

    async def mock_result(agent_id):
        if agent_id == ok_task.agent_id:
            return SampleResult(status="ok", value=1)
        return TaskFailure(
            task_name="doomed_task",
            agent_id=doomed_task.agent_id,
            error="fatal",
            attempts=4,
        )

    mock_get_result.side_effect = mock_result

    with pytest.raises(TaskFailedError, match="doomed_task"):
        await gather(ok_task, doomed_task, timeout=30)


async def test_get_result_with_complex_object(mock_get_result) -> None:
    task = ax.Task(
        task_name="test_task",
        context={"message": "test"},
        agent_id=uuid.uuid4(),
    )

    expected = ComplexResult(
        items=[{"a": 1}, {"b": 2}],
        nested={"key": [1, 2, 3]},
    )
    mock_get_result.return_value = expected

    result = await get_result(task, timeout=1)

    assert isinstance(result, ComplexResult)
    assert result.items == [{"a": 1}, {"b": 2}]
    assert result.nested == {"key": [1, 2, 3]}
