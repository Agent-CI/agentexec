"""Test Task data structure and serialization."""

import json
import uuid

import pytest


def test_task_creation() -> None:
    """Test that tasks can be created with minimal parameters."""
    from agentexec import Task

    task = Task(task_name="test_task", payload={"message": "hello"}, agent_id=uuid.uuid4())

    assert task.task_name == "test_task"
    assert task.payload == {"message": "hello"}
    assert task.agent_id is not None
    assert isinstance(task.agent_id, uuid.UUID)


def test_task_with_custom_agent_id() -> None:
    """Test that tasks can be created with a custom agent_id."""
    from agentexec import Task

    custom_id = uuid.uuid4()
    task = Task(
        task_name="test_task",
        payload={"message": "hello"},
        agent_id=custom_id,
    )

    assert task.agent_id == custom_id


def test_task_serialization() -> None:
    """Test that tasks can be serialized to JSON."""
    from agentexec import Task

    agent_id = uuid.uuid4()
    task = Task(
        task_name="test_task",
        payload={"message": "hello", "value": 42},
        agent_id=agent_id,
    )

    task_json = task.model_dump_json()
    task_data = json.loads(task_json)

    assert task_data["task_name"] == "test_task"
    assert task_data["payload"]["message"] == "hello"
    assert task_data["payload"]["value"] == 42
    assert task_data["agent_id"] == str(agent_id)


def test_task_deserialization() -> None:
    """Test that tasks can be deserialized from JSON."""
    from agentexec import Task

    agent_id = uuid.uuid4()
    task_json = json.dumps(
        {
            "task_name": "test_task",
            "payload": {"message": "hello", "value": 42},
            "agent_id": str(agent_id),
        }
    )

    task = Task.model_validate_json(task_json)

    assert task.task_name == "test_task"
    assert task.payload == {"message": "hello", "value": 42}
    assert task.agent_id == agent_id


def test_task_round_trip() -> None:
    """Test that tasks can be serialized and deserialized."""
    from agentexec import Task

    original = Task(
        task_name="test_task",
        payload={"message": "hello", "nested": {"key": "value"}},
        agent_id=uuid.uuid4(),
    )

    serialized = original.model_dump_json()
    deserialized = Task.model_validate_json(serialized)

    assert deserialized.task_name == original.task_name
    assert deserialized.payload == original.payload
    assert deserialized.agent_id == original.agent_id


def test_task_handler_kwargs() -> None:
    """Test that handler_kwargs has correct structure."""
    from agentexec import Task

    agent_id = uuid.uuid4()
    task = Task(
        task_name="test_task",
        payload={"message": "hello", "value": 42},
        agent_id=agent_id,
    )

    kwargs = task.handler_kwargs

    # Verify structure: payload and agent_id as separate keys
    assert "payload" in kwargs
    assert "agent_id" in kwargs
    assert kwargs["agent_id"] == agent_id
    assert kwargs["payload"]["message"] == "hello"
    assert kwargs["payload"]["value"] == 42
