import uuid

import pytest
from pydantic import BaseModel

import agentexec as ax
from agentexec.state import KEY_RESULT, backend


class DummyContext(BaseModel):
    pass


class ResearchResult(BaseModel):
    company: str
    valuation: int


class AnalysisResult(BaseModel):
    conclusion: str
    confidence: float


class NestedData(BaseModel):
    items: list[str]
    metadata: dict[str, int]


class ComplexResult(BaseModel):
    status: str
    data: NestedData


async def test_gather_without_task_definitions(monkeypatch) -> None:
    """Test that gather() works without needing TaskDefinitions."""
    task1 = ax.Task(
        task_name="research",
        context={},
        agent_id=uuid.uuid4(),
    )
    task2 = ax.Task(
        task_name="analysis",
        context={},
        agent_id=uuid.uuid4(),
    )

    result1 = ResearchResult(company="Anthropic", valuation=1000000)
    result2 = AnalysisResult(conclusion="Strong", confidence=0.95)

    # Mock backend storage
    storage = {}

    async def mock_state_set(key, value, ttl_seconds=None):
        storage[key] = value
        return True

    async def mock_state_get(key):
        return storage.get(key)

    monkeypatch.setattr(backend.state, "set", mock_state_set)
    monkeypatch.setattr(backend.state, "get", mock_state_get)

    # Store results via the same path task.execute() would
    for task, result in [(task1, result1), (task2, result2)]:
        key = backend.format_key(*KEY_RESULT, str(task.agent_id))
        await backend.state.set(key, backend.serialize(result))

    # Gather results
    results = await ax.gather(task1, task2)

    assert isinstance(results[0], ResearchResult)
    assert isinstance(results[1], AnalysisResult)
    assert results[0].company == "Anthropic"
    assert results[1].confidence == 0.95


async def test_result_roundtrip_preserves_type() -> None:
    """Test that serialize → deserialize preserves exact type."""
    original = ResearchResult(company="Acme", valuation=500000)

    serialized = backend.serialize(original)
    deserialized = backend.deserialize(serialized)

    assert type(deserialized) is ResearchResult
    assert deserialized == original


async def test_nested_models_preserve_structure() -> None:
    """Test that nested Pydantic models are preserved."""
    original = ComplexResult(
        status="success",
        data=NestedData(items=["a", "b"], metadata={"count": 2}),
    )

    serialized = backend.serialize(original)
    deserialized = backend.deserialize(serialized)

    assert type(deserialized) is ComplexResult
    assert type(deserialized.data) is NestedData
    assert deserialized.data.items == ["a", "b"]
    assert deserialized.data.metadata == {"count": 2}
