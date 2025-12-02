"""Test self-describing result serialization (pickle-like behavior with JSON)."""

import uuid

import pytest
from pydantic import BaseModel

import agentexec as ax
from agentexec import state


class DummyContext(BaseModel):
    """Dummy context for testing."""

    pass


class ResearchResult(BaseModel):
    """Sample result model."""

    company: str
    valuation: int


class AnalysisResult(BaseModel):
    """Another result model."""

    conclusion: str
    confidence: float


class NestedData(BaseModel):
    """Nested data structure for testing."""

    items: list[str]
    metadata: dict[str, int]


class ComplexResult(BaseModel):
    """Complex result with nested structure."""

    status: str
    data: NestedData


async def test_gather_without_task_definitions(monkeypatch) -> None:
    """Test that gather() works without needing TaskDefinitions.

    This demonstrates that results are self-describing - they include
    their type information, so we can deserialize without a registry.
    """
    # Create tasks without TaskDefinitions (as enqueue() does)
    task1 = ax.Task(
        task_name="research",
        context=DummyContext(),
        agent_id=uuid.uuid4(),
    )
    task2 = ax.Task(
        task_name="analysis",
        context=DummyContext(),
        agent_id=uuid.uuid4(),
    )

    # Store results with type information
    result1 = ResearchResult(company="Anthropic", valuation=1000000)
    result2 = AnalysisResult(conclusion="Strong", confidence=0.95)

    # Mock backend storage
    storage = {}

    def mock_format_key(*args):
        return ":".join(args)

    async def mock_aset(key, value, ttl_seconds=None):
        storage[key] = value
        return True

    async def mock_aget(key):
        return storage.get(key)

    monkeypatch.setattr(state.backend, "format_key", mock_format_key)
    monkeypatch.setattr(state.backend, "aset", mock_aset)
    monkeypatch.setattr(state.backend, "aget", mock_aget)

    await state.aset_result(task1.agent_id, result1)
    await state.aset_result(task2.agent_id, result2)

    # Gather results - no TaskDefinition needed!
    results = await ax.gather(task1, task2)

    # Results are correctly typed
    assert isinstance(results[0], ResearchResult)
    assert isinstance(results[1], AnalysisResult)
    assert results[0].company == "Anthropic"
    assert results[1].confidence == 0.95


async def test_result_roundtrip_preserves_type() -> None:
    """Test that serialize → deserialize preserves exact type."""
    original = ResearchResult(company="Acme", valuation=500000)

    # Serialize
    serialized = state.backend.serialize(original)

    # Deserialize - should get back the same type
    deserialized = state.backend.deserialize(serialized)

    assert type(deserialized) is ResearchResult
    assert deserialized == original


async def test_nested_models_preserve_structure() -> None:
    """Test that nested Pydantic models are preserved."""
    original = ComplexResult(
        status="success",
        data=NestedData(items=["a", "b"], metadata={"count": 2}),
    )

    # Roundtrip
    serialized = state.backend.serialize(original)
    deserialized = state.backend.deserialize(serialized)

    assert type(deserialized) is ComplexResult
    assert type(deserialized.data) is NestedData
    assert deserialized.data.items == ["a", "b"]
    assert deserialized.data.metadata == {"count": 2}
