"""Test Pipeline orchestration functionality."""

from dataclasses import dataclass, field
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from agentexec.pipeline import Pipeline, StepDefinition


class InputContext(BaseModel):
    """Initial context for pipeline tests."""

    value: int


class IntermediateA(BaseModel):
    """Intermediate result A."""

    a_value: int


class IntermediateB(BaseModel):
    """Intermediate result B."""

    b_value: int


class FinalResult(BaseModel):
    """Final pipeline result."""

    result: str


@dataclass
class MockWorkerContext:
    """Mock context for testing."""

    tasks: dict = field(default_factory=dict)


@pytest.fixture
def mock_pool():
    """Create a mock Pool for testing."""
    pool = MagicMock()
    pool._context = MockWorkerContext()
    return pool


@pytest.fixture
def pipeline(mock_pool):
    """Create a Pipeline for testing."""
    return Pipeline(mock_pool)


def test_pipeline_initialization(mock_pool) -> None:
    """Test Pipeline can be initialized."""
    p = Pipeline(mock_pool)

    assert p._steps == {}
    assert p._user_pipeline_class is None
    assert p._pool is mock_pool


def test_step_decorator_registers_step(pipeline) -> None:
    """Test that @pipeline.step() decorator registers steps."""

    @pipeline.step(0)
    async def first_step(ctx: InputContext) -> IntermediateA:
        return IntermediateA(a_value=ctx.value * 2)

    assert "first_step" in pipeline._steps
    step_def = pipeline._steps["first_step"]
    assert isinstance(step_def, StepDefinition)
    assert step_def.name == "first_step"
    assert step_def.order == 0
    assert step_def.handler == first_step


def test_step_definition_captures_types(pipeline) -> None:
    """Test that step definition captures parameter and return types."""

    @pipeline.step(0)
    async def typed_step(ctx: InputContext) -> IntermediateA:
        return IntermediateA(a_value=ctx.value)

    step_def = pipeline._steps["typed_step"]
    assert step_def.return_type == IntermediateA
    assert step_def.param_types == {"ctx": InputContext}


def test_base_class_registration(pipeline) -> None:
    """Test that inheriting from pipeline.Base registers the class."""

    class MyPipeline(pipeline.Base):
        @pipeline.step(0)
        async def step_one(self, ctx: InputContext) -> IntermediateA:
            return IntermediateA(a_value=ctx.value)

    assert pipeline._user_pipeline_class is MyPipeline


async def test_pipeline_run_executes_steps_in_order(pipeline) -> None:
    """Test that pipeline.run() executes steps in order."""
    execution_order = []

    class OrderedPipeline(pipeline.Base):
        @pipeline.step(0)
        async def first(self, ctx: InputContext) -> IntermediateA:
            execution_order.append("first")
            return IntermediateA(a_value=ctx.value * 2)

        @pipeline.step(1)
        async def second(self, x: IntermediateA) -> IntermediateB:
            execution_order.append("second")
            return IntermediateB(b_value=x.a_value + 10)

        @pipeline.step(2)
        async def third(self, y: IntermediateB) -> FinalResult:
            execution_order.append("third")
            return FinalResult(result=f"result: {y.b_value}")

    result = await pipeline.run(InputContext(value=5))

    assert execution_order == ["first", "second", "third"]
    assert result.result == "result: 20"  # (5 * 2) + 10 = 20


async def test_pipeline_run_without_class_raises(pipeline) -> None:
    """Test that pipeline.run() raises if no class is defined."""

    @pipeline.step(0)
    async def orphan_step(ctx: InputContext) -> IntermediateA:
        return IntermediateA(a_value=ctx.value)

    with pytest.raises(RuntimeError):
        await pipeline.run(InputContext(value=1))


def test_step_ordering_with_non_sequential_numbers(pipeline) -> None:
    """Test that steps can use non-sequential order values."""

    @pipeline.step(10)
    async def later(x: IntermediateA) -> FinalResult:
        return FinalResult(result=str(x.a_value))

    @pipeline.step(5)
    async def earlier(ctx: InputContext) -> IntermediateA:
        return IntermediateA(a_value=ctx.value)

    steps = sorted(pipeline._steps.values(), key=lambda s: s.order)

    assert steps[0].name == "earlier"
    assert steps[1].name == "later"


def test_step_ordering_with_string_keys(pipeline) -> None:
    """Test that steps can use string order values."""

    @pipeline.step("b")
    async def second_step(x: IntermediateA) -> FinalResult:
        return FinalResult(result=str(x.a_value))

    @pipeline.step("a")
    async def first_step(ctx: InputContext) -> IntermediateA:
        return IntermediateA(a_value=ctx.value)

    steps = sorted(pipeline._steps.values(), key=lambda s: s.order)

    assert steps[0].name == "first_step"
    assert steps[1].name == "second_step"
