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

    b_value: str


class FinalResult(BaseModel):
    """Final pipeline result."""

    combined: str


@dataclass
class MockWorkerContext:
    """Mock WorkerContext for testing."""

    tasks: dict = field(default_factory=dict)


@pytest.fixture
def mock_pool():
    """Create a mock WorkerPool for testing."""
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
    assert p._pipeline_class is None
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

    assert pipeline._pipeline_class is MyPipeline


async def test_pipeline_run_executes_steps_in_order(pipeline) -> None:
    """Test that pipeline.run() executes steps in order."""
    execution_order = []

    class OrderedPipeline(pipeline.Base):
        @pipeline.step(0)
        async def first(self, ctx: InputContext) -> int:
            execution_order.append("first")
            return ctx.value * 2

        @pipeline.step(1)
        async def second(self, x: int) -> int:
            execution_order.append("second")
            return x + 10

        @pipeline.step(2)
        async def third(self, y: int) -> str:
            execution_order.append("third")
            return f"result: {y}"

    result = await pipeline.run(None, InputContext(value=5))

    assert execution_order == ["first", "second", "third"]
    assert result == "result: 20"  # (5 * 2) + 10 = 20


async def test_pipeline_run_without_class_raises(pipeline) -> None:
    """Test that pipeline.run() raises if no class is defined."""
    with pytest.raises(RuntimeError, match="Pipeline must inherit from pipeline.Base"):
        await pipeline.run(None, InputContext(value=1))


async def test_pipeline_passes_output_to_next_step(pipeline) -> None:
    """Test that step output is passed as input to next step."""

    class PassingPipeline(pipeline.Base):
        @pipeline.step(0)
        async def produce(self, ctx: InputContext) -> int:
            return ctx.value * 100

        @pipeline.step(1)
        async def consume(self, value: int) -> str:
            return f"got {value}"

    result = await pipeline.run(None, InputContext(value=7))
    assert result == "got 700"


async def test_pipeline_handles_tuple_return(pipeline) -> None:
    """Test that pipeline handles tuple return types correctly."""

    class TuplePipeline(pipeline.Base):
        @pipeline.step(0)
        async def split(self, ctx: InputContext) -> tuple[int, str]:
            return (ctx.value, f"str_{ctx.value}")

        @pipeline.step(1)
        async def combine(self, num: int, text: str) -> str:
            return f"{text}_{num * 2}"

    result = await pipeline.run(None, InputContext(value=5))
    assert result == "str_5_10"


def test_verify_type_flow_count_mismatch(pipeline) -> None:
    """Test that type verification catches count mismatches."""

    @pipeline.step(0)
    async def returns_two(ctx: InputContext) -> tuple[int, str]:
        return (1, "a")

    @pipeline.step(1)
    async def expects_one(x: int) -> str:
        return str(x)

    class BadPipeline(pipeline.Base):
        pass

    # Add methods to class after definition to avoid registration issues
    BadPipeline.returns_two = returns_two
    BadPipeline.expects_one = expects_one

    with pytest.raises(TypeError, match="returns 2 values.*expects 1 parameters"):
        pipeline._validate_type_flow()


def test_verify_type_flow_type_mismatch(pipeline) -> None:
    """Test that type verification catches type mismatches."""

    @pipeline.step(0)
    async def returns_int(ctx: InputContext) -> int:
        return 1

    @pipeline.step(1)
    async def expects_str(x: str) -> str:
        return x

    class TypeMismatchPipeline(pipeline.Base):
        pass

    with pytest.raises(TypeError, match="Type mismatch"):
        pipeline._validate_type_flow()


def test_verify_type_flow_allows_none_types(pipeline) -> None:
    """Test that type verification skips None return types."""

    @pipeline.step(0)
    async def no_return_type(ctx: InputContext):
        return 42

    @pipeline.step(1)
    async def expects_int(x: int) -> str:
        return str(x)

    class NoTypePipeline(pipeline.Base):
        pass

    # Should not raise - None return type is skipped
    pipeline._verify_type_flow()


def test_step_ordering_with_non_sequential_numbers(pipeline) -> None:
    """Test that steps can use non-sequential order values."""

    @pipeline.step(10)
    async def later(x: int) -> str:
        return str(x)

    @pipeline.step(5)
    async def earlier(ctx: InputContext) -> int:
        return ctx.value

    steps = sorted(pipeline._steps.values(), key=lambda s: s.order)

    assert steps[0].name == "earlier"
    assert steps[1].name == "later"


def test_step_ordering_with_string_keys(pipeline) -> None:
    """Test that steps can use string order values."""

    @pipeline.step("b")
    async def second_step(x: int) -> str:
        return str(x)

    @pipeline.step("a")
    async def first_step(ctx: InputContext) -> int:
        return ctx.value

    steps = sorted(pipeline._steps.values(), key=lambda s: s.order)

    assert steps[0].name == "first_step"
    assert steps[1].name == "second_step"


async def test_pipeline_type_verification_rejects_mismatched_params(pipeline) -> None:
    """Test pipeline raises TypeError when step returns value but next step takes no params."""

    class MismatchedPipeline(pipeline.Base):
        @pipeline.step(0)
        async def first(self, ctx: InputContext) -> int:
            return ctx.value

        @pipeline.step(1)
        async def second(self) -> str:
            return "no params"

    with pytest.raises(TypeError, match="returns 1 values.*expects 0 parameters"):
        await pipeline.run(None, InputContext(value=42))
