"""Test Pipeline type flow validation.

This module tests the type checking between pipeline steps, ensuring that:
- Return types from one step match parameter types of the next step
- Tuple returns are properly unpacked into multiple parameters
- Type mismatches are caught at validation time
- Subclass relationships are respected
"""

from dataclasses import dataclass, field
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from agentexec.pipeline import Pipeline


# =============================================================================
# Test Models
# =============================================================================


class Context(BaseModel):
    """Input context for pipeline tests."""

    value: str


class ResultA(BaseModel):
    """Base result type."""

    value: str


class ResultB(ResultA):
    """Derived result type (extends ResultA)."""

    pass


class ResultC(BaseModel):
    """Unrelated result type."""

    value: str


class Combined(BaseModel):
    """Combined result for tuple unpacking tests."""

    a: str
    b: str


# =============================================================================
# Fixtures
# =============================================================================


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


# =============================================================================
# Valid Flows - Single Value
# =============================================================================


class TestValidSingleValueFlows:
    """Test valid single-value flows between steps."""

    async def test_single_step(self, pipeline) -> None:
        """Test single step pipeline (Context -> Result)."""

        class SinglePipeline(pipeline.Base):
            @pipeline.step(0)
            async def process(self, ctx: Context) -> ResultA:
                return ResultA(value=f"processed {ctx.value}")

        result = await pipeline.run(Context(value="test"))
        assert result.value == "processed test"

    async def test_two_step_flow(self, pipeline) -> None:
        """Test two step pipeline (Context -> ResultA -> ResultC)."""

        class TwoStepPipeline(pipeline.Base):
            @pipeline.step(0)
            async def first(self, ctx: Context) -> ResultA:
                return ResultA(value=f"step1:{ctx.value}")

            @pipeline.step(1)
            async def second(self, result: ResultA) -> ResultC:
                return ResultC(value=f"step2:{result.value}")

        result = await pipeline.run(Context(value="input"))
        assert result.value == "step2:step1:input"

    async def test_three_step_flow(self, pipeline) -> None:
        """Test three step pipeline (Context -> ResultA -> ResultC -> Combined)."""

        class ThreeStepPipeline(pipeline.Base):
            @pipeline.step(0)
            async def first(self, ctx: Context) -> ResultA:
                return ResultA(value=ctx.value)

            @pipeline.step(1)
            async def second(self, a: ResultA) -> ResultC:
                return ResultC(value=f"from_a:{a.value}")

            @pipeline.step(2)
            async def third(self, c: ResultC) -> Combined:
                return Combined(a=c.value, b="done")

        result = await pipeline.run(Context(value="start"))
        assert result.a == "from_a:start"
        assert result.b == "done"

    async def test_subclass_accepted_for_base_type(self, pipeline) -> None:
        """Test that a derived type can be passed where base type is expected."""

        class SubclassPipeline(pipeline.Base):
            @pipeline.step(0)
            async def produce_derived(self, ctx: Context) -> ResultB:
                return ResultB(value="from_derived")

            @pipeline.step(1)
            async def consume_base(self, result: ResultA) -> ResultC:
                return ResultC(value=result.value)

        result = await pipeline.run(Context(value="x"))
        assert result.value == "from_derived"


# =============================================================================
# Valid Flows - Tuple Unpacking
# =============================================================================


class TestValidTupleFlows:
    """Test valid tuple return/parameter flows."""

    async def test_tuple_two_models(self, pipeline) -> None:
        """Test tuple of two Pydantic models unpacks correctly."""

        class TuplePipeline(pipeline.Base):
            @pipeline.step(0)
            async def split(self, ctx: Context) -> tuple[ResultA, ResultC]:
                return (
                    ResultA(value=f"a:{ctx.value}"),
                    ResultC(value=f"c:{ctx.value}"),
                )

            @pipeline.step(1)
            async def combine(self, a: ResultA, c: ResultC) -> Combined:
                return Combined(a=a.value, b=c.value)

        result = await pipeline.run(Context(value="test"))
        assert result.a == "a:test"
        assert result.b == "c:test"

    async def test_tuple_three_models(self, pipeline) -> None:
        """Test tuple of three Pydantic models unpacks correctly."""

        class TriplePipeline(pipeline.Base):
            @pipeline.step(0)
            async def split(self, ctx: Context) -> tuple[ResultA, ResultB, ResultC]:
                return (
                    ResultA(value="first"),
                    ResultB(value="second"),
                    ResultC(value="third"),
                )

            @pipeline.step(1)
            async def combine(self, a: ResultA, b: ResultB, c: ResultC) -> Combined:
                return Combined(a=f"{a.value}+{b.value}", b=c.value)

        result = await pipeline.run(Context(value="x"))
        assert result.a == "first+second"
        assert result.b == "third"

    async def test_tuple_same_type(self, pipeline) -> None:
        """Test tuple of same model type unpacks correctly."""

        class SameTypePipeline(pipeline.Base):
            @pipeline.step(0)
            async def split(self, ctx: Context) -> tuple[ResultA, ResultA]:
                return (
                    ResultA(value=f"left:{ctx.value}"),
                    ResultA(value=f"right:{ctx.value}"),
                )

            @pipeline.step(1)
            async def combine(self, left: ResultA, right: ResultA) -> Combined:
                return Combined(a=left.value, b=right.value)

        result = await pipeline.run(Context(value="data"))
        assert result.a == "left:data"
        assert result.b == "right:data"


# =============================================================================
# Invalid Flows - Count Mismatches
# =============================================================================


class TestInvalidCountMismatches:
    """Test that count mismatches between steps are caught."""

    def test_returns_two_expects_one(self, pipeline) -> None:
        """Test error when step returns tuple but next expects single value."""

        @pipeline.step(0)
        async def returns_two(ctx: Context) -> tuple[ResultA, ResultC]:
            return (ResultA(value="a"), ResultC(value="c"))

        @pipeline.step(1)
        async def expects_one(x: ResultA) -> Combined:
            return Combined(a=x.value, b="")

        class BadPipeline(pipeline.Base):
            pass

        BadPipeline.returns_two = returns_two
        BadPipeline.expects_one = expects_one

        with pytest.raises(TypeError):
            pipeline._validate_type_flow()

    def test_returns_one_expects_two(self, pipeline) -> None:
        """Test error when step returns single value but next expects tuple."""

        @pipeline.step(0)
        async def returns_one(ctx: Context) -> ResultA:
            return ResultA(value="a")

        @pipeline.step(1)
        async def expects_two(x: ResultA, y: ResultC) -> Combined:
            return Combined(a=x.value, b=y.value)

        class BadPipeline(pipeline.Base):
            pass

        BadPipeline.returns_one = returns_one
        BadPipeline.expects_two = expects_two

        with pytest.raises(TypeError):
            pipeline._validate_type_flow()

    async def test_returns_value_expects_none(self, pipeline) -> None:
        """Test error when step returns value but next takes no params."""

        class MismatchedPipeline(pipeline.Base):
            @pipeline.step(0)
            async def first(self, ctx: Context) -> ResultA:
                return ResultA(value=ctx.value)

            @pipeline.step(1)
            async def second(self) -> ResultC:
                return ResultC(value="no params")

        with pytest.raises(TypeError):
            await pipeline.run(Context(value="42"))


# =============================================================================
# Invalid Flows - Type Mismatches
# =============================================================================


class TestInvalidTypeMismatches:
    """Test that type mismatches between steps are caught."""

    def test_model_type_mismatch(self, pipeline) -> None:
        """Test error when Pydantic model types don't match."""

        @pipeline.step(0)
        async def returns_a(ctx: Context) -> ResultA:
            return ResultA(value="test")

        @pipeline.step(1)
        async def expects_c(result: ResultC) -> Combined:
            return Combined(a=result.value, b="")

        class ModelMismatchPipeline(pipeline.Base):
            pass

        ModelMismatchPipeline.returns_a = returns_a
        ModelMismatchPipeline.expects_c = expects_c

        with pytest.raises(TypeError):
            pipeline._validate_type_flow()

    def test_unrelated_model_rejected(self, pipeline) -> None:
        """Test that unrelated model types are rejected (no inheritance)."""

        @pipeline.step(0)
        async def returns_a(ctx: Context) -> ResultA:
            return ResultA(value="hello")

        @pipeline.step(1)
        async def expects_c(result: ResultC) -> Combined:
            return Combined(a=result.value, b="")

        class UnrelatedPipeline(pipeline.Base):
            pass

        UnrelatedPipeline.returns_a = returns_a
        UnrelatedPipeline.expects_c = expects_c

        with pytest.raises(TypeError):
            pipeline._validate_type_flow()


# =============================================================================
# Invalid Flows - Final Step Returns Tuple
# =============================================================================


class TestInvalidFinalStepTuple:
    """Test that final step returning tuple is rejected."""

    def test_final_step_tuple_rejected(self, pipeline) -> None:
        """Test error when final step returns a tuple (can't serialize)."""

        @pipeline.step(0)
        async def first(ctx: Context) -> ResultA:
            return ResultA(value=ctx.value)

        @pipeline.step(1)
        async def final_returns_tuple(x: ResultA) -> tuple[ResultA, ResultC]:
            return (x, ResultC(value="result"))

        class TupleFinalPipeline(pipeline.Base):
            pass

        TupleFinalPipeline.first = first
        TupleFinalPipeline.final_returns_tuple = final_returns_tuple

        with pytest.raises(TypeError):
            pipeline._validate_type_flow()


# =============================================================================
# Edge Cases
# =============================================================================


class TestInvalidNoSteps:
    """Test that pipelines with no steps are rejected."""

    def test_no_steps_rejected(self, pipeline) -> None:
        """Test error when pipeline has no steps defined."""
        with pytest.raises(RuntimeError):

            class EmptyPipeline(pipeline.Base):
                pass


class TestInvalidPrimitiveTypes:
    """Test that primitive (non-BaseModel) types are rejected."""

    def test_primitive_return_rejected(self, pipeline) -> None:
        """Test error when step returns a primitive type."""

        @pipeline.step(0)
        async def returns_int(ctx: Context) -> int:
            return 1

        @pipeline.step(1)
        async def expects_int(x: int) -> ResultA:
            return ResultA(value=str(x))

        class PrimitivePipeline(pipeline.Base):
            pass

        PrimitivePipeline.returns_int = returns_int
        PrimitivePipeline.expects_int = expects_int

        with pytest.raises(TypeError):
            pipeline._validate_type_flow()

    def test_primitive_param_rejected(self, pipeline) -> None:
        """Test error when step has a primitive parameter type."""

        @pipeline.step(0)
        async def returns_a(ctx: Context) -> ResultA:
            return ResultA(value="test")

        @pipeline.step(1)
        async def expects_str(x: str) -> ResultC:
            return ResultC(value=x)

        class PrimitivePipeline(pipeline.Base):
            pass

        PrimitivePipeline.returns_a = returns_a
        PrimitivePipeline.expects_str = expects_str

        with pytest.raises(TypeError):
            pipeline._validate_type_flow()

    def test_primitive_tuple_element_rejected(self, pipeline) -> None:
        """Test error when tuple contains primitive types."""

        @pipeline.step(0)
        async def returns_mixed(ctx: Context) -> tuple[ResultA, int]:
            return (ResultA(value="a"), 1)

        @pipeline.step(1)
        async def expects_mixed(a: ResultA, b: int) -> Combined:
            return Combined(a=a.value, b=str(b))

        class MixedPipeline(pipeline.Base):
            pass

        MixedPipeline.returns_mixed = returns_mixed
        MixedPipeline.expects_mixed = expects_mixed

        with pytest.raises(TypeError):
            pipeline._validate_type_flow()
