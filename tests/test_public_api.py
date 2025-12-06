"""Test that the public API is properly exposed."""

import uuid

import pytest
from pydantic import BaseModel
from sqlalchemy import create_engine

import agentexec as ax


class SampleContext(BaseModel):
    """Sample context for public API tests."""

    param: str


class SampleResult(BaseModel):
    """Sample result for public API tests."""

    message: str


@pytest.fixture
def pool():
    """Create a Pool for testing."""
    engine = create_engine("sqlite:///:memory:")
    return ax.Pool(engine=engine)


def test_main_imports() -> None:
    """Test that main package imports work."""
    from agentexec import CONF, Task, Pool, enqueue

    assert Pool is not None
    assert CONF is not None
    assert Task is not None
    assert enqueue is not None


def test_runner_imports() -> None:
    """Test that runner imports work."""
    pytest.importorskip("agents")
    from agentexec import OpenAIRunner

    assert OpenAIRunner is not None


def test_runner_initialization() -> None:
    """Test that OpenAIRunner can be initialized."""
    pytest.importorskip("agents")
    from agentexec import OpenAIRunner

    runner = OpenAIRunner(
        agent_id=uuid.uuid4(),
        max_turns_recovery=True,
        wrap_up_prompt="Please summarize your findings.",
    )

    assert runner is not None
    assert runner.max_turns_recovery is True
    assert runner.prompts.wrap_up == "Please summarize your findings."
    assert hasattr(runner, "tools")
    assert hasattr(runner, "run")


def test_config_environment_variables() -> None:
    """Test that config respects environment variables."""
    import os

    from agentexec.config import Config

    # Set environment variable
    os.environ["AGENTEXEC_NUM_WORKERS"] = "8"

    # Create new config instance
    test_config = Config()

    assert test_config.num_workers == 8

    # Cleanup
    del os.environ["AGENTEXEC_NUM_WORKERS"]


def test_task_decorator_interface(pool) -> None:
    """Test that @pool.task() decorator works."""

    @pool.task("test_task")
    async def test_handler(agent_id: uuid.UUID, context: SampleContext) -> SampleResult:
        return SampleResult(message=f"Processed: {context.param}")

    # Decorator returns the raw handler function
    assert callable(test_handler)

    # Verify task definition was registered with pool
    assert "test_task" in pool._context.tasks
    task_def = pool._context.tasks["test_task"]
    assert task_def.context_type == SampleContext
