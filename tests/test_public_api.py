"""Test that the public API is properly exposed."""

import uuid

import pytest
from pydantic import BaseModel
from sqlalchemy import create_engine

import agentexec as ax


class SampleContext(BaseModel):
    """Sample context for public API tests."""

    param: str


@pytest.fixture
def pool():
    """Create a WorkerPool for testing."""
    engine = create_engine("sqlite:///:memory:")
    return ax.WorkerPool(engine=engine)


def test_main_imports() -> None:
    """Test that main package imports work."""
    from agentexec import CONF, Task, WorkerPool, enqueue

    assert WorkerPool is not None
    assert CONF is not None
    assert Task is not None
    assert enqueue is not None


def test_openai_runner_imports() -> None:
    """Test that OpenAI runner imports work."""
    pytest.importorskip("agents")
    from agentexec.runners import OpenAIRunner

    assert OpenAIRunner is not None


def test_pydantic_ai_runner_imports() -> None:
    """Test that Pydantic AI runner imports work."""
    pytest.importorskip("pydantic_ai")
    from agentexec.runners import PydanticAIRunner

    assert PydanticAIRunner is not None


def test_worker_pool_initialization() -> None:
    """Test that WorkerPool can be initialized."""
    engine = create_engine("sqlite:///:memory:")
    pool = ax.WorkerPool(engine=engine)

    assert pool is not None
    assert hasattr(pool, "start")
    assert hasattr(pool, "shutdown")


def test_openai_runner_initialization() -> None:
    """Test that OpenAIRunner can be initialized."""
    pytest.importorskip("agents")
    from agentexec.runners import OpenAIRunner

    runner = OpenAIRunner(
        agent_id=uuid.uuid4(),
        max_turns_recovery=True,
        wrap_up_prompt="Please summarize your findings.",
        report_status_prompt="Use report_status to report progress.",
    )

    assert runner is not None
    assert runner.max_turns_recovery is True
    assert runner.prompts.report_status == "Use report_status to report progress."
    assert hasattr(runner, "tools")
    assert hasattr(runner, "run")


def test_pydantic_ai_runner_initialization() -> None:
    """Test that PydanticAIRunner can be initialized."""
    pytest.importorskip("pydantic_ai")
    from agentexec.runners import PydanticAIRunner

    runner = PydanticAIRunner(
        agent_id=uuid.uuid4(),
        max_turns_recovery=True,
        wrap_up_prompt="Please summarize your findings.",
        report_status_prompt="Use report_status to report progress.",
    )

    assert runner is not None
    assert runner.max_turns_recovery is True
    assert runner.prompts.report_status == "Use report_status to report progress."
    assert hasattr(runner, "tools")
    assert hasattr(runner, "run")
    assert hasattr(runner, "run_streamed")


def test_config_access() -> None:
    """Test that configuration can be accessed."""
    assert ax.CONF.redis_url is None  # Default is None, must be configured
    assert ax.CONF.num_workers > 0
    assert ax.CONF.queue_name is not None


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
    async def test_handler(agent_id: uuid.UUID, context: SampleContext) -> str:
        return f"Processed: {context.param}"

    # Verify task definition was registered with pool
    assert "test_task" in pool._context.tasks
    task_def = pool._context.tasks["test_task"]
    assert task_def.handler == test_handler
    assert task_def.context_class == SampleContext
