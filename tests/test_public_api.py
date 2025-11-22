"""Test that the public API is properly exposed."""

import uuid

import pytest
from sqlalchemy import create_engine


def test_main_imports() -> None:
    """Test that main package imports work."""
    from agentexec import CONF, Task, WorkerPool, enqueue

    assert WorkerPool is not None
    assert CONF is not None
    assert Task is not None
    assert enqueue is not None


def test_runner_imports() -> None:
    """Test that runner imports work."""
    pytest.importorskip("agents")
    from agentexec.runners import OpenAIRunner

    assert OpenAIRunner is not None


def test_worker_pool_initialization() -> None:
    """Test that WorkerPool can be initialized."""
    from agentexec import WorkerPool

    engine = create_engine("sqlite:///:memory:")
    pool = WorkerPool(engine=engine)
    assert pool is not None
    assert hasattr(pool, "task")
    assert hasattr(pool, "start")
    assert hasattr(pool, "shutdown")


def test_runner_initialization() -> None:
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


def test_config_access() -> None:
    """Test that configuration can be accessed."""
    from agentexec import CONF

    assert CONF.redis_url is not None
    assert CONF.num_workers > 0
    assert CONF.queue_name is not None


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


def test_task_decorator_interface() -> None:
    """Test that the task decorator interface works."""
    from agentexec import WorkerPool

    engine = create_engine("sqlite:///:memory:")
    pool = WorkerPool(engine=engine)

    # Test decorator registration
    @pool.task("test_task")
    async def test_handler(agent_id: uuid.UUID, payload: dict) -> str:
        return f"Processed: {payload.get('param')}"

    # Verify handler was registered
    assert "test_task" in pool._handlers
    assert pool._handlers["test_task"] == test_handler
