"""Test runner base classes and functionality."""

import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from agentexec.activity.models import Activity, Base, Status
from agentexec.runners.base import BaseAgentRunner, _RunnerPrompts, _RunnerTools


@pytest.fixture
def db_session():
    """Set up an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()


class TestRunnerPrompts:
    """Tests for _RunnerPrompts class."""

    def test_default_prompts(self):
        """Test default prompt values."""
        prompts = _RunnerPrompts()

        assert "report_activity" in prompts.report_status
        assert "summarize your findings" in prompts.wrap_up

    def test_custom_wrap_up_prompt(self):
        """Test custom wrap_up prompt."""
        prompts = _RunnerPrompts(wrap_up="Provide a summary.")

        assert prompts.wrap_up == "Provide a summary."

    def test_partial_custom_prompts(self):
        """Test that only specified prompts are overridden."""
        prompts = _RunnerPrompts(wrap_up="Custom wrap up")

        assert prompts.wrap_up == "Custom wrap up"
        # Default should remain for report_status
        assert "report_activity" in prompts.report_status


class TestRunnerTools:
    """Tests for _RunnerTools class."""

    def test_initialization(self):
        """Test _RunnerTools initialization with agent_id."""
        agent_id = uuid.uuid4()
        tools = _RunnerTools(agent_id)

        assert tools._agent_id == agent_id

    def test_report_status_returns_function(self):
        """Test that report_status property returns a callable."""
        agent_id = uuid.uuid4()
        tools = _RunnerTools(agent_id)

        report_fn = tools.report_status

        assert callable(report_fn)

    def test_report_status_function_signature(self):
        """Test report_status function has correct signature."""
        agent_id = uuid.uuid4()
        tools = _RunnerTools(agent_id)

        report_fn = tools.report_status

        # Check the function has the expected parameters
        import inspect

        sig = inspect.signature(report_fn)
        params = list(sig.parameters.keys())

        assert "message" in params
        assert "percentage" in params

    def test_report_status_function_docstring(self):
        """Test report_status function has a docstring."""
        agent_id = uuid.uuid4()
        tools = _RunnerTools(agent_id)

        report_fn = tools.report_status

        assert report_fn.__doc__ is not None
        assert "progress" in report_fn.__doc__.lower()

    def test_report_status_updates_activity(self, db_session, monkeypatch):
        """Test that report_status function calls activity.update."""
        agent_id = uuid.uuid4()

        # Track calls to activity.update
        update_calls = []

        def mock_update(*args, **kwargs):
            update_calls.append(kwargs)
            return True

        monkeypatch.setattr("agentexec.runners.base.activity.update", mock_update)

        tools = _RunnerTools(agent_id)
        report_fn = tools.report_status

        result = report_fn("Working on task", 50)

        assert result == "Status updated"
        assert len(update_calls) == 1
        assert update_calls[0]["agent_id"] == agent_id
        assert update_calls[0]["message"] == "Working on task"
        assert update_calls[0]["percentage"] == 50


class TestBaseAgentRunner:
    """Tests for BaseAgentRunner class."""

    def test_initialization_defaults(self):
        """Test BaseAgentRunner initializes with default values."""
        agent_id = uuid.uuid4()
        runner = BaseAgentRunner(agent_id)

        assert runner.agent_id == agent_id
        assert runner.max_turns_recovery is True
        assert runner.recovery_turns == 5

    def test_initialization_custom_values(self):
        """Test BaseAgentRunner with custom values."""
        agent_id = uuid.uuid4()
        runner = BaseAgentRunner(
            agent_id,
            max_turns_recovery=False,
            recovery_turns=10,
            wrap_up_prompt="Custom wrap up",
        )

        assert runner.max_turns_recovery is False
        assert runner.recovery_turns == 10
        assert runner.prompts.wrap_up == "Custom wrap up"

    def test_prompts_namespace(self):
        """Test that runner has prompts namespace."""
        agent_id = uuid.uuid4()
        runner = BaseAgentRunner(agent_id)

        assert hasattr(runner, "prompts")
        assert isinstance(runner.prompts, _RunnerPrompts)

    def test_tools_namespace(self):
        """Test that runner has tools namespace."""
        agent_id = uuid.uuid4()
        runner = BaseAgentRunner(agent_id)

        assert hasattr(runner, "tools")
        assert isinstance(runner.tools, _RunnerTools)

    def test_tools_bound_to_agent_id(self):
        """Test that tools are bound to the correct agent_id."""
        agent_id = uuid.uuid4()
        runner = BaseAgentRunner(agent_id)

        assert runner.tools._agent_id == agent_id


class TestOpenAIRunner:
    """Tests for OpenAIRunner (if agents package available)."""

    @pytest.fixture
    def skip_if_no_agents(self):
        """Skip test if agents package is not available."""
        pytest.importorskip("agents")

    def test_openai_runner_initialization(self, skip_if_no_agents):
        """Test OpenAIRunner can be initialized."""
        from agentexec.runners import OpenAIRunner

        agent_id = uuid.uuid4()
        runner = OpenAIRunner(
            agent_id,
            max_turns_recovery=True,
            recovery_turns=3,
        )

        assert runner.agent_id == agent_id
        assert runner.max_turns_recovery is True
        assert runner.recovery_turns == 3

    def test_openai_runner_tools_are_decorated(self, skip_if_no_agents):
        """Test OpenAIRunner tools are wrapped with @function_tool."""
        from agentexec.runners import OpenAIRunner

        agent_id = uuid.uuid4()
        runner = OpenAIRunner(agent_id)

        report_fn = runner.tools.report_status

        # The function should be wrapped by @function_tool
        # function_tool creates a FunctionTool object with a name attribute
        assert hasattr(report_fn, "name")
        assert report_fn.name == "report_activity"

    def test_openai_runner_default_max_turns_recovery(self, skip_if_no_agents):
        """Test OpenAIRunner default max_turns_recovery is False."""
        from agentexec.runners import OpenAIRunner

        agent_id = uuid.uuid4()
        runner = OpenAIRunner(agent_id)

        # OpenAI runner defaults to False for max_turns_recovery
        assert runner.max_turns_recovery is False

    async def test_openai_runner_run_success(self, skip_if_no_agents, monkeypatch):
        """Test OpenAIRunner.run executes successfully."""
        from unittest.mock import AsyncMock, MagicMock
        from agentexec.runners import OpenAIRunner

        agent_id = uuid.uuid4()
        runner = OpenAIRunner(agent_id)

        # Mock the Agent and Runner.run
        mock_agent = MagicMock()
        mock_result = MagicMock()
        mock_result.final_output = "Test result"

        async def mock_run(*args, **kwargs):
            return mock_result

        monkeypatch.setattr("agentexec.runners.openai.Runner.run", mock_run)

        result = await runner.run(
            agent=mock_agent,
            input="Test input",
            max_turns=10,
        )

        assert result is mock_result

    async def test_openai_runner_run_with_max_turns_exceeded_no_recovery(
        self, skip_if_no_agents, monkeypatch
    ):
        """Test OpenAIRunner.run raises MaxTurnsExceeded when recovery disabled."""
        from unittest.mock import MagicMock
        from agents import MaxTurnsExceeded
        from agentexec.runners import OpenAIRunner

        agent_id = uuid.uuid4()
        runner = OpenAIRunner(agent_id, max_turns_recovery=False)

        mock_agent = MagicMock()
        exc = MaxTurnsExceeded("Max turns exceeded")

        async def mock_run(*args, **kwargs):
            raise exc

        monkeypatch.setattr("agentexec.runners.openai.Runner.run", mock_run)

        with pytest.raises(MaxTurnsExceeded):
            await runner.run(agent=mock_agent, input="Test input")

    async def test_openai_runner_run_with_max_turns_recovery(
        self, skip_if_no_agents, monkeypatch
    ):
        """Test OpenAIRunner.run attempts recovery when max turns exceeded."""
        from unittest.mock import AsyncMock, MagicMock
        from agents import MaxTurnsExceeded
        from agentexec.runners import OpenAIRunner

        agent_id = uuid.uuid4()
        runner = OpenAIRunner(
            agent_id,
            max_turns_recovery=True,
            recovery_turns=3,
            wrap_up_prompt="Please wrap up.",
        )

        mock_agent = MagicMock()
        mock_run_data = MagicMock()
        mock_run_data.input = "Original input"
        mock_run_data.new_items = []

        exc = MaxTurnsExceeded("Max turns exceeded")
        exc.run_data = mock_run_data

        mock_recovery_result = MagicMock()
        mock_recovery_result.final_output = "Recovery result"

        call_count = 0

        async def mock_run(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise exc
            return mock_recovery_result

        monkeypatch.setattr("agentexec.runners.openai.Runner.run", mock_run)

        result = await runner.run(agent=mock_agent, input="Test input")

        assert result is mock_recovery_result
        assert call_count == 2


class TestExtractInput:
    """Tests for _extract_input helper function."""

    @pytest.fixture
    def skip_if_no_agents(self):
        """Skip test if agents package is not available."""
        pytest.importorskip("agents")

    def test_extract_input_with_string_input(self, skip_if_no_agents):
        """Test _extract_input with string input."""
        from unittest.mock import MagicMock
        from agents import MaxTurnsExceeded
        from agentexec.runners.openai import _extract_input

        mock_run_data = MagicMock()
        mock_run_data.input = "Hello"
        mock_run_data.new_items = []

        exc = MaxTurnsExceeded("Max turns")
        exc.run_data = mock_run_data

        result = _extract_input(exc)

        assert result == [{"role": "user", "content": "Hello"}]

    def test_extract_input_with_list_input(self, skip_if_no_agents):
        """Test _extract_input with list input."""
        from unittest.mock import MagicMock
        from agents import MaxTurnsExceeded
        from agentexec.runners.openai import _extract_input

        input_list = [{"role": "user", "content": "Message 1"}]

        mock_run_data = MagicMock()
        mock_run_data.input = input_list
        mock_run_data.new_items = []

        exc = MaxTurnsExceeded("Max turns")
        exc.run_data = mock_run_data

        result = _extract_input(exc)

        assert result == input_list

    def test_extract_input_with_new_items(self, skip_if_no_agents):
        """Test _extract_input includes new items."""
        from unittest.mock import MagicMock
        from agents import MaxTurnsExceeded
        from agentexec.runners.openai import _extract_input

        mock_item = MagicMock()
        mock_item.to_input_item.return_value = {"role": "assistant", "content": "Response"}

        mock_run_data = MagicMock()
        mock_run_data.input = "Hello"
        mock_run_data.new_items = [mock_item]

        exc = MaxTurnsExceeded("Max turns")
        exc.run_data = mock_run_data

        result = _extract_input(exc)

        assert len(result) == 2
        assert result[0] == {"role": "user", "content": "Hello"}
        assert result[1] == {"role": "assistant", "content": "Response"}

    def test_extract_input_raises_without_run_data(self, skip_if_no_agents):
        """Test _extract_input raises when no run_data available."""
        from agents import MaxTurnsExceeded
        from agentexec.runners.openai import _extract_input

        exc = MaxTurnsExceeded("Max turns")
        exc.run_data = None

        with pytest.raises(Exception):
            _extract_input(exc)


class TestOpenAIRunnerStreamed:
    """Tests for OpenAIRunner.run_streamed method."""

    @pytest.fixture
    def skip_if_no_agents(self):
        """Skip test if agents package is not available."""
        pytest.importorskip("agents")

    async def test_openai_runner_run_streamed_success(self, skip_if_no_agents, monkeypatch):
        """Test OpenAIRunner.run_streamed executes successfully."""
        from unittest.mock import AsyncMock, MagicMock
        from agentexec.runners import OpenAIRunner

        agent_id = uuid.uuid4()
        runner = OpenAIRunner(agent_id)

        mock_agent = MagicMock()
        mock_result = MagicMock()

        # Mock stream_events to yield events
        async def mock_stream_events():
            yield {"type": "text", "content": "Test"}
            yield {"type": "done"}

        mock_result.stream_events = mock_stream_events

        def mock_run_streamed(*args, **kwargs):
            return mock_result

        monkeypatch.setattr("agentexec.runners.openai.Runner.run_streamed", mock_run_streamed)

        result = await runner.run_streamed(
            agent=mock_agent,
            input="Test input",
            max_turns=10,
        )

        assert result is mock_result

    async def test_openai_runner_run_streamed_with_forwarder(
        self, skip_if_no_agents, monkeypatch
    ):
        """Test OpenAIRunner.run_streamed with forwarder callback."""
        from unittest.mock import AsyncMock, MagicMock
        from agentexec.runners import OpenAIRunner

        agent_id = uuid.uuid4()
        runner = OpenAIRunner(agent_id)

        mock_agent = MagicMock()
        mock_result = MagicMock()
        forwarded_events = []

        async def capture_event(event):
            forwarded_events.append(event)

        async def mock_stream_events():
            yield {"type": "text", "content": "First"}
            yield {"type": "text", "content": "Second"}

        mock_result.stream_events = mock_stream_events

        def mock_run_streamed(*args, **kwargs):
            return mock_result

        monkeypatch.setattr("agentexec.runners.openai.Runner.run_streamed", mock_run_streamed)

        result = await runner.run_streamed(
            agent=mock_agent,
            input="Test input",
            forwarder=capture_event,
        )

        assert result is mock_result
        assert len(forwarded_events) == 2
        assert forwarded_events[0]["content"] == "First"
        assert forwarded_events[1]["content"] == "Second"

    async def test_openai_runner_run_streamed_max_turns_exceeded_no_recovery(
        self, skip_if_no_agents, monkeypatch
    ):
        """Test run_streamed raises MaxTurnsExceeded when recovery disabled."""
        from unittest.mock import MagicMock
        from agents import MaxTurnsExceeded
        from agentexec.runners import OpenAIRunner

        agent_id = uuid.uuid4()
        runner = OpenAIRunner(agent_id, max_turns_recovery=False)

        mock_agent = MagicMock()
        exc = MaxTurnsExceeded("Max turns exceeded")
        mock_result = MagicMock()

        async def mock_stream_events():
            raise exc
            yield  # Make this a generator

        mock_result.stream_events = mock_stream_events

        def mock_run_streamed(*args, **kwargs):
            return mock_result

        monkeypatch.setattr("agentexec.runners.openai.Runner.run_streamed", mock_run_streamed)

        with pytest.raises(MaxTurnsExceeded):
            await runner.run_streamed(agent=mock_agent, input="Test input")

    async def test_openai_runner_run_streamed_with_recovery(
        self, skip_if_no_agents, monkeypatch
    ):
        """Test run_streamed attempts recovery when max turns exceeded."""
        from unittest.mock import MagicMock
        from agents import MaxTurnsExceeded
        from agentexec.runners import OpenAIRunner

        agent_id = uuid.uuid4()
        runner = OpenAIRunner(
            agent_id,
            max_turns_recovery=True,
            recovery_turns=3,
        )

        mock_agent = MagicMock()
        mock_run_data = MagicMock()
        mock_run_data.input = "Original"
        mock_run_data.new_items = []

        exc = MaxTurnsExceeded("Max turns exceeded")
        exc.run_data = mock_run_data

        call_count = 0
        mock_results = []

        def mock_run_streamed(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            mock_result = MagicMock()

            async def stream_events():
                if call_count == 1:
                    raise exc
                yield {"type": "done"}

            mock_result.stream_events = stream_events
            mock_results.append(mock_result)
            return mock_result

        monkeypatch.setattr("agentexec.runners.openai.Runner.run_streamed", mock_run_streamed)

        result = await runner.run_streamed(agent=mock_agent, input="Test input")

        assert call_count == 2  # First call raises, second is recovery

    async def test_openai_runner_run_raises_other_exceptions(
        self, skip_if_no_agents, monkeypatch
    ):
        """Test OpenAIRunner.run re-raises non-MaxTurnsExceeded exceptions."""
        from unittest.mock import MagicMock
        from agentexec.runners import OpenAIRunner

        agent_id = uuid.uuid4()
        runner = OpenAIRunner(agent_id)

        mock_agent = MagicMock()

        async def mock_run(*args, **kwargs):
            raise ValueError("Some other error")

        monkeypatch.setattr("agentexec.runners.openai.Runner.run", mock_run)

        with pytest.raises(ValueError, match="Some other error"):
            await runner.run(agent=mock_agent, input="Test input")
