"""Tests for PydanticAIRunner."""

import uuid
from unittest.mock import AsyncMock, Mock, patch

import pytest

# Skip all tests if pydantic-ai is not installed
pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent, AgentRunResult
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.messages import ModelRequest, ModelResponse, UserPromptPart
from pydantic_ai.result import StreamedRunResult
from pydantic_ai.tools import Tool

from agentexec.runners.pydantic_ai import PydanticAIRunner, _PydanticAIRunnerTools


class TestPydanticAIRunnerInitialization:
    """Tests for PydanticAIRunner initialization."""

    def test_basic_initialization(self) -> None:
        """Test that PydanticAIRunner can be initialized with basic parameters."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(agent_id=agent_id)

        assert runner is not None
        assert runner.agent_id == agent_id
        assert runner.max_turns_recovery is False
        assert runner.recovery_turns == 5
        assert hasattr(runner, "tools")
        assert hasattr(runner, "prompts")
        assert hasattr(runner, "run")
        assert hasattr(runner, "run_streamed")

    def test_initialization_with_recovery(self) -> None:
        """Test initialization with max_turns_recovery enabled."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(
            agent_id=agent_id,
            max_turns_recovery=True,
            recovery_turns=3,
        )

        assert runner.max_turns_recovery is True
        assert runner.recovery_turns == 3

    def test_initialization_with_custom_prompts(self) -> None:
        """Test initialization with custom prompts."""
        agent_id = uuid.uuid4()
        custom_status_prompt = "Custom status reporting instructions"
        custom_wrap_up_prompt = "Custom wrap up instructions"

        runner = PydanticAIRunner(
            agent_id=agent_id,
            report_status_prompt=custom_status_prompt,
            wrap_up_prompt=custom_wrap_up_prompt,
        )

        assert runner.prompts.report_status == custom_status_prompt
        assert runner.prompts.wrap_up == custom_wrap_up_prompt

    def test_tools_namespace_exists(self) -> None:
        """Test that tools namespace is properly initialized."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(agent_id=agent_id)

        assert isinstance(runner.tools, _PydanticAIRunnerTools)
        assert hasattr(runner.tools, "report_status")

    def test_report_status_tool_is_pydantic_tool(self) -> None:
        """Test that report_status is a Pydantic AI Tool."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(agent_id=agent_id)

        tool = runner.tools.report_status
        assert isinstance(tool, Tool)
        assert tool.name == "report_activity"


class TestPydanticAIRunnerExecution:
    """Tests for PydanticAIRunner run methods."""

    @pytest.mark.asyncio
    async def test_basic_run(self) -> None:
        """Test basic agent execution without recovery."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(agent_id=agent_id)

        # Create a mock agent
        mock_agent = Mock(spec=Agent)
        mock_result = Mock(spec=AgentRunResult)
        mock_result.data = "Test result"

        # Mock the agent.run method
        mock_agent.run = AsyncMock(return_value=mock_result)

        # Run the agent
        result = await runner.run(
            agent=mock_agent,
            user_prompt="Test prompt",
            max_turns=10,
        )

        assert result == mock_result
        mock_agent.run.assert_called_once()
        call_kwargs = mock_agent.run.call_args.kwargs

        assert call_kwargs["user_prompt"] == "Test prompt"
        assert call_kwargs["usage_limits"].request_limit == 10

    @pytest.mark.asyncio
    async def test_run_with_deps(self) -> None:
        """Test agent execution with dependencies."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(agent_id=agent_id)

        mock_agent = Mock(spec=Agent)
        mock_result = Mock(spec=AgentRunResult)
        mock_agent.run = AsyncMock(return_value=mock_result)

        deps = {"key": "value"}
        await runner.run(
            agent=mock_agent,
            user_prompt="Test prompt",
            deps=deps,
        )

        call_kwargs = mock_agent.run.call_args.kwargs
        assert call_kwargs["deps"] == deps

    @pytest.mark.asyncio
    async def test_run_with_message_history(self) -> None:
        """Test agent execution with message history."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(agent_id=agent_id)

        mock_agent = Mock(spec=Agent)
        mock_result = Mock(spec=AgentRunResult)
        mock_agent.run = AsyncMock(return_value=mock_result)

        message_history = [
            ModelRequest(parts=[UserPromptPart(content="Previous message")]),
        ]

        await runner.run(
            agent=mock_agent,
            user_prompt="Test prompt",
            message_history=message_history,
        )

        call_kwargs = mock_agent.run.call_args.kwargs
        assert call_kwargs["message_history"] == message_history

    @pytest.mark.asyncio
    async def test_run_with_model_settings(self) -> None:
        """Test agent execution with custom model settings."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(agent_id=agent_id)

        mock_agent = Mock(spec=Agent)
        mock_result = Mock(spec=AgentRunResult)
        mock_agent.run = AsyncMock(return_value=mock_result)

        model_settings = {"temperature": 0.7, "max_tokens": 1000}

        await runner.run(
            agent=mock_agent,
            user_prompt="Test prompt",
            model_settings=model_settings,
        )

        call_kwargs = mock_agent.run.call_args.kwargs
        assert call_kwargs["model_settings"] == model_settings


class TestPydanticAIRunnerRecovery:
    """Tests for max turns recovery functionality."""

    @pytest.mark.asyncio
    async def test_recovery_disabled_raises_exception(self) -> None:
        """Test that exception is raised when recovery is disabled."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(
            agent_id=agent_id,
            max_turns_recovery=False,
        )

        mock_agent = Mock(spec=Agent)
        mock_exception = UsageLimitExceeded("Request limit exceeded")
        mock_exception.message_history = []
        mock_agent.run = AsyncMock(side_effect=mock_exception)

        with pytest.raises(UsageLimitExceeded):
            await runner.run(
                agent=mock_agent,
                user_prompt="Test prompt",
                max_turns=5,
            )

    @pytest.mark.asyncio
    async def test_recovery_enabled_retries(self) -> None:
        """Test that recovery mechanism retries with wrap-up prompt."""
        agent_id = uuid.uuid4()
        wrap_up_prompt = "Please summarize"
        runner = PydanticAIRunner(
            agent_id=agent_id,
            max_turns_recovery=True,
            wrap_up_prompt=wrap_up_prompt,
            recovery_turns=3,
        )

        mock_agent = Mock(spec=Agent)

        # First call raises UsageLimitExceeded
        mock_exception = UsageLimitExceeded("Request limit exceeded")
        mock_exception.message_history = [
            ModelRequest(parts=[UserPromptPart(content="Original prompt")]),
            Mock(spec=ModelResponse),  # Mock a response
        ]

        # Second call (recovery) succeeds
        mock_recovery_result = Mock(spec=AgentRunResult)
        mock_recovery_result.data = "Recovery result"

        mock_agent.run = AsyncMock(side_effect=[mock_exception, mock_recovery_result])

        result = await runner.run(
            agent=mock_agent,
            user_prompt="Test prompt",
            max_turns=5,
        )

        assert result == mock_recovery_result
        assert mock_agent.run.call_count == 2

        # Check second call (recovery)
        second_call_kwargs = mock_agent.run.call_args_list[1].kwargs
        assert second_call_kwargs["user_prompt"] is None
        assert second_call_kwargs["usage_limits"].request_limit == 3

        # Verify wrap-up prompt was added to message history
        recovery_messages = second_call_kwargs["message_history"]
        assert len(recovery_messages) > 0
        # Last message should be the wrap-up prompt
        last_message = recovery_messages[-1]
        assert isinstance(last_message, ModelRequest)
        assert last_message.parts[0].content == wrap_up_prompt

    @pytest.mark.asyncio
    async def test_recovery_preserves_message_history(self) -> None:
        """Test that recovery preserves conversation history."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(
            agent_id=agent_id,
            max_turns_recovery=True,
            recovery_turns=3,
        )

        mock_agent = Mock(spec=Agent)

        # Create mock message history
        original_messages = [
            ModelRequest(parts=[UserPromptPart(content="Message 1")]),
            Mock(spec=ModelResponse),
            ModelRequest(parts=[UserPromptPart(content="Message 2")]),
            Mock(spec=ModelResponse),
        ]

        mock_exception = UsageLimitExceeded("Request limit exceeded")
        mock_exception.message_history = original_messages

        mock_recovery_result = Mock(spec=AgentRunResult)
        mock_agent.run = AsyncMock(side_effect=[mock_exception, mock_recovery_result])

        await runner.run(
            agent=mock_agent,
            user_prompt="Test prompt",
            max_turns=5,
        )

        # Check that recovery call received the message history
        recovery_call_kwargs = mock_agent.run.call_args_list[1].kwargs
        recovery_messages = recovery_call_kwargs["message_history"]

        # Should have original messages plus wrap-up prompt
        assert len(recovery_messages) == len(original_messages) + 1

    @pytest.mark.asyncio
    async def test_other_exceptions_not_caught(self) -> None:
        """Test that non-UsageLimitExceeded exceptions are propagated."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(
            agent_id=agent_id,
            max_turns_recovery=True,
        )

        mock_agent = Mock(spec=Agent)
        mock_agent.run = AsyncMock(side_effect=ValueError("Different error"))

        with pytest.raises(ValueError, match="Different error"):
            await runner.run(
                agent=mock_agent,
                user_prompt="Test prompt",
            )


class TestPydanticAIRunnerStreaming:
    """Tests for streaming functionality."""

    @pytest.mark.asyncio
    async def test_basic_streaming(self) -> None:
        """Test basic streaming execution."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(agent_id=agent_id)

        mock_agent = Mock(spec=Agent)
        mock_result = Mock(spec=StreamedRunResult)
        mock_agent.run_stream = AsyncMock(return_value=mock_result)

        result = await runner.run_streamed(
            agent=mock_agent,
            user_prompt="Test prompt",
            max_turns=10,
        )

        assert result == mock_result
        mock_agent.run_stream.assert_called_once()
        call_kwargs = mock_agent.run_stream.call_args.kwargs
        assert call_kwargs["user_prompt"] == "Test prompt"
        assert call_kwargs["usage_limits"].request_limit == 10

    @pytest.mark.asyncio
    async def test_streaming_with_recovery(self) -> None:
        """Test that streaming works with recovery mechanism."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(
            agent_id=agent_id,
            max_turns_recovery=True,
            recovery_turns=3,
        )

        mock_agent = Mock(spec=Agent)

        mock_exception = UsageLimitExceeded("Request limit exceeded")
        mock_exception.message_history = [
            ModelRequest(parts=[UserPromptPart(content="Original")]),
        ]

        mock_recovery_result = Mock(spec=StreamedRunResult)
        mock_agent.run_stream = AsyncMock(
            side_effect=[mock_exception, mock_recovery_result]
        )

        result = await runner.run_streamed(
            agent=mock_agent,
            user_prompt="Test prompt",
            max_turns=5,
        )

        assert result == mock_recovery_result
        assert mock_agent.run_stream.call_count == 2


class TestPydanticAIRunnerTools:
    """Tests for tool integration."""

    def test_report_status_tool_bound_to_agent_id(self) -> None:
        """Test that report_status tool is bound to the correct agent_id."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(agent_id=agent_id)

        tool = runner.tools.report_status
        assert isinstance(tool, Tool)
        # The underlying function should be bound to this agent_id
        assert tool.function is not None

    @patch("agentexec.activity.update")
    def test_report_status_tool_calls_activity_update(self, mock_update: Mock) -> None:
        """Test that report_status tool calls activity.update."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(agent_id=agent_id)

        tool = runner.tools.report_status

        # Call the underlying function
        result = tool.function(message="Working on task", percentage=50)

        assert result == "Status updated"
        mock_update.assert_called_once_with(
            agent_id=agent_id,
            message="Working on task",
            completion_percentage=50,
        )

    def test_multiple_runners_have_separate_tools(self) -> None:
        """Test that different runners have separate tool instances."""
        agent_id_1 = uuid.uuid4()
        agent_id_2 = uuid.uuid4()

        runner1 = PydanticAIRunner(agent_id=agent_id_1)
        runner2 = PydanticAIRunner(agent_id=agent_id_2)

        # Tools should be different instances
        assert runner1.tools is not runner2.tools
        assert runner1.tools.report_status is not runner2.tools.report_status


class TestPydanticAIRunnerIntegration:
    """Integration tests for PydanticAIRunner."""

    def test_runner_public_api_import(self) -> None:
        """Test that PydanticAIRunner can be imported from public API."""
        from agentexec.runners import PydanticAIRunner as ImportedRunner

        assert ImportedRunner is not None
        assert ImportedRunner == PydanticAIRunner

    def test_runner_works_with_pydantic_ai_agent(self) -> None:
        """Test that runner works with actual Pydantic AI Agent class."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(agent_id=agent_id)

        # Create a real Pydantic AI agent (not mocked) to verify compatibility
        _ = Agent("test")

        # Verify runner has the right interface for Pydantic AI agents
        assert hasattr(runner, "run")
        assert hasattr(runner, "run_streamed")

    def test_prompts_can_be_injected_into_agent(self) -> None:
        """Test that runner prompts can be used in agent system prompts."""
        agent_id = uuid.uuid4()
        runner = PydanticAIRunner(agent_id=agent_id)

        # Create an agent with runner's status prompt
        system_prompt_text = (
            f"You are a helpful assistant. {runner.prompts.report_status}"
        )
        agent = Agent(
            "test",
            system_prompt=system_prompt_text,
        )

        # Verify the prompt was included by checking the agent has system prompts
        assert len(agent._system_prompts) > 0
        # The first system prompt should be our text (or contain it)
        first_prompt = agent._system_prompts[0]
        # Handle both str and object cases
        if hasattr(first_prompt, "content"):
            assert runner.prompts.report_status in first_prompt.content
        else:
            assert runner.prompts.report_status in str(first_prompt)
