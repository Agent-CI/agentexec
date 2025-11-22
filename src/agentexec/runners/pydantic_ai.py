import logging
import uuid
from typing import Any, Callable

from pydantic_ai import Agent
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.messages import (
    Message,
    ModelRequest,
    ModelResponse,
    UserPrompt,
)
from pydantic_ai.result import RunResult, StreamedRunResult
from pydantic_ai.tools import Tool
from pydantic_ai.usage import UsageLimits

from agentexec.runners.base import BaseAgentRunner, _RunnerTools


logger = logging.getLogger(__name__)


def _extract_messages(e: UsageLimitExceeded) -> list[Message]:
    """
    Extract the full conversation message history from a `UsageLimitExceeded` exception.

    Args:
        e: The UsageLimitExceeded exception instance
    Returns:
        List of Message objects representing the full conversation history
    """
    if not hasattr(e, "messages") or not e.messages:
        logger.warning("No messages available in UsageLimitExceeded exception")
        return []

    return list(e.messages)


class _PydanticAIRunnerTools(_RunnerTools):
    """Pydantic AI-specific tools wrapper that creates Tool instances."""

    @property
    def report_status(self) -> Tool:
        """Get the status update tool wrapped as a Pydantic AI Tool."""
        # Get the base report_activity function
        base_func = super().report_status

        # Create a Tool instance for Pydantic AI
        return Tool(
            function=base_func,
            name="report_activity",
            description=(
                "Report progress and status updates. "
                "Use this tool to report your progress as you work through the task."
            ),
        )


class PydanticAIRunner(BaseAgentRunner):
    """Runner for Pydantic AI agents with automatic activity tracking.

    This runner wraps Pydantic AI agents and provides:
    - Automatic agent_id generation
    - Activity lifecycle management (QUEUED -> RUNNING -> COMPLETE/ERROR)
    - Request limit recovery with configurable wrap-up prompts
    - Status update tool with agent_id pre-baked

    Example:
        runner = agentexec.PydanticAIRunner(
            agent_id=agent_id,
            max_turns_recovery=True,
            wrap_up_prompt="Please summarize your findings.",
            report_status_prompt="Use report_activity(message, percentage) to report progress.",
        )

        agent = Agent(
            'anthropic:claude-sonnet-4-0',
            system_prompt=f"Research companies. {runner.prompts.report_status}",
            tools=[runner.tools.report_status],
        )

        result = await runner.run(
            agent=agent,
            user_prompt="Research Acme Corp",
            max_turns=15,
        )
    """

    def __init__(
        self,
        agent_id: uuid.UUID,
        *,
        max_turns_recovery: bool = False,
        wrap_up_prompt: str | None = None,
        recovery_turns: int = 5,
        report_status_prompt: str | None = None,
    ) -> None:
        """Initialize the Pydantic AI runner.

        Args:
            agent_id: UUID for tracking this agent's activity.
            max_turns_recovery: Enable automatic recovery when request limit exceeded.
            wrap_up_prompt: Prompt to use for recovery run.
            recovery_turns: Number of turns allowed for recovery.
            report_status_prompt: Instruction snippet about using the status tool.
        """
        super().__init__(
            agent_id,
            max_turns_recovery=max_turns_recovery,
            recovery_turns=recovery_turns,
            wrap_up_prompt=wrap_up_prompt,
            report_status_prompt=report_status_prompt,
        )
        # Override with Pydantic AI-specific tools
        self.tools = _PydanticAIRunnerTools(self.agent_id)

    async def run(
        self,
        agent: Agent[Any, Any],
        user_prompt: str | list[Message],
        max_turns: int = 10,
        deps: Any | None = None,
        message_history: list[Message] | None = None,
        model_settings: dict[str, Any] | None = None,
    ) -> RunResult[Any]:
        """Run the agent with automatic activity tracking.

        Args:
            agent: Pydantic AI Agent instance.
            user_prompt: User input/prompt for the agent, or list of messages.
            max_turns: Maximum number of agent iterations (maps to request_limit).
            deps: Optional dependencies to pass to the agent.
            message_history: Optional message history to continue from.
            model_settings: Optional model settings to pass to the agent.

        Returns:
            RunResult from the agent execution.
        """
        try:
            result = await agent.run(
                user_prompt=user_prompt,
                message_history=message_history,
                deps=deps,
                usage_limits=UsageLimits(request_limit=max_turns),
                model_settings=model_settings,
            )
        except UsageLimitExceeded as e:
            if not self.max_turns_recovery:
                raise

            logger.info("Request limit exceeded, attempting recovery")

            # Extract the conversation history
            messages = _extract_messages(e)

            # Append wrap-up prompt
            messages.append(UserPrompt(content=self.prompts.wrap_up, timestamp=None))

            # Retry with recovery turns limit
            result = await agent.run(
                user_prompt="",  # Empty prompt since we're continuing with history
                message_history=messages,
                deps=deps,
                usage_limits=UsageLimits(request_limit=self.recovery_turns),
                model_settings=model_settings,
            )
        except Exception:
            raise

        return result

    async def run_streamed(
        self,
        agent: Agent[Any, Any],
        user_prompt: str | list[Message],
        max_turns: int = 10,
        deps: Any | None = None,
        message_history: list[Message] | None = None,
        model_settings: dict[str, Any] | None = None,
    ) -> StreamedRunResult[Any]:
        """Run the agent in streaming mode with automatic activity tracking.

        The returned streaming result can be used with async context manager pattern.
        Activity tracking happens automatically.

        Args:
            agent: Pydantic AI Agent instance.
            user_prompt: User input/prompt for the agent, or list of messages.
            max_turns: Maximum number of agent iterations (maps to request_limit).
            deps: Optional dependencies to pass to the agent.
            message_history: Optional message history to continue from.
            model_settings: Optional model settings to pass to the agent.

        Returns:
            StreamedRunResult from the agent execution.

        Example:
            async with await runner.run_streamed(agent, "Research XYZ") as result:
                async for message in result.stream_text():
                    print(message)
        """
        try:
            result = await agent.run_stream(
                user_prompt=user_prompt,
                message_history=message_history,
                deps=deps,
                usage_limits=UsageLimits(request_limit=max_turns),
                model_settings=model_settings,
            )
        except UsageLimitExceeded as e:
            if not self.max_turns_recovery:
                raise

            logger.info("Request limit exceeded during streaming, attempting recovery")

            # Extract the conversation history
            messages = _extract_messages(e)

            # Append wrap-up prompt
            messages.append(UserPrompt(content=self.prompts.wrap_up, timestamp=None))

            # Retry with recovery turns limit
            result = await agent.run_stream(
                user_prompt="",  # Empty prompt since we're continuing with history
                message_history=messages,
                deps=deps,
                usage_limits=UsageLimits(request_limit=self.recovery_turns),
                model_settings=model_settings,
            )
        except Exception:
            raise

        return result
