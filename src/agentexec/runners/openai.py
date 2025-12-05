import logging
import uuid
from typing import Any, Callable

from agents import Agent, MaxTurnsExceeded, Runner, function_tool
from agents.items import TResponseInputItem
from agents.result import RunResult, RunResultStreaming
from openai.types.responses.easy_input_message_param import EasyInputMessageParam

from agentexec.runners.base import BaseAgentRunner, _RunnerTools


logger = logging.getLogger(__name__)


def _extract_input(e: MaxTurnsExceeded) -> list[TResponseInputItem]:
    """
    Extract the full conversation input history from a `MaxTurnsExceeded` exception.

    Args:
        e: The MaxTurnsExceeded exception instance
    Returns:
        List of TResponseInputItem representing the full conversation history
    """
    if not e.run_data:
        logger.error("No run data available in MaxTurnsExceeded exception")
        raise

    # Reconstruct the full conversation history
    final_input: list[TResponseInputItem] = (
        list(e.run_data.input)
        if isinstance(e.run_data.input, list)
        else [EasyInputMessageParam(role="user", content=e.run_data.input)]
    )

    # Add all the conversation items that were generated
    final_input.extend([item.to_input_item() for item in e.run_data.new_items])

    return final_input


class _OpenAIRunnerTools(_RunnerTools):
    """OpenAI-specific tools wrapper that decorates with @function_tool."""

    @property
    def report_status(self) -> Any:
        """Get the status update tool wrapped with @function_tool decorator."""
        return function_tool(super().report_status)


class OpenAIRunner(BaseAgentRunner):
    """Runner for OpenAI Agents SDK with automatic activity tracking.

    This runner wraps the OpenAI Agents SDK and provides:
    - Automatic agent_id generation
    - Activity lifecycle management (QUEUED -> RUNNING -> COMPLETE/ERROR)
    - Max turns recovery with configurable wrap-up prompts
    - Status update tool with agent_id pre-baked

    Example:
        runner = agentexec.OpenAIRunner(
            max_turns_recovery=True,
            wrap_up_prompt="Please summarize your findings.",
            status_prompt="Use update_status(message, percentage) to report progress.",
        )

        agent = Agent(
            name="Research Agent",
            instructions=f"Research companies. {runner.status_prompt}",
            tools=[runner.tools.report_status],
            model="gpt-4o",
        )

        result = await runner.run(
            session=session,
            agent=agent,
            input="Research Acme Corp",
            agent_id=agent_id,  # Optional
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
        """Initialize the OpenAI runner.

        Args:
            agent_id: UUID for tracking this agent's activity.
            max_turns_recovery: Enable automatic recovery when max turns exceeded.
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
        # Override with OpenAI-specific tools
        self.tools = _OpenAIRunnerTools(self.agent_id)

    def _wrap_up_prompt(self) -> EasyInputMessageParam:
        return EasyInputMessageParam(
            role="system",
            content=self.prompts.wrap_up,
        )

    async def run(
        self,
        agent: Agent[Any],
        input: str | list[TResponseInputItem],
        max_turns: int = 10,
        context: Any | None = None,
    ) -> RunResult:
        """Run the agent with automatic activity tracking.

        Args:
            session: SQLAlchemy session for database operations.
            agent: Agent instance.
            input: User input/prompt for the agent.
            agent_id: Optional agent ID (generated if not provided).
            max_turns: Maximum number of agent iterations.
            agent_type: Optional agent type for activity tracking.
            context: Optional context for the agent run.

        Returns:
            Result from the agent execution.
        """
        # TODO match method signature of Runner.run
        try:
            result = await Runner.run(
                agent,
                input,
                max_turns=max_turns,
                context=context,
            )
        except MaxTurnsExceeded as e:
            if not self.max_turns_recovery:
                raise

            logger.info("Max turns exceeded, attempting recovery")
            final_input = _extract_input(e)
            final_input.append(self._wrap_up_prompt())
            result = await Runner.run(
                agent,
                final_input,
                max_turns=self.recovery_turns,
                context=context,
            )
        except Exception:
            raise

        return result

    async def run_streamed(
        self,
        agent: Agent[Any],
        input: str | list[TResponseInputItem],
        max_turns: int = 10,
        context: Any | None = None,
        forwarder: Callable | None = None,
    ) -> RunResultStreaming:
        """Run the agent in streaming mode with automatic activity tracking.

        The returned streaming result can be used just like the underlying framework's
        streaming result. Activity tracking happens automatically.

        Args:
            session: SQLAlchemy session for database operations.
            agent: Agent instance.
            input: User input/prompt for the agent.
            agent_id: Optional agent ID (generated if not provided).
            max_turns: Maximum number of agent iterations.
            agent_type: Optional agent type for activity tracking.
            context: Optional context for the agent run.

        Returns:
            Streaming result from the agent execution.

        Example:
            result = await runner.run_streamed(session, agent, "Research XYZ", agent_id="123")
            async for event in result.stream_events():
                print(event)
        """
        # TODO match method signature of Runner.run_streamed
        # TODO I want to defer the `await` to the caller side
        # TODO forwarder is just a placeholder but we need to come up with a solution for that functionality
        try:
            result = Runner.run_streamed(
                agent,
                input,
                max_turns=max_turns,
                context=context,
            )

            async for event in result.stream_events():
                if forwarder:
                    await forwarder(event)
                # yield event

            return result
        except MaxTurnsExceeded as e:
            if not self.max_turns_recovery:
                raise

            logger.info("Max turns exceeded, attempting recovery")
            final_input = _extract_input(e)
            final_input.append(self._wrap_up_prompt())
            result = Runner.run_streamed(
                agent,
                final_input,
                max_turns=self.recovery_turns,
                context=context,
            )

            async for event in result.stream_events():
                if forwarder:
                    await forwarder(event)
                # yield event

            return result
        except Exception:
            raise

        return result
