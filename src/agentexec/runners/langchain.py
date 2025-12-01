"""LangChain agent runner with activity tracking.

Supports both classic AgentExecutor and LangGraph agents with automatic
max iterations handling via LangChain's built-in early_stopping_method.
"""

import logging
import uuid
from typing import Any, AsyncIterator

from langchain.agents import AgentExecutor
from langchain_core.tools import tool

from agentexec.runners.base import BaseAgentRunner, _RunnerTools

logger = logging.getLogger(__name__)


class _LangChainRunnerTools(_RunnerTools):
    """LangChain-specific tools wrapper that decorates with @tool."""

    @property
    def report_status(self) -> Any:
        """Get the status update tool wrapped with @tool decorator.

        Returns:
            LangChain tool for status reporting.
        """
        agent_id = self._agent_id

        @tool
        def report_activity(message: str, percentage: int) -> str:
            """Report progress and status updates.

            Use this tool to report your progress as you work through the task.

            Args:
                message: A brief description of what you're currently doing
                percentage: Your estimated completion percentage (0-100)

            Returns:
                Confirmation message
            """
            from agentexec import activity

            activity.update(
                agent_id=agent_id,
                message=message,
                completion_percentage=percentage,
            )
            return "Status updated"

        return report_activity


class LangChainRunner(BaseAgentRunner):
    """Runner for LangChain agents with automatic activity tracking.

    This runner wraps LangChain's AgentExecutor and provides:
    - Automatic agent_id generation
    - Activity lifecycle management (QUEUED -> RUNNING -> COMPLETE/ERROR)
    - Max iterations recovery with configurable wrap-up prompts
    - Status update tool with agent_id pre-baked
    - Support for streaming via astream_events

    Example:
        from langchain_openai import ChatOpenAI
        from langchain.agents import create_react_agent, AgentExecutor
        from langchain import hub

        # Create LangChain agent
        llm = ChatOpenAI(model="gpt-4")
        prompt = hub.pull("hwchase17/react")
        agent = create_react_agent(llm, tools, prompt)
        agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

        # Wrap with agentexec runner
        runner = agentexec.LangChainRunner(
            agent_id=activity.id,
            agent_executor=agent_executor,
            max_turns_recovery=True,
        )

        # Execute with activity tracking
        result = await runner.run("Research competitor products")
    """

    def __init__(
        self,
        agent_id: uuid.UUID,
        agent_executor: AgentExecutor,
        *,
        max_turns_recovery: bool = False,
        wrap_up_prompt: str | None = None,
        recovery_turns: int = 5,
        report_status_prompt: str | None = None,
    ) -> None:
        """Initialize the LangChain runner.

        Args:
            agent_id: UUID for tracking this agent's activity.
            agent_executor: LangChain AgentExecutor instance.
            max_turns_recovery: Enable automatic recovery when max iterations exceeded.
                When True, uses LangChain's early_stopping_method='generate' to have the
                LLM generate a final answer when max iterations is reached.
            wrap_up_prompt: Prompt to use for recovery run (not used with built-in method).
            recovery_turns: Number of turns allowed for recovery (not used with built-in method).
            report_status_prompt: Instruction snippet about using the status tool.

        Note:
            LangChain's AgentExecutor has built-in max iterations handling via
            early_stopping_method. When max_turns_recovery=True, we leverage this
            instead of manually catching exceptions. The agent will automatically
            call the LLM one final time to generate a summary when max_iterations
            is reached.
        """
        super().__init__(
            agent_id,
            max_turns_recovery=max_turns_recovery,
            recovery_turns=recovery_turns,
            wrap_up_prompt=wrap_up_prompt,
            report_status_prompt=report_status_prompt,
        )
        self.agent_executor = agent_executor

        # Configure early stopping for max iterations
        # 'generate' calls LLM one final time to create answer (like wrap-up)
        # 'force' just returns "Agent stopped due to iteration limit"
        if max_turns_recovery and self.agent_executor.early_stopping_method == "force":
            logger.info(
                "Enabling early_stopping_method='generate' for max iterations recovery"
            )
            self.agent_executor.early_stopping_method = "generate"

        # Override with LangChain-specific tools
        self.tools = _LangChainRunnerTools(self.agent_id)

        # Inject status reporting tool into agent's tools if not already present
        status_tool = self.tools.report_status
        if status_tool not in self.agent_executor.tools:
            self.agent_executor.tools.append(status_tool)

    async def run(
        self,
        input: str | dict[str, Any],
        max_iterations: int = 15,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Run the LangChain agent with automatic activity tracking.

        LangChain's AgentExecutor handles max iterations internally via
        early_stopping_method. If max_turns_recovery=True was set during
        initialization, the agent will automatically generate a final answer
        when max_iterations is reached.

        Args:
            input: User input/prompt for the agent. Can be a string or dict with "input" key.
            max_iterations: Maximum number of agent iterations.
            **kwargs: Additional arguments passed to AgentExecutor.ainvoke().

        Returns:
            Result from the agent execution. When max iterations is reached with
            early_stopping_method='generate', the result will contain the LLM's
            final generated answer.

        Raises:
            Exception: If agent execution fails (non-max-iterations errors).

        Note:
            Unlike OpenAI's MaxTurnsExceeded exception, LangChain's AgentExecutor
            handles max iterations gracefully and returns a normal result, so no
            exception handling is needed.
        """
        # Normalize input to dict format expected by AgentExecutor
        if isinstance(input, str):
            agent_input = {"input": input}
        else:
            agent_input = input

        result = await self.agent_executor.ainvoke(
            agent_input,
            config={"max_iterations": max_iterations, **kwargs.get("config", {})},
        )
        return result

    async def run_streamed(
        self,
        input: str | dict[str, Any],
        max_iterations: int = 15,
        **kwargs: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        """Run the LangChain agent in streaming mode with automatic activity tracking.

        This method uses LangChain's astream_events API to provide granular streaming
        of agent execution, including intermediate steps, tool calls, and LLM responses.

        LangChain's AgentExecutor handles max iterations internally, so streaming
        will continue until completion or max_iterations is reached (with early
        stopping applied if configured).

        Args:
            input: User input/prompt for the agent. Can be a string or dict with "input" key.
            max_iterations: Maximum number of agent iterations.
            **kwargs: Additional arguments passed to AgentExecutor.astream_events().

        Yields:
            Event dictionaries from the agent execution.

        Raises:
            Exception: If agent execution fails (non-max-iterations errors).

        Example:
            async for event in runner.run_streamed("Research topic"):
                if event["event"] == "on_chat_model_stream":
                    print(event["data"]["chunk"].content, end="")
        """
        # Normalize input to dict format expected by AgentExecutor
        if isinstance(input, str):
            agent_input = {"input": input}
        else:
            agent_input = input

        async for event in self.agent_executor.astream_events(
            agent_input,
            version="v2",
            config={"max_iterations": max_iterations, **kwargs.get("config", {})},
        ):
            yield event
