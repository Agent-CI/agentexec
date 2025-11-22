"""LangChain agent runner with activity tracking."""

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
        self.agent_executor = agent_executor

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

        Args:
            input: User input/prompt for the agent. Can be a string or dict with "input" key.
            max_iterations: Maximum number of agent iterations.
            **kwargs: Additional arguments passed to AgentExecutor.ainvoke().

        Returns:
            Result from the agent execution.

        Raises:
            Exception: If agent execution fails.
        """
        # Normalize input to dict format expected by AgentExecutor
        if isinstance(input, str):
            agent_input = {"input": input}
        else:
            agent_input = input

        try:
            result = await self.agent_executor.ainvoke(
                agent_input,
                config={"max_iterations": max_iterations, **kwargs.get("config", {})},
            )
            return result
        except Exception as e:
            # Check if this is a max iterations error
            if "iterations" in str(e).lower() and self.max_turns_recovery:
                logger.info("Max iterations exceeded, attempting recovery")

                # Create recovery input with wrap-up prompt
                recovery_input = {
                    "input": self.prompts.wrap_up,
                    "chat_history": agent_input.get("chat_history", []),
                }

                result = await self.agent_executor.ainvoke(
                    recovery_input,
                    config={"max_iterations": self.recovery_turns},
                )
                return result
            raise

    async def run_streamed(
        self,
        input: str | dict[str, Any],
        max_iterations: int = 15,
        **kwargs: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        """Run the LangChain agent in streaming mode with automatic activity tracking.

        This method uses LangChain's astream_events API to provide granular streaming
        of agent execution, including intermediate steps, tool calls, and LLM responses.

        Args:
            input: User input/prompt for the agent. Can be a string or dict with "input" key.
            max_iterations: Maximum number of agent iterations.
            **kwargs: Additional arguments passed to AgentExecutor.astream_events().

        Yields:
            Event dictionaries from the agent execution.

        Raises:
            Exception: If agent execution fails.

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

        try:
            async for event in self.agent_executor.astream_events(
                agent_input,
                version="v2",
                config={"max_iterations": max_iterations, **kwargs.get("config", {})},
            ):
                yield event
        except Exception as e:
            # Check if this is a max iterations error
            if "iterations" in str(e).lower() and self.max_turns_recovery:
                logger.info("Max iterations exceeded, attempting recovery in streaming mode")

                # Create recovery input with wrap-up prompt
                recovery_input = {
                    "input": self.prompts.wrap_up,
                    "chat_history": agent_input.get("chat_history", []),
                }

                async for event in self.agent_executor.astream_events(
                    recovery_input,
                    version="v2",
                    config={"max_iterations": self.recovery_turns},
                ):
                    yield event
            else:
                raise
