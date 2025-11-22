import logging
import uuid
from typing import Any, Callable

from google.adk.agents import LlmAgent
from google.adk.runners import InMemoryRunner, Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from agentexec.runners.base import BaseAgentRunner, _RunnerTools


logger = logging.getLogger(__name__)


class _GoogleADKRunnerTools(_RunnerTools):
    """Google ADK-specific tools wrapper.

    Note: Google ADK tools are typically defined as regular Python functions.
    The agent framework handles tool registration automatically.
    """

    @property
    def report_status(self) -> Any:
        """Get the status update tool for Google ADK.

        Returns the plain function as Google ADK handles tool registration.
        """
        return super().report_status


class GoogleADKRunResult:
    """Result wrapper for Google ADK agent execution."""

    def __init__(self, final_content: types.Content | None, events: list[Any]):
        """Initialize the result.

        Args:
            final_content: The final response content from the agent.
            events: List of all events generated during execution.
        """
        self.final_content = final_content
        self.events = events

    @property
    def final_output(self) -> str | None:
        """Extract the final text output from the result."""
        if self.final_content and self.final_content.parts:
            # Extract text from the first part
            for part in self.final_content.parts:
                if hasattr(part, "text") and part.text:
                    return part.text
        return None


class GoogleADKRunner(BaseAgentRunner):
    """Runner for Google Agent Development Kit (ADK) with automatic activity tracking.

    This runner wraps the Google ADK and provides:
    - Automatic agent_id generation
    - Activity lifecycle management (QUEUED -> RUNNING -> COMPLETE/ERROR)
    - Session management with InMemorySessionService
    - Status update tool with agent_id pre-baked
    - Streaming event support

    Example:
        runner = agentexec.GoogleADKRunner(
            agent_id=agent_id,
            app_name="my_app",
            max_turns_recovery=False,
            report_status_prompt="Use report_activity(message, percentage) to report progress.",
        )

        agent = LlmAgent(
            name="Research Agent",
            model="gemini-2.0-flash",
            instruction=f"Research companies. {runner.prompts.report_status}",
            tools=[runner.tools.report_status],
        )

        result = await runner.run(
            agent=agent,
            input="Research Acme Corp",
            max_turns=15,
        )
    """

    app_name: str
    session_service: InMemorySessionService
    _runner: Runner | None

    def __init__(
        self,
        agent_id: uuid.UUID,
        *,
        app_name: str = "agentexec",
        max_turns_recovery: bool = False,
        wrap_up_prompt: str | None = None,
        recovery_turns: int = 5,
        report_status_prompt: str | None = None,
    ) -> None:
        """Initialize the Google ADK runner.

        Args:
            agent_id: UUID for tracking this agent's activity.
            app_name: Application name for session management.
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
        # Override with Google ADK-specific tools
        self.tools = _GoogleADKRunnerTools(self.agent_id)

        # Initialize session service
        self.app_name = app_name
        self.session_service = InMemorySessionService()
        self._runner = None

    def _get_or_create_runner(self, agent: LlmAgent) -> Runner:
        """Get or create a Runner instance for the agent.

        Args:
            agent: The LlmAgent to create a runner for.

        Returns:
            Configured Runner instance.
        """
        # Create a new runner for this agent
        return Runner(
            agent=agent,
            app_name=self.app_name,
            session_service=self.session_service,
        )

    async def _ensure_session(self, user_id: str, session_id: str) -> None:
        """Ensure a session exists in the session service.

        Args:
            user_id: User identifier for the session.
            session_id: Session identifier.
        """
        # Create session if it doesn't exist
        try:
            await self.session_service.create_session(
                app_name=self.app_name,
                user_id=user_id,
                session_id=session_id,
            )
        except Exception as e:
            # Session might already exist, which is fine
            logger.debug(f"Session creation note: {e}")

    async def run(
        self,
        agent: LlmAgent,
        input: str | types.Content,
        max_turns: int = 10,
        context: Any | None = None,
        user_id: str | None = None,
        session_id: str | None = None,
    ) -> GoogleADKRunResult:
        """Run the agent with automatic activity tracking.

        Args:
            agent: LlmAgent instance.
            input: User input/prompt for the agent (string or Content object).
            max_turns: Maximum number of agent iterations (Note: Google ADK manages this internally).
            context: Optional context for the agent run (not used by Google ADK currently).
            user_id: User ID for session management (defaults to agent_id).
            session_id: Session ID (defaults to agent_id).

        Returns:
            GoogleADKRunResult with final output and events.
        """
        # Use agent_id as default for user_id and session_id
        user_id = user_id or str(self.agent_id)
        session_id = session_id or str(self.agent_id)

        # Ensure session exists
        await self._ensure_session(user_id, session_id)

        # Create runner
        runner = self._get_or_create_runner(agent)

        # Convert input to Content if it's a string
        if isinstance(input, str):
            content = types.Content(
                role="user",
                parts=[types.Part(text=input)]
            )
        else:
            content = input

        # Run the agent and collect events
        events = []
        final_content = None

        try:
            async for event in runner.run_async(
                user_id=user_id,
                session_id=session_id,
                new_message=content,
            ):
                events.append(event)
                if hasattr(event, "is_final_response") and event.is_final_response():
                    final_content = event.content

        except Exception as e:
            logger.error(f"Error during Google ADK agent execution: {e}")
            raise

        return GoogleADKRunResult(final_content=final_content, events=events)

    async def run_streamed(
        self,
        agent: LlmAgent,
        input: str | types.Content,
        max_turns: int = 10,
        context: Any | None = None,
        forwarder: Callable | None = None,
        user_id: str | None = None,
        session_id: str | None = None,
    ) -> GoogleADKRunResult:
        """Run the agent in streaming mode with automatic activity tracking.

        The forwarder callback receives each event as it's generated, allowing
        real-time processing of agent execution.

        Args:
            agent: LlmAgent instance.
            input: User input/prompt for the agent (string or Content object).
            max_turns: Maximum number of agent iterations (Note: Google ADK manages this internally).
            context: Optional context for the agent run (not used by Google ADK currently).
            forwarder: Optional async callback to process each event as it arrives.
            user_id: User ID for session management (defaults to agent_id).
            session_id: Session ID (defaults to agent_id).

        Returns:
            GoogleADKRunResult with final output and events.

        Example:
            async def handle_event(event):
                print(f"Event: {event}")

            result = await runner.run_streamed(
                agent=agent,
                input="Research XYZ",
                forwarder=handle_event
            )
        """
        # Use agent_id as default for user_id and session_id
        user_id = user_id or str(self.agent_id)
        session_id = session_id or str(self.agent_id)

        # Ensure session exists
        await self._ensure_session(user_id, session_id)

        # Create runner
        runner = self._get_or_create_runner(agent)

        # Convert input to Content if it's a string
        if isinstance(input, str):
            content = types.Content(
                role="user",
                parts=[types.Part(text=input)]
            )
        else:
            content = input

        # Run the agent in streaming mode
        events = []
        final_content = None

        try:
            async for event in runner.run_async(
                user_id=user_id,
                session_id=session_id,
                new_message=content,
            ):
                events.append(event)

                # Forward event if forwarder is provided
                if forwarder:
                    await forwarder(event)

                # Check if this is the final response
                if hasattr(event, "is_final_response") and event.is_final_response():
                    final_content = event.content

        except Exception as e:
            logger.error(f"Error during Google ADK agent streaming execution: {e}")
            raise

        return GoogleADKRunResult(final_content=final_content, events=events)
