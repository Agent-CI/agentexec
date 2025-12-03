from __future__ import annotations
from typing import Any
from abc import ABC
import logging
import uuid

from agentexec import activity

logger = logging.getLogger(__name__)


class BaseAgentRunner(ABC):
    """Abstract base class for agent runners with activity tracking.

    This base class provides:
    - Automatic activity tracking (QUEUED -> RUNNING -> COMPLETE/ERROR)
    - Status update tool for agent self-reporting
    - Common lifecycle management
    - Error handling and recovery patterns

    Subclasses must implement:
    - _execute_agent(): The actual agent execution logic
    - max_turns_exceptions: Tuple of exception classes for max turns errors
    """

    agent_id: uuid.UUID
    max_turns_recovery: bool
    recovery_turns: int

    prompts: _RunnerPrompts
    tools: _RunnerTools

    def __init__(
        self,
        agent_id: uuid.UUID,
        *,
        max_turns_recovery: bool = True,
        recovery_turns: int = 5,
        wrap_up_prompt: str | None = None,
        report_status_prompt: str | None = None,
    ) -> None:
        """Initialize the runner.

        Args:
            max_turns_recovery: Enable automatic recovery when max turns exceeded.
            wrap_up_prompt: Prompt to use for recovery run.
            recovery_turns: Number of turns allowed for recovery.
            status_prompt: Instruction snippet about using the status tool.
        """
        self.agent_id = agent_id
        self.max_turns_recovery = max_turns_recovery
        self.recovery_turns = recovery_turns

        # Tools namespace for accessing runner-provided tools
        self.prompts = _RunnerPrompts(
            report_status=report_status_prompt,
            wrap_up=wrap_up_prompt,
        )
        self.tools = _RunnerTools(self.agent_id)


class _RunnerPrompts:
    """Namespace for runner-provided prompts.

    Accessed via runner.prompts.*
    """

    use_max_turms: str = (
        "You will be notified when you have exhausted the computing resources available, "
        "so continue until you are able to completely populate the schema or you are "
        "notified that you have exhausted your resources."
    )
    report_status: str = (
        "Using report_activity tool:\n"
        "    - Always report your current activity before you start a new step using the report_activity tool. \n"
        "    - Include a brief message about the task and context you are operating in (10 words or less). \n"
        "    - Don't use internal data or underlying system info and instead focus on what the user would care about. \n"
        "    - This informs the user of your progress as they have no visibility into your internal operations. \n"
        "    - You can call multiple tools in parallel per step so don't waste an entire step on this.\n"
        "    - Call it at the top of your list of the next round of tool uses; we should be careful to minimize turns used.\n"
    )
    wrap_up: str = "Please summarize your findings."

    def __init__(
        self,
        *,
        wrap_up: str | None = None,
        report_status: str | None = None,
    ) -> None:
        if wrap_up is not None:
            self.wrap_up = wrap_up
        if report_status is not None:
            self.report_status = report_status


class _RunnerTools:
    """Namespace for runner-provided tools.

    Accessed via runner.tools.*
    """

    _agent_id: uuid.UUID

    def __init__(self, agent_id: uuid.UUID) -> None:
        self._agent_id = agent_id

    @property
    def report_status(self) -> Any:
        """Get the status update tool.

        This tool allows agents to report their progress back to the activity tracker.
        The tool is bound to the current agent_id during runner.run().

        Subclasses should override this to wrap with framework-specific decorators.

        Returns:
            Plain function for status updates.
        """
        agent_id = self._agent_id

        def report_activity(message: str, percentage: int) -> str:
            """Report progress and status updates.

            Use this tool to report your progress as you work through the task.

            Args:
                message: A brief description of what you're currently doing
                percentage: Your estimated completion percentage (0-100)

            Returns:
                Confirmation message
            """
            activity.update(
                agent_id=agent_id,
                message=message,
                percentage=percentage,
            )
            return "Status updated"

        return report_activity
