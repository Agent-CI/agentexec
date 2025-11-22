from __future__ import annotations
from typing import Any, Protocol, TypedDict, Unpack
from uuid import UUID
from pydantic import BaseModel, Field

from agentexec import activity


class TaskHandlerKwargs(TypedDict):
    """Type for kwargs passed to task handlers.

    Handlers receive the payload as a dict and agent_id as a separate parameter.
    """

    agent_id: UUID
    payload: dict[str, Any]


class TaskHandler(Protocol):
    """Protocol for task handler functions.

    Handlers accept **kwargs matching HandlerKwargs structure.
    Return value is ignored. Can be sync or async.
    """

    def __call__(self, **kwargs: Unpack[TaskHandlerKwargs]) -> None: ...


class Task(BaseModel):
    """Represents a background task.

    Tasks are serialized to JSON and enqueued to Redis for workers to process.
    Each task has a type (matching a registered handler), a payload with
    parameters, and an agent_id for tracking.
    """

    task_name: str
    payload: dict[str, Any] = Field(default_factory=dict)
    agent_id: UUID

    @classmethod
    def create(cls, task_name: str, payload: dict[str, Any]) -> Task:
        """Create a new task with automatic activity tracking.

        This is a convenience method that creates both a Task instance and
        its corresponding activity record in one step.

        Args:
            task_name: Name/type of the task (e.g., "research", "analysis")
            payload: Task parameters to pass to the handler

        Returns:
            Task instance with agent_id set

        Example:
            task = Task.create("research_company", {"company": "Acme Corp"})
            # Activity record is created automatically
        """
        agent_id = activity.create(
            task_name=task_name,
            message="Waiting to start.",
        )
        return cls(
            task_name=task_name,
            payload=payload,
            agent_id=agent_id,
        )

    def started(self) -> None:
        """Mark the task as started in the activity log.

        Updates the activity status to RUNNING with a starting message.

        Example:
            task = Task.create("research", {"company": "Acme"})
            task.started()
        """
        activity.update(
            agent_id=self.agent_id,
            message="Task started.",
            completion_percentage=0,
        )

    def completed(self) -> None:
        """Mark the task as completed in the activity log.

        Updates the activity status to COMPLETE with a completion message.

        Example:
            task = Task.create("research", {"company": "Acme"})
            task.completed()
        """
        activity.update(
            agent_id=self.agent_id,
            message="Task completed successfully.",
            completion_percentage=100,
            status=activity.Status.COMPLETE,
        )

    def errored(self, exception: Exception) -> None:
        """Mark the task as errored in the activity log.

        Updates the activity status to ERROR with the provided error message.

        Args:
            error_message: Description of the error that occurred

        Example:
            task = Task.create("research", {"company": "Acme"})
            task.error("Failed to fetch data from API.")
        """
        activity.update(
            agent_id=self.agent_id,
            message=f"Task failed with error: {exception}",
            status=activity.Status.ERROR,
        )

    @property
    def handler_kwargs(self) -> TaskHandlerKwargs:
        """Get kwargs to pass to the task handler.

        Builds a dictionary containing the task's payload and agent_id,
        matching the TaskHandlerKwargs structure expected by handlers.

        Returns:
            Dict with 'payload' (task parameters) and 'agent_id' (tracking ID)

        Example:
            kwargs = task._handler_kwargs
            # Returns: {"payload": {"company": "Acme"}, "agent_id": UUID(...)}
        """
        return {"agent_id": self.agent_id, "payload": self.payload}
