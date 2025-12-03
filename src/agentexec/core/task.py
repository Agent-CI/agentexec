from __future__ import annotations

import asyncio
import inspect
from typing import Any, Coroutine, Protocol, TypedDict, Union, Unpack, get_type_hints
from uuid import UUID

from pydantic import BaseModel, ConfigDict, PrivateAttr, field_serializer

from agentexec import activity, state
from agentexec.config import CONF


class TaskHandlerKwargs(TypedDict):
    """Type for kwargs passed to task handlers.

    Handlers receive a typed Pydantic context and agent_id.
    """

    agent_id: UUID
    context: BaseModel


class TaskHandler(Protocol):
    """Protocol for task handler functions (sync or async).

    Handlers accept **kwargs matching TaskHandlerKwargs structure.
    Must return a Pydantic BaseModel (or Coroutine that resolves to BaseModel).
    """

    __name__: str  # All functions have __name__ attribute

    def __call__(
        self,
        **kwargs: Unpack[TaskHandlerKwargs],
    ) -> Union[BaseModel, Coroutine[Any, Any, BaseModel]]: ...


class TaskDefinition:
    """Definition of a task type (created at registration time).

    Encapsulates the handler function and its metadata (context class, etc.).
    One TaskDefinition can spawn many Task instances.

    This object is created once when a task is registered via @pool.task(),
    and acts as a factory to reconstruct Task instances from the queue with
    properly typed context.

    Example:
        @pool.task("research_company")
        async def research(agent_id: UUID, context: ResearchContext):
            print(context.company_name)

        # TaskDefinition captures ResearchContext from the type hint
        # and uses it to deserialize tasks from the queue
    """

    name: str
    handler: TaskHandler
    context_class: type[BaseModel]
    # TODO we handle this with serialize/deserialize when writing the result so this can probably go away
    result_class: type[BaseModel]

    def __init__(self, name: str, handler: TaskHandler):
        """Initialize task definition.

        Args:
            name: Task type name
            handler: Handler function (sync or async)

        Raises:
            TypeError: If handler doesn't have a typed 'context' parameter with BaseModel subclass
            TypeError: If handler doesn't have a return type annotation with BaseModel subclass
        """
        self.name = name
        self.handler = handler
        self.context_class = self._infer_context_class(handler)
        self.result_class = self._infer_result_class(handler)

    def _infer_context_class(self, handler: TaskHandler) -> type[BaseModel]:
        """Infer context class from handler's type annotations.

        Looks for a 'context' parameter with a Pydantic BaseModel type hint.

        Args:
            handler: The task handler function

        Returns:
            Context class (BaseModel subclass)

        Raises:
            TypeError: If 'context' parameter is missing or not a BaseModel subclass
        """
        hints = get_type_hints(handler)
        if "context" not in hints:
            raise TypeError(
                f"Task handler '{handler.__name__}' must have a 'context' parameter "
                f"with a BaseModel type annotation"
            )

        context_type = hints["context"]
        if not (inspect.isclass(context_type) and issubclass(context_type, BaseModel)):
            raise TypeError(
                f"Task handler '{handler.__name__}' context parameter must be a "
                f"BaseModel subclass, got {context_type}"
            )

        return context_type

    def _infer_result_class(self, handler: TaskHandler) -> type[BaseModel]:
        """Infer result class from handler's return type annotation.

        Looks for a return annotation with a Pydantic BaseModel type hint.

        Args:
            handler: The task handler function

        Returns:
            Result class (BaseModel subclass)

        Raises:
            TypeError: If return annotation is missing or not a BaseModel subclass
        """
        hints = get_type_hints(handler)
        if "return" not in hints:
            raise TypeError(
                f"Task handler '{handler.__name__}' must have a return type "
                f"annotation with a BaseModel subclass"
            )

        return_type = hints["return"]
        if not (inspect.isclass(return_type) and issubclass(return_type, BaseModel)):
            raise TypeError(
                f"Task handler '{handler.__name__}' return type must be a "
                f"BaseModel subclass, got {return_type}"
            )

        return return_type


class Task(BaseModel):
    """Represents a background task instance.

    Tasks are serialized to JSON and enqueued to Redis for workers to process.
    Each task has a type (matching a registered TaskDefinition), a typed context,
    and an agent_id for tracking.

    The context is stored as its native Pydantic type. Serialization to dict
    happens automatically via field_serializer when dumping to JSON.

    After deserialization, call bind() to attach the TaskDefinition, then
    execute() to run the task handler.

    Example:
        # Create with typed context
        ctx = ResearchContext(company_name="Anthropic")
        task = Task.create("research", ctx)
        task.context.company_name  # Typed access!

        # Serialize to JSON for Redis (context becomes dict)
        json_str = task.model_dump_json()

        # Worker deserializes and executes
        task = Task.from_serialized(task_def, data)
        await task.execute()
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    task_name: str
    context: BaseModel
    agent_id: UUID
    _definition: TaskDefinition | None = PrivateAttr(default=None)

    @field_serializer("context")
    def serialize_context(self, value: BaseModel) -> dict[str, Any]:
        """Serialize context to dict for JSON storage."""
        return value.model_dump(mode="json")

    @classmethod
    def from_serialized(cls, definition: TaskDefinition, data: dict[str, Any]) -> Task:
        """Create a Task from serialized data with its definition bound.

        Args:
            definition: The TaskDefinition containing the handler and context_class
            data: Serialized task data with task_name, context, and agent_id

        Returns:
            Task instance with typed context and bound definition
        """
        task = cls(
            task_name=data["task_name"],
            context=definition.context_class.model_validate(data["context"]),
            agent_id=data["agent_id"],
        )
        task._definition = definition
        return task

    @classmethod
    def create(cls, task_name: str, context: BaseModel) -> Task:
        """Create a new task with automatic activity tracking.

        This is a convenience method that creates both a Task instance and
        its corresponding activity record in one step.

        Args:
            task_name: Name/type of the task (e.g., "research", "analysis")
            context: Task context as a Pydantic model

        Returns:
            Task instance with agent_id set

        Example:
            ctx = ResearchContext(company="Acme")
            task = Task.create("research_company", ctx)
            task.context.company  # Typed access
        """
        agent_id = activity.create(
            task_name=task_name,
            message=CONF.activity_message_create,
        )

        return cls(
            task_name=task_name,
            context=context,
            agent_id=agent_id,
        )

    async def execute(self) -> BaseModel | None:
        """Execute the task using its bound definition's handler.

        Manages task lifecycle: marks started, runs handler, marks completed/errored.

        Returns:
            Handler return value, or None if handler raised an exception

        Raises:
            RuntimeError: If task has not been bound to a definition
        """
        if self._definition is None:
            raise RuntimeError("Task must be bound to a definition before execution")

        activity.update(
            agent_id=self.agent_id,
            message=CONF.activity_message_started,
            percentage=0,
        )

        try:
            kwargs: TaskHandlerKwargs = {
                "agent_id": self.agent_id,
                "context": self.context,
            }

            result: BaseModel
            if asyncio.iscoroutinefunction(self._definition.handler):
                result = await self._definition.handler(**kwargs)
            else:
                result = self._definition.handler(**kwargs)  # type: ignore[assignment]

            await state.aset_result(
                self.agent_id,
                result,
                ttl_seconds=CONF.result_ttl,
            )

            activity.update(
                agent_id=self.agent_id,
                message=CONF.activity_message_complete,
                percentage=100,
                status=activity.Status.COMPLETE,
            )
            return result
        except Exception as e:
            activity.update(
                agent_id=self.agent_id,
                message=CONF.activity_message_error.format(error=e),
                status=activity.Status.ERROR,
            )
            return None
