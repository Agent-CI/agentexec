from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import Any, Protocol, TypeAlias, TypeVar, cast, get_type_hints
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from agentexec import activity
from agentexec.config import CONF
from agentexec.state import KEY_RESULT, backend


TaskResult: TypeAlias = BaseModel
ContextT = TypeVar("ContextT", bound=BaseModel)
ResultT = TypeVar("ResultT", bound=TaskResult)


class _SyncTaskHandler(Protocol[ContextT, ResultT]):
    __name__: str

    def __call__(
        self,
        *,
        agent_id: UUID,
        context: ContextT,
    ) -> ResultT: ...


class _AsyncTaskHandler(Protocol[ContextT, ResultT]):
    __name__: str

    async def __call__(
        self,
        *,
        agent_id: UUID,
        context: ContextT,
    ) -> ResultT: ...


# TODO: Using Any,Any here because of contravariance limitations with function parameters.
# A function accepting MyContext (specific) is not statically assignable to one expecting
# BaseModel (general). Runtime validation in TaskDefinition._infer_context_type catches
# invalid context/return types. Revisit if Python typing evolves to support this pattern.
TaskHandler: TypeAlias = _SyncTaskHandler[Any, Any] | _AsyncTaskHandler[Any, Any]


class TaskDefinition:
    """Definition of a task type (created at registration time).

    Encapsulates the handler function and its metadata (context class, lock key).
    One TaskDefinition can spawn many Task instances.
    """

    name: str
    handler: TaskHandler
    context_type: type[BaseModel]
    result_type: type[BaseModel] | None
    lock_key: str | None

    def __init__(
        self,
        name: str,
        handler: TaskHandler,
        *,
        context_type: type[BaseModel] | None = None,
        result_type: type[BaseModel] | None = None,
        lock_key: str | None = None,
    ) -> None:
        """Initialize task definition.

        Args:
            name: Task type name.
            handler: Handler function (sync or async).
            context_type: Explicit context type (inferred from annotations if omitted).
            result_type: Explicit result type (inferred from annotations if omitted).
            lock_key: String template for distributed locking, evaluated against
                context fields (e.g. ``"user:{user_id}"``). When set, only one task
                with the same evaluated lock key can run at a time.

        Raises:
            TypeError: If handler doesn't have a typed ``context`` parameter
                with a BaseModel subclass.
        """
        self.name = name
        self.handler = handler
        self.context_type = context_type or self._infer_context_type(handler)
        self.result_type = result_type or self._infer_result_type(handler)
        self.lock_key = lock_key

    def get_lock_key(self, context: Mapping[str, Any]) -> str | None:
        """Evaluate the lock key template against context data."""
        return self.lock_key.format(**context) if self.lock_key else None

    def hydrate_context(self, context: Mapping[str, Any]) -> BaseModel:
        """Validate raw context data into the registered Pydantic model."""
        return self.context_type.model_validate(context)

    async def execute(self, task: Task) -> TaskResult | None:
        """Execute the task handler and manage its lifecycle.

        Handles activity tracking (started/complete/error) and result storage.
        """
        context = self.hydrate_context(task.context)

        await activity.update(
            agent_id=task.agent_id,
            message=CONF.activity_message_started,
            percentage=0,
        )

        try:
            if inspect.iscoroutinefunction(self.handler):
                handler = cast(_AsyncTaskHandler, self.handler)
                result = await handler(agent_id=task.agent_id, context=context)
            else:
                handler = cast(_SyncTaskHandler, self.handler)
                result = handler(agent_id=task.agent_id, context=context)

            if isinstance(result, BaseModel):
                key = backend.format_key(*KEY_RESULT, str(task.agent_id))
                await backend.state.set(key, backend.serialize(result), ttl_seconds=CONF.result_ttl)

            await activity.update(
                agent_id=task.agent_id,
                message=CONF.activity_message_complete,
                percentage=100,
                status=activity.Status.COMPLETE,
            )
            return result
        except Exception as e:
            await activity.update(
                agent_id=task.agent_id,
                message=CONF.activity_message_error.format(error=e),
                status=activity.Status.ERROR,
            )
            raise e

    def _infer_context_type(self, handler: TaskHandler) -> type[BaseModel]:
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

    def _infer_result_type(self, handler: TaskHandler) -> type[BaseModel] | None:
        hints = get_type_hints(handler)
        if "return" not in hints:
            return None
        return_type = hints["return"]
        if not (inspect.isclass(return_type) and issubclass(return_type, BaseModel)):
            return None
        return return_type


class Task(BaseModel):
    """A background task instance — pure data, no behavior.

    Tasks are serialized to JSON and pushed to the queue. Workers pop them,
    look up the TaskDefinition by task_name, and execute via the definition.

    Context is stored as a raw dict. The TaskDefinition hydrates it into
    the registered Pydantic model at execution time.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    task_name: str
    context: Mapping[str, Any]
    agent_id: UUID
    retry_count: int = 0

    @classmethod
    async def create(
        cls,
        task_name: str,
        context: BaseModel,
        metadata: dict[str, Any] | None = None,
    ) -> Task:
        """Create a new task with automatic activity tracking.

        Creates an activity record and returns a Task ready to be
        serialized and pushed to the queue.

        Args:
            task_name: Name of the registered task.
            context: Pydantic model with the task's input data.
            metadata: Optional dict attached to the activity record
                (e.g. ``{"organization_id": "org-123"}``).

        Returns:
            Task instance with ``agent_id`` set for tracking.
        """
        agent_id = await activity.create(
            task_name=task_name,
            message=CONF.activity_message_create,
            metadata=metadata,
        )

        return cls(
            task_name=task_name,
            context=context.model_dump(mode="json"),
            agent_id=agent_id,
        )
