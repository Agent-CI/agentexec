from __future__ import annotations
import asyncio
import inspect
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Protocol,
    TypeAlias,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)
from uuid import UUID

from pydantic import BaseModel

from agentexec import activity
from agentexec.core import queue
from agentexec.core.task import Task, TaskResult

if TYPE_CHECKING:
    from agentexec.worker import Pool

StepResult: TypeAlias = Union[TaskResult, tuple[TaskResult, ...]]


class _SyncStepHandler(Protocol):
    """Protocol for pipeline step handler methods.

    Step handlers are methods on a pipeline class that receive
    one or more BaseModel context arguments from the previous step.
    """

    __name__: str

    def __call__(
        self,
        instance: _PipelineBase,
        **kwargs: BaseModel,
    ) -> StepResult: ...


class _AsyncStepHandler(Protocol):
    """Protocol for async pipeline step handler methods.

    Step handlers are methods on a pipeline class that receive
    one or more BaseModel context arguments from the previous step.
    """

    __name__: str

    async def __call__(
        self,
        instance: _PipelineBase,
        **kwargs: BaseModel,
    ) -> StepResult: ...


StepHandler: TypeAlias = _SyncStepHandler | _AsyncStepHandler


def _format_pipeline_name(cls: type) -> str:
    """Generate a default pipeline name based on the class name."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__).lower()


class _PipelineBaseMeta(type):
    """Metaclass that registers pipeline subclasses with their bound pipeline."""

    @classmethod
    def bind_pipeline(mcs, pipeline: Pipeline) -> type[_PipelineBase]:
        """Create a new PipelineBase class bound to the given pipeline.

        Args:
            pipeline: Pipeline instance to bind

        Returns:
            New PipelineBase class
        """
        return mcs(
            "PipelineBase",
            (_PipelineBase,),
            {"_pipeline": pipeline},
        )


class _PipelineBase(metaclass=_PipelineBaseMeta):
    """Base class for pipeline definitions."""

    _pipeline: ClassVar[Pipeline]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        # bind the pipeline only for subclasses, not the base itself
        if cls.__name__ == "PipelineBase":
            return

        cls._pipeline._bind_user_pipeline(cls)
        cls._pipeline._register_task()


class StepDefinition:
    """Definition of a pipeline step."""

    name: str
    order: Any
    handler: StepHandler
    return_type: type[BaseModel] | None
    param_types: dict[str, type[BaseModel] | None]
    _description: str | None = None

    def __init__(
        self,
        name: str,
        order: Any,
        handler: StepHandler,
        return_type: type[BaseModel] | None,
        param_types: dict[str, type[BaseModel] | None],
        description: str | None = None,
    ) -> None:
        self.name = name
        self.order = order
        self.handler = handler
        self.return_type = return_type
        self.param_types = param_types
        self._description = description

    @property
    def description(self) -> str:
        """Get the step description for activity tracking."""
        if self._description:
            return self._description
        return self.handler.__name__

    async def __call__(self, instance: _PipelineBase, **kwargs: BaseModel) -> StepResult:
        """Invoke the step handler."""
        if asyncio.iscoroutinefunction(self.handler):
            handler = cast(_AsyncStepHandler, self.handler)
            return await handler(instance, **kwargs)
        else:
            handler = cast(_SyncStepHandler, self.handler)
            return handler(instance, **kwargs)


class Pipeline:
    """Orchestrates multi-step task workflows.

    Pipelines are defined using a class-based, decorator-driven interface.
    Steps are methods decorated with @pipeline.step() that execute in order.

    Example:
        pipeline = ax.Pipeline(pool)

        class ResearchPipeline(pipeline.Base):
            @pipeline.step(0)
            async def research(self, ctx: InputContext) -> tuple[A, B]:
                a = await ax.enqueue("task_a", ctx)
                b = await ax.enqueue("task_b", ctx)
                return await ax.gather(a, b)

            @pipeline.step(1)
            async def analyze(self, a: A, b: B) -> Result:
                ...

        # Queue to worker (non-blocking)
        task = await pipeline.enqueue(context=InputContext(...))

        # Run inline (blocking)
        result = await pipeline.run(context=InputContext(...))
    """

    _pool: Pool | None
    _name: str | None
    _steps: dict[str, StepDefinition]
    _user_pipeline_class: type[_PipelineBase] | None = None
    _user_pipeline_instance: _PipelineBase | None = None

    def __init__(
        self,
        pool: Pool,
        *,
        name: str | None = None,
    ) -> None:
        """Initialize a pipeline.

        Args:
            pool: Pool instance for task registration and enqueueing.
            name: Optional name for task registration; defaults to class-based name.
        """
        self._steps = {}
        self._pool = pool
        self._name = name

    @property
    def name(self) -> str:
        """Pipeline name used for task registration."""
        if self._name:  # overridden name
            return self._name

        return _format_pipeline_name(self._get_user_pipeline_class())

    @property
    def Base(self) -> type[_PipelineBase]:
        """Returns a base class bound to this pipeline instance.

        When a class inherits from pipeline.Base, it is automatically
        registered as the pipeline's implementation class and as a task
        with the pool.
        """
        return _PipelineBaseMeta.bind_pipeline(self)

    def _bind_user_pipeline(self, cls: type[_PipelineBase]) -> None:
        """Manually set the pipeline implementation class.

        Args:
            cls: Pipeline implementation class
        """
        self._user_pipeline_class = cls

    def _register_task(self) -> None:
        """
        Register Task handler with the pipeline's worker pool
        """
        assert self._pool, "Pipeline must inherit from pipeline.Base"

        if not self._steps:
            raise RuntimeError(f"Pipeline '{self.name}' has no steps defined.")

        self._pool.add_task(
            name=self.name,
            func=self._run_task,
            context_type=self._input_type,
            result_type=self._output_type,
        )
        self._pool = None  # pool is only needed for registration

    def _get_user_pipeline_class(self) -> type[_PipelineBase]:
        """Get the pipeline implementation class.

        Returns:
            Pipeline implementation class

        Raises:
            RuntimeError: If no pipeline class has been defined
        """
        if self._user_pipeline_class is None:
            raise RuntimeError("Pipeline must inherit from pipeline.Base.")
        return self._user_pipeline_class

    def _get_user_pipeline_instance(self) -> _PipelineBase:
        """
        Instantiate a single instance of the pipeline class.
        Returns:
            Pipeline implementation instance
        """
        if self._user_pipeline_instance is None:
            self._user_pipeline_instance = self._get_user_pipeline_class()()
        return self._user_pipeline_instance

    @property
    def _sorted_steps(self) -> list[StepDefinition]:
        """Steps sorted by their order value."""
        return sorted(self._steps.values(), key=lambda s: s.order)

    @property
    def _input_type(self) -> type[BaseModel]:
        """Input context type for the first step."""
        first_step = self._sorted_steps[0]
        param_types = list(first_step.param_types.values())
        if not param_types:
            raise RuntimeError("First step must have at least one parameter for input context.")
        assert isinstance(param_types[0], type)  # TODO type checker
        return param_types[0]

    @property
    def _output_type(self) -> type[BaseModel] | None:
        """Output type from the final step."""
        last_step = self._sorted_steps[-1]
        return last_step.return_type

    def step(
        self,
        order: Any,
        description: str | None = None,
    ) -> Callable[[StepHandler], StepHandler]:
        """Decorator to register a method as a pipeline step.

        Args:
            order: Sortable value for step sequencing (0, 1, 2 or 'a', 'b', 'c')
            description: Optional description for activity tracking messages

        Returns:
            Decorated function
        """

        def decorator(func: StepHandler) -> StepHandler:
            hints = get_type_hints(func)
            sig = inspect.signature(func)
            param_types = {
                name: hints.get(name)
                for name, _ in sig.parameters.items()
                if name != "self"  #
            }

            self._steps[func.__name__] = StepDefinition(
                name=func.__name__,
                order=order,
                handler=func,
                return_type=hints.get("return"),
                param_types=param_types,
                description=description,
            )
            return func

        return decorator

    async def enqueue(
        self,
        context: BaseModel,
        metadata: dict[str, Any] | None = None,
    ) -> Task:
        """Enqueue the pipeline to run on a worker.

        Args:
            context: Initial context passed to the first step
            metadata: Optional dict of arbitrary metadata to attach to the activity.
                Useful for multi-tenancy (e.g., {"organization_id": "org-123"}).

        Returns:
            Task instance for tracking the pipeline execution
        """
        self._validate_type_flow()
        return await queue.enqueue(self.name, context, metadata=metadata)

    async def run(self, context: BaseModel) -> StepResult:
        """Execute the pipeline inline (blocking).

        Args:
            context: Initial context passed to the first step

        Returns:
            Output from the final step
        """
        self._validate_type_flow()
        _context: StepResult = context
        for step in self._sorted_steps:
            _context = await self._run_step(step, _context)

        return _context

    async def _run_task(
        self,
        *,
        agent_id: UUID,
        context: BaseModel,
    ) -> TaskResult:
        """Run the pipeline as a task handler.

        Args:
            agent_id: Agent ID for activity tracking
            context: Initial context passed to the first step
        Returns:
            Output from the final step
        """
        self._validate_type_flow()
        steps = self._sorted_steps
        total_steps = len(steps)

        _context: StepResult = context
        for i, step in enumerate(steps):
            activity.update(
                agent_id,
                f"Started {step.description}",
                percentage=int((i / total_steps) * 100),
            )
            _context = await self._run_step(step, _context)

        assert isinstance(_context, TaskResult), "Final step must return BaseModel"
        return _context

    async def _run_step(
        self,
        step: StepDefinition,
        context: StepResult,
    ) -> StepResult:
        """Run a single step of the pipeline.

        Args:
            step: StepDefinition to execute
            **kwargs: Parameters to pass to the step
        Returns:
            Output from the step
        """
        instance = self._get_user_pipeline_instance()

        param_names = list(step.param_types.keys())
        if isinstance(context, tuple):
            kwargs = dict(zip(param_names, context))
        elif param_names:
            kwargs = {param_names[0]: context}
        else:
            kwargs = {}

        return await step(instance, **kwargs)

    def _validate_type_flow(self) -> None:
        """Validate that all step types are BaseModel and flow correctly.

        Raises:
            TypeError: If types aren't BaseModel subclasses or don't connect properly
        """
        steps = self._sorted_steps
        assert steps, "Pipeline has no steps defined."  # caught on subclass init

        def _unpack_types(t: type) -> tuple[type, ...]:
            """Unpack type annotation, handling tuples."""
            return get_args(t) if get_origin(t) is tuple else (t,)

        def _is_base_model(t: type | None) -> bool:
            """Check if a single type is a BaseModel subclass."""
            return inspect.isclass(t) and issubclass(t, BaseModel)

        # Validate all types are BaseModel subclasses
        for step in steps:
            for name, ptype in step.param_types.items():
                if not _is_base_model(ptype):
                    raise TypeError(f"Step '{step.name}' parameter '{name}' must be BaseModel.")
            if step.return_type is None:  # TODO consdider supporting None return types
                raise TypeError(f"Step '{step.name}' must have a return type.")
            for rtype in _unpack_types(step.return_type):
                if not _is_base_model(rtype):
                    raise TypeError(f"'{step.name}' return type must be {StepResult}.")

        # Validate type flow between consecutive steps
        for in_step, out_step in zip(steps[:-1], steps[1:]):
            return_types = _unpack_types(in_step.return_type)
            param_types = list(out_step.param_types.values())

            if len(return_types) != len(param_types):
                raise TypeError(
                    f"'{in_step.name}' returns {len(return_types)} value(s), but "
                    f"'{out_step.name}' expects {len(param_types)} parameter(s)."
                )

            for ret_type, param_type in zip(return_types, param_types):
                if param_type and ret_type != param_type and not issubclass(ret_type, param_type):
                    raise TypeError(
                        f"'{in_step.name}' returns {ret_type.__name__}, but "
                        f"'{out_step.name}' expects {param_type.__name__}."
                    )

        # Final step must return a single BaseModel
        if not _is_base_model(steps[-1].return_type):
            raise TypeError(f"Final step '{steps[-1].name}' must return a BaseModel.")
