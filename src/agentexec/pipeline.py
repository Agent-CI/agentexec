from __future__ import annotations
import inspect
from dataclasses import dataclass
from typing import Any, Callable, TypeVar, get_type_hints, get_origin, get_args

from pydantic import BaseModel

from agentexec.worker import WorkerPool

StepHandler = TypeVar("StepHandler", bound=Callable[..., Any])


@dataclass
class StepDefinition:
    """Definition of a pipeline step."""

    name: str
    order: Any
    handler: StepHandler
    return_type: type | None
    param_types: dict[str, type | None]


class Pipeline:
    """Orchestrates multi-step task workflows.

    Pipelines are defined using a class-based, decorator-driven interface.
    Steps are methods decorated with @pipeline.step() that execute in order.

    Example:
        pipeline = ax.Pipeline(pool=pool)

        class ResearchPipeline(pipeline.Base):
            @pipeline.step(0, weight=0.4)
            async def research(self, ctx: InputContext) -> tuple[A, B]:
                a = await ax.enqueue("task_a", ctx)
                b = await ax.enqueue("task_b", ctx)
                return await ax.gather(a, b)

            @pipeline.step(1, weight=0.6)
            async def analyze(self, a: A, b: B) -> Result:
                ...

        result = await pipeline.run(context=InputContext(...))
    """

    _pool: WorkerPool
    _steps: dict[str, StepDefinition]
    _pipeline_class: type | None

    def __init__(self, pool: WorkerPool):
        """Initialize a pipeline.

        Args:
            pool: WorkerPool for task execution
        """
        self._pool = pool
        self._steps = {}
        self._pipeline_class = None

    @property
    def Base(self) -> type:
        """Dynamic base class for pipeline definitions.

        When a class inherits from pipeline.Base, it is automatically
        registered as the pipeline's implementation class.
        """
        pipeline = self

        class PipelineBase:
            def __init_subclass__(cls, **kwargs: Any) -> None:
                super().__init_subclass__(**kwargs)
                pipeline._pipeline_class = cls

        return PipelineBase

    def step(
        self,
        order: Any,
    ) -> Callable[[StepHandler], StepHandler]:
        """Decorator to register a method as a pipeline step.

        Args:
            order: Sortable value for step sequencing (0, 1, 2 or 'a', 'b', 'c')
            weight: Relative weight for progress tracking (0.0 to 1.0)

        Returns:
            Decorator function
        """

        def decorator(func: StepHandler) -> StepHandler:
            hints = get_type_hints(func)
            return_type = hints.pop("return", None)

            sig = inspect.signature(func)
            param_types = {
                name: hints.get(name)
                for name, param in sig.parameters.items()
                if name != "self"  #
            }

            self._steps[func.__name__] = StepDefinition(
                name=func.__name__,
                order=order,
                handler=func,
                return_type=return_type,
                param_types=param_types,
            )
            return func

        return decorator

    async def run(self, context: BaseModel) -> Any:
        """Execute the pipeline.

        Args:
            context: Initial context passed to the first step

        Returns:
            Output from the final step

        Raises:
            RuntimeError: If no pipeline class has been defined
            TypeError: If step input/output types don't match
        """
        if self._pipeline_class is None:
            raise RuntimeError(
                "No pipeline class defined. Create a class that inherits from pipeline.Base."
            )

        instance = self._pipeline_class()  # TODO reuse instance.
        sorted_steps = sorted(self._steps.values(), key=lambda s: s.order)

        # Verify type flow at runtime
        self._verify_type_flow(sorted_steps)

        # Execute steps in order
        current_output: Any = context
        for step in sorted_steps:
            # Unpack tuple into kwargs if needed
            if isinstance(current_output, tuple):
                param_names = list(step.param_types.keys())
                kwargs = dict(zip(param_names, current_output))
            else:
                param_names = list(step.param_types.keys())
                if param_names:
                    kwargs = {param_names[0]: current_output}
                else:
                    kwargs = {}

            current_output = await step.handler(instance, **kwargs)

        return current_output

    def _verify_type_flow(self, steps: list[StepDefinition]) -> None:
        """Verify that step return types match next step's parameters.

        Args:
            steps: Ordered list of step definitions

        Raises:
            TypeError: If types don't connect properly
        """
        for i, step in enumerate(steps[:-1]):
            next_step = steps[i + 1]

            if step.return_type is None:
                continue

            # Handle tuple return types
            origin = get_origin(step.return_type)
            if origin is tuple:
                return_types = get_args(step.return_type)
            else:
                return_types = (step.return_type,)

            param_types = list(next_step.param_types.values())

            # Check count matches
            if len(return_types) != len(param_types):
                raise TypeError(
                    f"Step '{step.name}' returns {len(return_types)} values, "
                    f"but step '{next_step.name}' expects {len(param_types)} parameters. "
                    f"Return: {return_types}, Params: {param_types}"
                )

            # Check types match
            for j, (ret_type, param_type) in enumerate(zip(return_types, param_types)):
                if param_type is not None and ret_type != param_type:
                    # Allow subclass relationships
                    if not (
                        inspect.isclass(ret_type)
                        and inspect.isclass(param_type)
                        and issubclass(ret_type, param_type)
                    ):
                        raise TypeError(
                            f"Type mismatch between step '{step.name}' and '{next_step.name}': "
                            f"return type {ret_type} doesn't match parameter type {param_type}"
                        )
