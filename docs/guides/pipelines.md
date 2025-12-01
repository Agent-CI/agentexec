# Pipelines

Pipelines enable multi-step workflow orchestration, where the output of one step feeds into the next. This is ideal for complex AI agent workflows that require multiple stages of processing.

## Overview

A pipeline defines a series of steps that execute in order:

```
Step 0          Step 1          Step 2
┌─────────┐     ┌─────────┐     ┌─────────┐
│Research │────>│ Analyze │────>│  Report │
│ Company │     │  Data   │     │ Generate│
└─────────┘     └─────────┘     └─────────┘
     │               │               │
     ▼               ▼               ▼
 Task A          Task B          Task C
 Task B          (waits for      (waits for
 (parallel)       A & B)           B)
```

## Creating a Pipeline

### Basic Pipeline

```python
from pydantic import BaseModel
import agentexec as ax
from myapp.worker import pool

# Define input context
class CompanyResearchInput(BaseModel):
    company: str
    depth: str = "comprehensive"

# Create pipeline
pipeline = ax.Pipeline(pool=pool)

class CompanyResearchPipeline(pipeline.Base):
    """Multi-step company research pipeline."""

    @pipeline.step(0)
    async def research(self, ctx: CompanyResearchInput):
        """Step 0: Initial research."""
        task = await ax.enqueue("research_company", ResearchContext(
            company=ctx.company,
            focus_areas=["overview", "products"]
        ))
        return await ax.get_result(task.agent_id)

    @pipeline.step(1)
    async def analyze(self, research_result: dict):
        """Step 1: Analyze research findings."""
        task = await ax.enqueue("analyze_data", AnalysisContext(
            data=research_result["findings"]
        ))
        return await ax.get_result(task.agent_id)

    @pipeline.step(2)
    async def generate_report(self, analysis_result: dict):
        """Step 2: Generate final report."""
        task = await ax.enqueue("generate_report", ReportContext(
            analysis=analysis_result
        ))
        return await ax.get_result(task.agent_id)

# Run the pipeline
result = await pipeline.run(context=CompanyResearchInput(company="Acme Corp"))
print(result)  # Final report
```

### Parallel Tasks in a Step

Use `gather()` to run multiple tasks in parallel within a step:

```python
class ParallelResearchPipeline(pipeline.Base):

    @pipeline.step(0)
    async def parallel_research(self, ctx: CompanyResearchInput):
        """Run multiple research tasks in parallel."""
        # Queue multiple tasks
        overview_task = await ax.enqueue("research_overview", OverviewContext(
            company=ctx.company
        ))
        financials_task = await ax.enqueue("research_financials", FinancialsContext(
            company=ctx.company
        ))
        competitors_task = await ax.enqueue("research_competitors", CompetitorsContext(
            company=ctx.company
        ))

        # Wait for all to complete
        overview, financials, competitors = await ax.gather(
            overview_task,
            financials_task,
            competitors_task
        )

        return {"overview": overview, "financials": financials, "competitors": competitors}

    @pipeline.step(1)
    async def synthesize(self, research_results: dict):
        """Combine all research into a single report."""
        task = await ax.enqueue("synthesize", SynthesizeContext(
            overview=research_results["overview"],
            financials=research_results["financials"],
            competitors=research_results["competitors"]
        ))
        return await ax.get_result(task.agent_id)
```

### Unpacking Results

When a step returns a tuple, the next step receives unpacked arguments:

```python
class UnpackingPipeline(pipeline.Base):

    @pipeline.step(0)
    async def fetch_data(self, ctx: InputContext):
        """Return multiple values."""
        task1 = await ax.enqueue("fetch_users", ctx)
        task2 = await ax.enqueue("fetch_orders", ctx)

        users, orders = await ax.gather(task1, task2)
        return users, orders  # Tuple return

    @pipeline.step(1)
    async def process(self, users: list, orders: list):
        """Receive unpacked values as separate arguments."""
        # users and orders are unpacked from the tuple
        task = await ax.enqueue("process_data", ProcessContext(
            user_count=len(users),
            order_count=len(orders)
        ))
        return await ax.get_result(task.agent_id)
```

## Step Configuration

### Step Order

Steps are executed in order based on their step number:

```python
@pipeline.step(0)  # Runs first
async def first_step(self, ctx):
    ...

@pipeline.step(1)  # Runs second
async def second_step(self, result):
    ...

@pipeline.step(2)  # Runs third
async def third_step(self, result):
    ...
```

You can also use string identifiers:

```python
@pipeline.step("fetch")
async def fetch(self, ctx):
    ...

@pipeline.step("process")
async def process(self, data):
    ...

@pipeline.step("finalize")
async def finalize(self, result):
    ...
```

### Type Annotations

Use type annotations for better IDE support and documentation:

```python
@pipeline.step(0)
async def research(self, ctx: CompanyResearchInput) -> dict:
    """Input is the pipeline context."""
    ...

@pipeline.step(1)
async def analyze(self, research_data: dict) -> AnalysisResult:
    """Input is the output of the previous step."""
    ...

@pipeline.step(2)
async def report(self, analysis: AnalysisResult) -> str:
    """Input is the output of the previous step."""
    ...
```

## Helper Functions

### gather()

Wait for multiple tasks to complete:

```python
# Queue multiple tasks
task1 = await ax.enqueue("task_a", ContextA(...))
task2 = await ax.enqueue("task_b", ContextB(...))
task3 = await ax.enqueue("task_c", ContextC(...))

# Wait for all to complete (returns tuple)
result_a, result_b, result_c = await ax.gather(task1, task2, task3)
```

### get_result()

Wait for a single task result:

```python
task = await ax.enqueue("my_task", MyContext(...))

# Wait up to 300 seconds (default)
result = await ax.get_result(task.agent_id)

# Custom timeout
result = await ax.get_result(task.agent_id, timeout=60)
```

## Error Handling

### Step-Level Errors

If a step fails, the pipeline stops:

```python
class ErrorHandlingPipeline(pipeline.Base):

    @pipeline.step(0)
    async def risky_step(self, ctx: InputContext):
        try:
            task = await ax.enqueue("risky_task", ctx)
            return await ax.get_result(task.agent_id)
        except Exception as e:
            # Log error, return fallback, or re-raise
            return {"error": str(e), "fallback": True}

    @pipeline.step(1)
    async def handle_result(self, result: dict):
        if result.get("fallback"):
            # Handle fallback case
            return await self.fallback_processing(result)
        return await self.normal_processing(result)
```

### Task-Level Errors

Check task status before using results:

```python
@pipeline.step(0)
async def check_status(self, ctx: InputContext):
    task = await ax.enqueue("my_task", ctx)

    # Wait for result
    try:
        result = await ax.get_result(task.agent_id, timeout=120)
        return {"success": True, "data": result}
    except TimeoutError:
        return {"success": False, "error": "Task timed out"}
```

### Partial Failure Handling

Handle partial failures in parallel tasks:

```python
@pipeline.step(0)
async def parallel_with_fallback(self, ctx: InputContext):
    tasks = [
        await ax.enqueue("task_a", ctx),
        await ax.enqueue("task_b", ctx),
        await ax.enqueue("task_c", ctx),
    ]

    results = []
    for task in tasks:
        try:
            result = await ax.get_result(task.agent_id, timeout=60)
            results.append({"success": True, "data": result})
        except Exception as e:
            results.append({"success": False, "error": str(e)})

    return results
```

## Real-World Examples

### Research and Report Pipeline

```python
from pydantic import BaseModel
from typing import Optional
import agentexec as ax

class ResearchInput(BaseModel):
    topic: str
    depth: str = "standard"
    include_sources: bool = True

pipeline = ax.Pipeline(pool=pool)

class ResearchReportPipeline(pipeline.Base):
    """Complete research and report generation pipeline."""

    @pipeline.step(0)
    async def gather_sources(self, ctx: ResearchInput):
        """Step 0: Find relevant sources."""
        task = await ax.enqueue("find_sources", FindSourcesContext(
            topic=ctx.topic,
            max_sources=10 if ctx.depth == "standard" else 25
        ))
        return await ax.get_result(task.agent_id)

    @pipeline.step(1)
    async def analyze_sources(self, sources: list):
        """Step 1: Analyze each source in parallel."""
        tasks = []
        for source in sources:
            task = await ax.enqueue("analyze_source", AnalyzeSourceContext(
                url=source["url"],
                content=source["content"]
            ))
            tasks.append(task)

        return await ax.gather(*tasks)

    @pipeline.step(2)
    async def synthesize(self, analyses: tuple):
        """Step 2: Synthesize all analyses."""
        task = await ax.enqueue("synthesize", SynthesizeContext(
            analyses=list(analyses)
        ))
        return await ax.get_result(task.agent_id)

    @pipeline.step(3)
    async def generate_report(self, synthesis: dict):
        """Step 3: Generate final report."""
        task = await ax.enqueue("generate_report", GenerateReportContext(
            synthesis=synthesis,
            format="markdown"
        ))
        return await ax.get_result(task.agent_id)

# Usage
report = await pipeline.run(context=ResearchInput(
    topic="AI Safety Research",
    depth="comprehensive"
))
```

### Data Processing Pipeline

```python
class DataInput(BaseModel):
    source_id: str
    transformations: list[str]
    output_format: str = "json"

class DataProcessingPipeline(pipeline.Base):
    """ETL-style data processing pipeline."""

    @pipeline.step("extract")
    async def extract(self, ctx: DataInput):
        """Extract data from source."""
        task = await ax.enqueue("extract_data", ExtractContext(
            source_id=ctx.source_id
        ))
        raw_data = await ax.get_result(task.agent_id)
        return raw_data, ctx.transformations

    @pipeline.step("transform")
    async def transform(self, raw_data: dict, transformations: list):
        """Apply transformations sequentially."""
        data = raw_data

        for transform in transformations:
            task = await ax.enqueue("apply_transform", TransformContext(
                data=data,
                transform_type=transform
            ))
            data = await ax.get_result(task.agent_id)

        return data

    @pipeline.step("load")
    async def load(self, transformed_data: dict):
        """Load data to destination."""
        task = await ax.enqueue("load_data", LoadContext(
            data=transformed_data
        ))
        return await ax.get_result(task.agent_id)
```

### Agent Collaboration Pipeline

```python
class CollaborationInput(BaseModel):
    project_brief: str
    team_size: int = 3

class AgentCollaborationPipeline(pipeline.Base):
    """Multiple agents collaborate on a project."""

    @pipeline.step(0)
    async def brainstorm(self, ctx: CollaborationInput):
        """Multiple agents brainstorm ideas."""
        tasks = []
        for i in range(ctx.team_size):
            task = await ax.enqueue("brainstorm_agent", BrainstormContext(
                brief=ctx.project_brief,
                agent_persona=f"Creative Agent {i+1}"
            ))
            tasks.append(task)

        ideas = await ax.gather(*tasks)
        return list(ideas)

    @pipeline.step(1)
    async def evaluate(self, ideas: list):
        """Critic agent evaluates all ideas."""
        task = await ax.enqueue("critic_agent", CriticContext(
            ideas=ideas
        ))
        return await ax.get_result(task.agent_id)

    @pipeline.step(2)
    async def refine(self, evaluation: dict):
        """Refine the best ideas."""
        best_ideas = evaluation["top_ideas"]

        tasks = []
        for idea in best_ideas:
            task = await ax.enqueue("refine_agent", RefineContext(
                idea=idea,
                feedback=evaluation["feedback"].get(idea["id"])
            ))
            tasks.append(task)

        return await ax.gather(*tasks)

    @pipeline.step(3)
    async def finalize(self, refined_ideas: tuple):
        """Create final proposal."""
        task = await ax.enqueue("proposal_agent", ProposalContext(
            refined_ideas=list(refined_ideas)
        ))
        return await ax.get_result(task.agent_id)
```

## Best Practices

### 1. Keep Steps Focused

Each step should have a single responsibility:

```python
# Good - focused steps
@pipeline.step(0)
async def fetch_data(self, ctx):
    ...

@pipeline.step(1)
async def validate_data(self, data):
    ...

@pipeline.step(2)
async def transform_data(self, validated_data):
    ...

# Bad - step does too much
@pipeline.step(0)
async def do_everything(self, ctx):
    data = fetch()
    validated = validate(data)
    return transform(validated)
```

### 2. Use Meaningful Step Names

```python
# Good
@pipeline.step("gather_sources")
@pipeline.step("analyze_content")
@pipeline.step("generate_summary")

# Bad
@pipeline.step(0)
@pipeline.step(1)
@pipeline.step(2)
```

### 3. Handle Timeouts Appropriately

```python
@pipeline.step(0)
async def long_running_step(self, ctx):
    task = await ax.enqueue("slow_task", ctx)

    # Use appropriate timeout
    try:
        return await ax.get_result(task.agent_id, timeout=600)  # 10 minutes
    except TimeoutError:
        # Handle timeout gracefully
        return {"error": "Task timed out", "partial_result": None}
```

### 4. Document Step Dependencies

```python
class MyPipeline(pipeline.Base):
    """
    Pipeline Flow:
    1. fetch_data -> returns raw data
    2. validate -> receives raw data, returns validated data
    3. transform -> receives validated data, returns transformed data
    4. save -> receives transformed data, returns save confirmation
    """

    @pipeline.step(0)
    async def fetch_data(self, ctx: InputContext) -> dict:
        """Fetches data from source. Output: raw data dict."""
        ...
```

## Debugging Pipelines

### Logging

Add logging to track pipeline execution:

```python
import logging

logger = logging.getLogger(__name__)

class DebugPipeline(pipeline.Base):

    @pipeline.step(0)
    async def step_one(self, ctx):
        logger.info(f"Starting step_one with context: {ctx}")
        result = await self._do_step_one(ctx)
        logger.info(f"Completed step_one with result: {result}")
        return result
```

### Progress Tracking

Track overall pipeline progress:

```python
class TrackedPipeline(pipeline.Base):

    def __init__(self, tracking_id: str):
        self.tracking_id = tracking_id

    @pipeline.step(0)
    async def step_one(self, ctx):
        ax.activity.update(self.tracking_id, "Pipeline: Starting step 1", 0)
        result = await self._do_step_one(ctx)
        ax.activity.update(self.tracking_id, "Pipeline: Step 1 complete", 33)
        return result

    @pipeline.step(1)
    async def step_two(self, data):
        ax.activity.update(self.tracking_id, "Pipeline: Starting step 2", 33)
        result = await self._do_step_two(data)
        ax.activity.update(self.tracking_id, "Pipeline: Step 2 complete", 66)
        return result

    @pipeline.step(2)
    async def step_three(self, data):
        ax.activity.update(self.tracking_id, "Pipeline: Starting step 3", 66)
        result = await self._do_step_three(data)
        ax.activity.update(self.tracking_id, "Pipeline: Complete", 100)
        return result
```

## Next Steps

- [OpenAI Runner](openai-runner.md) - Configure agent runners
- [Basic Usage](basic-usage.md) - Common patterns
- [API Reference](../api-reference/pipeline.md) - Pipeline API details
