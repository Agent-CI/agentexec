# Pipeline API Reference

This document covers the Pipeline API for multi-step workflow orchestration.

## Pipeline

Orchestrates multi-step task workflows.

```python
class Pipeline:
    def __init__(self, pool: Pool)
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `pool` | `Pool` | Worker pool for task registration and enqueueing |

### Example

```python
import agentexec as ax

pool = ax.Pool(database_url="sqlite:///agents.db")
pipeline = ax.Pipeline(pool)

class MyPipeline(pipeline.Base):
    @pipeline.step(0)
    async def first_step(self, ctx: InputContext):
        ...

    @pipeline.step(1)
    async def second_step(self, result):
        ...

# Queue to worker (non-blocking)
task = await pipeline.enqueue(context=InputContext(...))

# Run inline (blocking)
result = await pipeline.run(context=InputContext(...))
```

---

## Pipeline.Base

Base class for pipeline definitions.

```python
pipeline.Base: type
```

Subclass this to define your pipeline steps.

### Example

```python
class ResearchPipeline(pipeline.Base):
    """Pipeline for research tasks."""

    @pipeline.step(0)
    async def gather(self, ctx: InputContext):
        """Gather data from sources."""
        ...

    @pipeline.step(1)
    async def analyze(self, data):
        """Analyze gathered data."""
        ...
```

---

## @pipeline.step()

Decorator to define a pipeline step.

```python
def step(self, order: int | str, description: str | None = None) -> Callable
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `order` | `int \| str` | Step execution order (sorted using Python's `sorted()`) |
| `description` | `str \| None` | Optional description for activity tracking messages |

### Behavior

- Steps are sorted using Python's built-in `sorted()` function on the `order` value
- Steps execute in sorted order (numeric or alphabetical)
- Each step receives the return value of the previous step
- Tuple returns are unpacked as separate arguments

### Sorting Rules

The `order` parameter is sorted using Python's standard `sorted()` function:

```python
# Numeric values: sorted by value
sorted([2, 0, 1])  # → [0, 1, 2]

# String values: sorted alphabetically
sorted(["c_save", "a_fetch", "b_process"])  # → ["a_fetch", "b_process", "c_save"]
```

> **Warning**: Do not mix integers and strings in the same pipeline, as `sorted()` cannot compare them.

### Example

```python
# Numeric ordering with activity descriptions
@pipeline.step(0, "initial data gathering")  # Runs first
async def first(self, ctx: InputContext):
    return "result"

@pipeline.step(1, "result processing")  # Runs second
async def second(self, previous_result: str):
    return previous_result.upper()

# Alphabetical string ordering
@pipeline.step("a_fetch", "data fetch")
async def fetch(self, ctx):
    ...

@pipeline.step("b_process", "data processing")
async def process(self, data):
    ...

@pipeline.step("c_finalize", "finalization")
async def finalize(self, result):
    ...
```

---

## pipeline.enqueue()

Queue the pipeline to run on a worker.

```python
async def enqueue(self, context: BaseModel) -> Task
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `context` | `BaseModel` | Input context for the first step |

### Returns

`Task` - Task instance for tracking the pipeline execution.

### Example

```python
# Queue pipeline to run on a worker
task = await pipeline.enqueue(context=InputContext(
    query="Research AI safety",
    depth="comprehensive"
))

# Wait for result
result = await ax.get_result(task)
```

---

## pipeline.run()

Execute the pipeline inline (blocking).

```python
async def run(self, agent_id: str | UUID | None, context: BaseModel) -> Any
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `agent_id` | `str \| UUID \| None` | Agent ID for activity tracking (None to skip tracking) |
| `context` | `BaseModel` | Input context for the first step |

### Returns

`Any` - The return value of the final step.

### Example

```python
# Run pipeline inline without activity tracking
result = await pipeline.run(None, InputContext(
    query="Research AI safety",
    depth="comprehensive"
))

# Run pipeline with activity tracking
result = await pipeline.run(agent_id, InputContext(
    query="Research AI safety",
    depth="comprehensive"
))
```

---

## Step Parameter Passing

### Single Value

```python
@pipeline.step(0)
async def step_one(self, ctx: InputContext):
    return "single value"

@pipeline.step(1)
async def step_two(self, value: str):
    # Receives "single value"
    ...
```

### Tuple Unpacking

```python
@pipeline.step(0)
async def step_one(self, ctx: InputContext):
    return "value1", "value2", "value3"  # Tuple

@pipeline.step(1)
async def step_two(self, v1: str, v2: str, v3: str):
    # Receives unpacked values
    ...
```

### Dict Passing

```python
@pipeline.step(0)
async def step_one(self, ctx: InputContext):
    return {"key1": "value1", "key2": "value2"}

@pipeline.step(1)
async def step_two(self, data: dict):
    # Receives the dict as-is
    print(data["key1"])
```

---

## gather()

Wait for multiple tasks to complete.

```python
async def gather(*tasks: Task) -> tuple[Any, ...]
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `*tasks` | `Task` | Variable number of Task instances |

### Returns

`tuple[Any, ...]` - Results from each task in order.

### Example

```python
@pipeline.step(0)
async def parallel_tasks(self, ctx: InputContext):
    # Queue multiple tasks
    task1 = await ax.enqueue("task_a", ContextA(...))
    task2 = await ax.enqueue("task_b", ContextB(...))
    task3 = await ax.enqueue("task_c", ContextC(...))

    # Wait for all
    return await ax.gather(task1, task2, task3)

@pipeline.step(1)
async def process(self, result_a, result_b, result_c):
    # Tuple was unpacked
    combined = f"{result_a} {result_b} {result_c}"
    ...
```

---

## get_result()

Wait for a single task result.

```python
async def get_result(
    agent_id: str | UUID,
    timeout: int = 300
) -> Any
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `agent_id` | `str \| UUID` | required | Task identifier |
| `timeout` | `int` | `300` | Maximum seconds to wait |

### Returns

`Any` - The task's return value.

### Raises

- `TimeoutError` - Task didn't complete within timeout
- `KeyError` - Result not found

### Example

```python
@pipeline.step(0)
async def start_task(self, ctx: InputContext):
    task = await ax.enqueue("long_task", ctx)
    # Wait up to 10 minutes
    return await ax.get_result(task, timeout=600)
```

---

## Complete Pipeline Example

```python
from pydantic import BaseModel
import agentexec as ax

# Input context
class ResearchInput(BaseModel):
    company: str
    depth: str = "standard"

# Task contexts
class GatherContext(BaseModel):
    company: str
    sources: int

class AnalyzeContext(BaseModel):
    data: dict

class ReportContext(BaseModel):
    analysis: dict
    format: str

# Create pool and pipeline
pool = ax.Pool(database_url="sqlite:///agents.db")
pipeline = ax.Pipeline(pool)

class ResearchPipeline(pipeline.Base):
    """Multi-step company research pipeline."""

    @pipeline.step(0, "data gathering")
    async def gather_data(self, ctx: ResearchInput):
        """Step 0: Gather data from multiple sources in parallel."""
        sources = 5 if ctx.depth == "standard" else 15

        # Queue parallel tasks
        web_task = await ax.enqueue("search_web", GatherContext(
            company=ctx.company,
            sources=sources
        ))
        news_task = await ax.enqueue("search_news", GatherContext(
            company=ctx.company,
            sources=sources
        ))
        financial_task = await ax.enqueue("search_financials", GatherContext(
            company=ctx.company,
            sources=sources
        ))

        # Wait for all
        web, news, financial = await ax.gather(web_task, news_task, financial_task)

        return {
            "web": web,
            "news": news,
            "financial": financial
        }

    @pipeline.step(1, "data analysis")
    async def analyze(self, data: dict):
        """Step 1: Analyze gathered data."""
        task = await ax.enqueue("analyze_data", AnalyzeContext(data=data))
        return await ax.get_result(task)

    @pipeline.step(2, "report generation")
    async def generate_report(self, analysis: dict):
        """Step 2: Generate final report."""
        task = await ax.enqueue("generate_report", ReportContext(
            analysis=analysis,
            format="markdown"
        ))
        return await ax.get_result(task)

# Usage
async def main():
    ctx = ResearchInput(company="Acme Corp", depth="comprehensive")

    # Option 1: Run inline (blocking, no activity tracking)
    report = await pipeline.run(None, ctx)
    print(report)

    # Option 2: Queue to worker (non-blocking, activity tracked automatically)
    task = await pipeline.enqueue(context=ctx)
    report = await ax.get_result(task)
    print(report)
```

---

## Error Handling

### Step Errors

If a step raises an exception, the pipeline stops:

```python
@pipeline.step(0)
async def risky_step(self, ctx: InputContext):
    task = await ax.enqueue("risky_task", ctx)
    try:
        return await ax.get_result(task, timeout=60)
    except TimeoutError:
        # Return fallback or raise
        return {"error": "timeout", "fallback": True}

@pipeline.step(1)
async def handle_result(self, result: dict):
    if result.get("fallback"):
        return await self.fallback_logic(result)
    return await self.normal_logic(result)
```

### Task Errors

Check task status in results:

```python
@pipeline.step(0)
async def parallel_with_errors(self, ctx: InputContext):
    tasks = [
        await ax.enqueue("task_a", ctx),
        await ax.enqueue("task_b", ctx),
        await ax.enqueue("task_c", ctx),
    ]

    results = []
    for task in tasks:
        try:
            result = await ax.get_result(task, timeout=60)
            results.append({"success": True, "data": result})
        except Exception as e:
            results.append({"success": False, "error": str(e)})

    return results
```

---

## Pipeline Patterns

### Conditional Steps

```python
@pipeline.step(1)
async def conditional(self, data: dict):
    if data.get("needs_review"):
        task = await ax.enqueue("review_task", ReviewContext(data=data))
        return await ax.get_result(task)
    return data  # Skip review
```

### Fan-Out/Fan-In

```python
@pipeline.step(0)
async def fan_out(self, ctx: InputContext):
    """Process items in parallel."""
    tasks = []
    for item in ctx.items:
        task = await ax.enqueue("process_item", ItemContext(item=item))
        tasks.append(task)

    return await ax.gather(*tasks)

@pipeline.step(1)
async def fan_in(self, *results):
    """Combine all results."""
    task = await ax.enqueue("combine", CombineContext(results=list(results)))
    return await ax.get_result(task)
```

### Progress Tracking

```python
class TrackedPipeline(pipeline.Base):
    def __init__(self, tracking_id: str):
        self.tracking_id = tracking_id

    @pipeline.step(0)
    async def step_one(self, ctx):
        ax.activity.update(self.tracking_id, "Pipeline: Step 1/3", 0)
        result = await self._do_step_one(ctx)
        ax.activity.update(self.tracking_id, "Pipeline: Step 1 complete", 33)
        return result

    @pipeline.step(1)
    async def step_two(self, data):
        ax.activity.update(self.tracking_id, "Pipeline: Step 2/3", 33)
        result = await self._do_step_two(data)
        ax.activity.update(self.tracking_id, "Pipeline: Step 2 complete", 66)
        return result

    @pipeline.step(2)
    async def step_three(self, data):
        ax.activity.update(self.tracking_id, "Pipeline: Step 3/3", 66)
        result = await self._do_step_three(data)
        ax.activity.complete(self.tracking_id, "Pipeline complete", 100)
        return result
```
