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
from pydantic import BaseModel, Field
import agentexec as ax
from myapp.worker import pool


# Define typed models for inputs and outputs
class CompanyResearchInput(BaseModel):
    """Initial input to the pipeline."""

    company: str = Field(..., description="Company name to research")
    depth: str = "comprehensive"


class ResearchResult(BaseModel):
    """Output from research step."""

    company: str
    findings: list[str]
    sources: list[str] = Field(default_factory=list)


class AnalysisResult(BaseModel):
    """Output from analysis step."""

    company: str
    insights: list[str]
    sentiment: str


class FinalReport(BaseModel):
    """Final pipeline output."""

    company: str
    executive_summary: str
    key_findings: list[str]
    recommendation: str


# Create pipeline
pipeline = ax.Pipeline()


class CompanyResearchPipeline(pipeline.Base):
    """Multi-step company research pipeline."""

    @pipeline.step(0)
    async def research(self, ctx: CompanyResearchInput) -> ResearchResult:
        """Step 0: Initial research."""
        task = await ax.enqueue("research_company", ResearchContext(
            company=ctx.company,
            focus_areas=["overview", "products"],
        ))
        return await ax.get_result(task)

    @pipeline.step(1)
    async def analyze(self, research: ResearchResult) -> AnalysisResult:
        """Step 1: Analyze research findings."""
        task = await ax.enqueue("analyze_data", AnalysisContext(
            company=research.company,
            data=research.findings,
        ))
        return await ax.get_result(task)

    @pipeline.step(2)
    async def generate_report(self, analysis: AnalysisResult) -> FinalReport:
        """Step 2: Generate final report."""
        task = await ax.enqueue("generate_report", ReportContext(
            company=analysis.company,
            insights=analysis.insights,
        ))
        return await ax.get_result(task)


# Run the pipeline
result: FinalReport = await pipeline.run(
    context=CompanyResearchInput(company="Acme Corp"),
)
print(result.executive_summary)
```

### Parallel Tasks in a Step

Use `gather()` to run multiple tasks in parallel within a step:

```python
from pydantic import BaseModel, Field


class CompanyInput(BaseModel):
    """Pipeline input."""

    company_name: str


class BrandResearchResult(BaseModel):
    """Result from brand research."""

    company_name: str
    website_url: str | None = None
    founding_year: int | None = None
    brand_summary: str


class MarketResearchResult(BaseModel):
    """Result from market research."""

    company_name: str
    market_size: str | None = None
    competitors: list[str] = Field(default_factory=list)
    market_summary: str


class ProductResearchResult(BaseModel):
    """Result from product research."""

    company_name: str
    products: list[str] = Field(default_factory=list)
    product_summary: str


class CompanyReport(BaseModel):
    """Aggregated company research."""

    company_name: str
    brand: BrandResearchResult
    market: MarketResearchResult
    products: ProductResearchResult


class ParallelResearchPipeline(pipeline.Base):
    """Pipeline with parallel research tasks."""

    @pipeline.step(0)
    async def parallel_research(
        self,
        ctx: CompanyInput,
    ) -> tuple[BrandResearchResult, MarketResearchResult, ProductResearchResult]:
        """Run multiple research tasks in parallel."""
        brand_task = await ax.enqueue(
            "brand_research",
            CompanyInput(company_name=ctx.company_name),
        )
        market_task = await ax.enqueue(
            "market_research",
            CompanyInput(company_name=ctx.company_name),
        )
        product_task = await ax.enqueue(
            "product_research",
            CompanyInput(company_name=ctx.company_name),
        )

        # Wait for all to complete - returns tuple
        return await ax.gather(brand_task, market_task, product_task)

    @pipeline.step(1)
    async def aggregate_results(
        self,
        brand: BrandResearchResult,
        market: MarketResearchResult,
        products: ProductResearchResult,
    ) -> CompanyReport:
        """Aggregate research results into a report.

        This runs locally (no task queue) since it's just data transformation.
        """
        return CompanyReport(
            company_name=brand.company_name,
            brand=brand,
            market=market,
            products=products,
        )
```

### Unpacking Results

When a step returns a tuple, the next step receives unpacked arguments:

```python
class UserData(BaseModel):
    """User data result."""

    users: list[str]
    total: int


class OrderData(BaseModel):
    """Order data result."""

    orders: list[str]
    total: int


class ProcessedData(BaseModel):
    """Processed result."""

    user_count: int
    order_count: int
    summary: str


class UnpackingPipeline(pipeline.Base):

    @pipeline.step(0)
    async def fetch_data(
        self,
        ctx: InputContext,
    ) -> tuple[UserData, OrderData]:
        """Return multiple values as a tuple."""
        task1 = await ax.enqueue("fetch_users", ctx)
        task2 = await ax.enqueue("fetch_orders", ctx)

        # Returns tuple of (UserData, OrderData)
        return await ax.gather(task1, task2)

    @pipeline.step(1)
    async def process(
        self,
        users: UserData,
        orders: OrderData,
    ) -> ProcessedData:
        """Receive unpacked values as separate arguments."""
        task = await ax.enqueue("process_data", ProcessContext(
            user_count=users.total,
            order_count=orders.total,
        ))
        return await ax.get_result(task)
```

## Step Configuration

### Step Order

Steps are sorted and executed using Python's built-in `sorted()` function on the `order` argument. This means you can use either numeric or string values, and they will be sorted according to Python's standard sorting rules.

**Numeric ordering:**

```python
@pipeline.step(0)  # Runs first
async def first_step(self, ctx: InputContext) -> FirstResult:
    ...

@pipeline.step(1)  # Runs second
async def second_step(self, result: FirstResult) -> SecondResult:
    ...

@pipeline.step(2)  # Runs third
async def third_step(self, result: SecondResult) -> FinalResult:
    ...
```

**Alphabetical string ordering:**

```python
@pipeline.step("a_fetch")      # Runs first (alphabetically first)
async def fetch(self, ctx: InputContext) -> RawData:
    ...

@pipeline.step("b_process")    # Runs second
async def process(self, data: RawData) -> ProcessedData:
    ...

@pipeline.step("c_finalize")   # Runs third
async def finalize(self, result: ProcessedData) -> FinalResult:
    ...
```

**How sorting works:**

Since Python's `sorted()` is used, the order follows standard Python comparison rules:

```python
# Numeric: sorted by value
sorted([2, 0, 1])  # → [0, 1, 2]

# Strings: sorted alphabetically
sorted(["process", "fetch", "save"])  # → ["fetch", "process", "save"]

# Mixed types will raise TypeError - don't mix integers and strings!
```

> **Important**: Don't mix integers and strings as step order values in the same pipeline, as Python's `sorted()` cannot compare them and will raise a `TypeError`.

### Type Annotations

Use Pydantic models for inputs and outputs to get:

- **Type safety**: Validation at runtime
- **IDE support**: Autocomplete and type hints
- **Documentation**: Self-documenting pipelines

```python
class ResearchInput(BaseModel):
    topic: str
    max_sources: int = 10


class ResearchOutput(BaseModel):
    findings: list[str]
    sources: list[str]


class AnalysisOutput(BaseModel):
    insights: list[str]
    confidence: float


class FinalReport(BaseModel):
    summary: str
    recommendations: list[str]


@pipeline.step(0)
async def research(self, ctx: ResearchInput) -> ResearchOutput:
    """Input is the pipeline context."""
    ...

@pipeline.step(1)
async def analyze(self, research: ResearchOutput) -> AnalysisOutput:
    """Input is the output of the previous step."""
    ...

@pipeline.step(2)
async def report(self, analysis: AnalysisOutput) -> FinalReport:
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
result = await ax.get_result(task)

# Custom timeout
result = await ax.get_result(task, timeout=60)
```

## Best Practices

### 1. Define Typed Models

Always use Pydantic models for inputs and outputs:

```python
# Good - typed models
class ResearchInput(BaseModel):
    topic: str
    max_sources: int = 10


class ResearchOutput(BaseModel):
    findings: list[str]
    sources: list[str]


@pipeline.step(0)
async def research(self, ctx: ResearchInput) -> ResearchOutput:
    ...


# Bad - untyped dicts
@pipeline.step(0)
async def research(self, ctx: dict) -> dict:
    ...
```

### 2. Keep Steps Focused

Each step should have a single responsibility:

```python
# Good - focused steps
@pipeline.step(0)
async def fetch_data(self, ctx: FetchInput) -> RawData:
    ...

@pipeline.step(1)
async def validate_data(self, data: RawData) -> ValidatedData:
    ...

@pipeline.step(2)
async def transform_data(self, data: ValidatedData) -> TransformedData:
    ...


# Bad - step does too much
@pipeline.step(0)
async def do_everything(self, ctx: InputContext) -> FinalResult:
    data = await fetch()
    validated = validate(data)
    return transform(validated)
```

### 3. Use Descriptive Step Names

```python
# Good - descriptive string names
@pipeline.step("gather_sources")
async def gather_sources(self, ctx: Input) -> Sources:
    ...

@pipeline.step("analyze_content")
async def analyze_content(self, sources: Sources) -> Analysis:
    ...

@pipeline.step("generate_summary")
async def generate_summary(self, analysis: Analysis) -> Summary:
    ...
```

### 4. Handle Timeouts Appropriately

```python
class TimeoutResult(BaseModel):
    success: bool
    data: dict | None = None
    error: str | None = None


@pipeline.step(0)
async def long_running_step(self, ctx: InputContext) -> TimeoutResult:
    task = await ax.enqueue("slow_task", ctx)

    try:
        data = await ax.get_result(task, timeout=600)  # 10 minutes
        return TimeoutResult(success=True, data=data)
    except TimeoutError:
        return TimeoutResult(success=False, error="Task timed out")
```

### 5. Document Step Dependencies

```python
class MyPipeline(pipeline.Base):
    """
    Pipeline Flow:
    1. fetch_data(InputContext) -> RawData
    2. validate(RawData) -> ValidatedData
    3. transform(ValidatedData) -> TransformedData
    4. save(TransformedData) -> SaveResult
    """

    @pipeline.step(0)
    async def fetch_data(self, ctx: InputContext) -> RawData:
        """Fetches data from source."""
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
    async def step_one(self, ctx: InputContext) -> StepOneResult:
        logger.info(f"Starting step_one with context: {ctx}")
        result = await self._do_step_one(ctx)
        logger.info(f"Completed step_one with result: {result}")
        return result
```

### Progress Tracking

Track overall pipeline progress via activity updates:

```python
class TrackedPipeline(pipeline.Base):

    def __init__(self, tracking_id: str):
        self.tracking_id = tracking_id

    @pipeline.step(0)
    async def step_one(self, ctx: InputContext) -> StepOneResult:
        ax.activity.update(self.tracking_id, "Pipeline: Starting step 1", 0)
        result = await self._do_step_one(ctx)
        ax.activity.update(self.tracking_id, "Pipeline: Step 1 complete", 33)
        return result

    @pipeline.step(1)
    async def step_two(self, data: StepOneResult) -> StepTwoResult:
        ax.activity.update(self.tracking_id, "Pipeline: Starting step 2", 33)
        result = await self._do_step_two(data)
        ax.activity.update(self.tracking_id, "Pipeline: Step 2 complete", 66)
        return result

    @pipeline.step(2)
    async def step_three(self, data: StepTwoResult) -> FinalResult:
        ax.activity.update(self.tracking_id, "Pipeline: Starting step 3", 66)
        result = await self._do_step_three(data)
        ax.activity.update(self.tracking_id, "Pipeline: Complete", 100)
        return result
```

## Next Steps

- [OpenAI Runner](openai-runner.md) - Configure agent runners
- [Basic Usage](basic-usage.md) - Common patterns
- [API Reference](../api-reference/pipeline.md) - Pipeline API details
