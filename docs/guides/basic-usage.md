# Basic Usage Guide

This guide covers common patterns and best practices for using agentexec in your applications.

## Project Structure

A typical agentexec project structure:

```
my-agent-project/
├── pyproject.toml          # Project dependencies
├── .env                    # Environment variables
├── src/
│   └── myapp/
│       ├── __init__.py
│       ├── worker.py       # Worker pool and task definitions
│       ├── contexts.py     # Pydantic context models
│       ├── agents.py       # Agent definitions
│       └── db.py           # Database setup
├── tests/
│   └── test_tasks.py
└── README.md
```

## Defining Contexts

Contexts are Pydantic models that define what data your task needs:

```python
# contexts.py
from pydantic import BaseModel, Field
from typing import Optional

class ResearchContext(BaseModel):
    """Context for research tasks."""
    company: str = Field(..., description="Company name to research")
    focus_areas: list[str] = Field(default_factory=list)
    max_sources: int = Field(default=10, ge=1, le=100)

class AnalysisContext(BaseModel):
    """Context for analysis tasks."""
    data_id: str
    analysis_type: str = "comprehensive"
    include_charts: bool = False

class EmailContext(BaseModel):
    """Context for email tasks."""
    to: str
    subject: str
    body: str
    attachments: list[str] = Field(default_factory=list)
```

### Context Best Practices

**Use Field for validation and documentation:**

```python
class OrderContext(BaseModel):
    order_id: str = Field(..., min_length=1)
    quantity: int = Field(..., ge=1, le=1000)
    priority: str = Field(default="normal", pattern="^(low|normal|high)$")
```

**Keep contexts focused:**

```python
# Good - focused context
class SearchContext(BaseModel):
    query: str
    max_results: int = 10

# Bad - context does too much
class EverythingContext(BaseModel):
    query: str
    email_to: str
    file_path: str
    database_id: str
```

**Use references instead of large data:**

```python
# Good - reference to data
class ProcessContext(BaseModel):
    document_id: str  # Fetch document in handler

# Bad - embedding large data
class ProcessContext(BaseModel):
    document_content: str  # Could be megabytes
```

## Defining Tasks

Tasks are async functions decorated with `@pool.task()`:

```python
# worker.py
from uuid import UUID
import agentexec as ax
from agents import Agent

from .contexts import ResearchContext
from .db import engine, DATABASE_URL

pool = ax.WorkerPool(engine=engine, database_url=DATABASE_URL)

@pool.task("research_company")
async def research_company(agent_id: UUID, context: ResearchContext) -> dict:
    """Research a company and return findings."""

    runner = ax.OpenAIRunner(agent_id=agent_id)

    agent = Agent(
        name="Research Agent",
        instructions=f"""Research {context.company}.
        Focus on: {', '.join(context.focus_areas) or 'general information'}
        {runner.prompts.report_status}""",
        tools=[runner.tools.report_status],
        model="gpt-4o",
    )

    result = await runner.run(
        agent,
        input=f"Research {context.company}",
        max_turns=15
    )

    return {"company": context.company, "findings": result.final_output}
```

### Task Best Practices

**Keep tasks focused:**

```python
# Good - single responsibility
@pool.task("validate_order")
async def validate_order(agent_id: UUID, context: OrderContext) -> bool:
    ...

@pool.task("process_payment")
async def process_payment(agent_id: UUID, context: PaymentContext) -> str:
    ...

# Bad - task does too much
@pool.task("handle_order")
async def handle_order(agent_id: UUID, context: OrderContext):
    validate_order()
    process_payment()
    send_confirmation()
    update_inventory()
```

**Handle errors explicitly:**

```python
@pool.task("api_task")
async def api_task(agent_id: UUID, context: APIContext) -> dict:
    try:
        result = await call_external_api(context.endpoint)
        return {"success": True, "data": result}
    except APIError as e:
        ax.activity.error(agent_id, f"API error: {e.message}")
        return {"success": False, "error": str(e)}
```

**Use timeouts:**

```python
import asyncio

@pool.task("slow_task")
async def slow_task(agent_id: UUID, context: SlowContext) -> dict:
    try:
        result = await asyncio.wait_for(
            slow_operation(),
            timeout=context.timeout_seconds
        )
        return result
    except asyncio.TimeoutError:
        ax.activity.update(agent_id, "Operation timed out, using fallback")
        return await fallback_operation()
```

## Enqueueing Tasks

### From Application Code

```python
import asyncio
import agentexec as ax
from .contexts import ResearchContext

async def start_research(company: str) -> str:
    """Queue a research task and return the agent ID."""
    task = await ax.enqueue(
        "research_company",
        ResearchContext(company=company, focus_areas=["products", "financials"])
    )
    return str(task.agent_id)
```

### With Priority

```python
from agentexec import Priority

# High priority - processed first
urgent_task = await ax.enqueue(
    "process_order",
    OrderContext(order_id="123"),
    priority=Priority.HIGH
)

# Low priority (default) - processed after high priority
background_task = await ax.enqueue(
    "generate_report",
    ReportContext(report_type="weekly")
)
```

### To Specific Queue

```python
# Enqueue to a specific queue
task = await ax.enqueue(
    "send_email",
    EmailContext(to="user@example.com", subject="Hello"),
    queue_name="email_queue"
)
```

## Tracking Progress

### Basic Status Check

```python
from sqlalchemy.orm import Session

async def get_task_status(agent_id: str) -> dict:
    with Session(engine) as session:
        activity = ax.activity.detail(session, agent_id)

        if not activity:
            return {"error": "Task not found"}

        return {
            "status": activity.status.value,
            "progress": activity.latest_completion_percentage,
            "updated_at": activity.updated_at.isoformat(),
        }
```

### Polling for Completion

```python
import asyncio

async def wait_for_completion(agent_id: str, timeout: int = 300) -> dict:
    """Wait for a task to complete, checking every 2 seconds."""
    start = asyncio.get_event_loop().time()

    while True:
        with Session(engine) as session:
            activity = ax.activity.detail(session, agent_id)

            if activity and activity.status in ("COMPLETE", "ERROR"):
                return {
                    "status": activity.status.value,
                    "logs": [log.message for log in activity.logs]
                }

        if asyncio.get_event_loop().time() - start > timeout:
            return {"error": "Timeout waiting for task"}

        await asyncio.sleep(2)
```

### Getting Results

```python
# Get result for pipeline coordination
result = await ax.get_result(agent_id, timeout=300)
print(result)  # {"company": "Acme", "findings": "..."}
```

## Running Workers

### Standalone Worker Process

```python
# worker.py
if __name__ == "__main__":
    pool.run()  # Blocks until shutdown
```

Run with:
```bash
uv run python -m myapp.worker
```

### With Custom Configuration

```python
import os

if __name__ == "__main__":
    # Override configuration
    os.environ["AGENTEXEC_NUM_WORKERS"] = "8"

    print(f"Starting {ax.CONF.num_workers} workers")
    pool.run()
```

### Multiple Worker Types

Run different workers for different task types:

```python
# email_worker.py
email_pool = ax.WorkerPool(
    engine=engine,
    database_url=DATABASE_URL,
    queue_name="email_queue"
)

@email_pool.task("send_email")
async def send_email(agent_id: UUID, context: EmailContext):
    ...

if __name__ == "__main__":
    email_pool.run()
```

```python
# research_worker.py
research_pool = ax.WorkerPool(
    engine=engine,
    database_url=DATABASE_URL,
    queue_name="research_queue"
)

@research_pool.task("research_company")
async def research_company(agent_id: UUID, context: ResearchContext):
    ...

if __name__ == "__main__":
    research_pool.run()
```

## Database Setup

The preferred method for managing database tables is **Alembic migrations**. This gives you version-controlled, reversible migrations that work well in production. For quick prototyping, you can use SQLAlchemy's `create_all()` as a simple fallback.

### Recommended: Alembic Migrations

Alembic provides proper migration management for production applications.

**1. Initialize Alembic in your project:**

```bash
uv add alembic
alembic init alembic
```

**2. Configure `alembic/env.py` to include agentexec models:**

```python
# alembic/env.py
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# Import your application's models
from myapp.models import Base as AppBase

# Import agentexec
import agentexec as ax

config = context.config

# Get database URL from environment
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///agents.db")
config.set_main_option("sqlalchemy.url", DATABASE_URL)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Combine metadata from both your app and agentexec
# This allows Alembic to manage tables from both sources
target_metadata = [AppBase.metadata, ax.Base.metadata]


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

**3. Generate and run migrations:**

```bash
# Generate migration for agentexec tables
alembic revision --autogenerate -m "Add agentexec tables"

# Apply migrations
alembic upgrade head
```

See the [examples/openai-agents-fastapi](https://github.com/Agent-CI/agentexec/tree/main/examples/openai-agents-fastapi) directory for a complete Alembic setup.

### Quick Start: SQLAlchemy create_all()

For quick prototyping or simple scripts, you can use `create_all()`:

```python
# db.py
from sqlalchemy import create_engine
import agentexec as ax

DATABASE_URL = "sqlite:///agents.db"
engine = create_engine(DATABASE_URL)

# Create tables (simple approach for getting started)
ax.Base.metadata.create_all(engine)
```

> **Note**: `create_all()` only creates tables that don't exist. It won't update existing tables when agentexec is upgraded. For production, use Alembic migrations.

## Error Handling Patterns

### Retry with Backoff

```python
from tenacity import retry, stop_after_attempt, wait_exponential

@pool.task("retry_task")
async def retry_task(agent_id: UUID, context: MyContext):
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30)
    )
    async def do_work():
        ax.activity.update(agent_id, "Attempting operation...")
        return await unreliable_operation()

    try:
        return await do_work()
    except Exception as e:
        ax.activity.error(agent_id, f"Failed after 3 retries: {e}")
        raise
```

### Partial Failure Handling

```python
@pool.task("batch_process")
async def batch_process(agent_id: UUID, context: BatchContext):
    results = []
    errors = []

    for i, item in enumerate(context.items):
        try:
            result = await process_item(item)
            results.append(result)
        except Exception as e:
            errors.append({"item": item, "error": str(e)})

        progress = int((i + 1) / len(context.items) * 100)
        ax.activity.update(agent_id, f"Processed {i+1}/{len(context.items)}", progress)

    return {
        "successful": len(results),
        "failed": len(errors),
        "errors": errors
    }
```

### Graceful Degradation

```python
@pool.task("enhanced_research")
async def enhanced_research(agent_id: UUID, context: ResearchContext):
    # Try primary source
    try:
        ax.activity.update(agent_id, "Querying primary data source...")
        data = await primary_api.fetch(context.company)
    except APIError:
        ax.activity.update(agent_id, "Primary source unavailable, using fallback...")
        data = await fallback_api.fetch(context.company)

    return await analyze(data)
```

## Testing Tasks

### Unit Testing

```python
# test_tasks.py
import pytest
from uuid import uuid4
from unittest.mock import patch, AsyncMock

from myapp.worker import research_company
from myapp.contexts import ResearchContext

@pytest.mark.asyncio
async def test_research_company():
    agent_id = uuid4()
    context = ResearchContext(company="Test Corp")

    with patch("myapp.worker.ax.OpenAIRunner") as mock_runner:
        mock_runner.return_value.run = AsyncMock(
            return_value=AsyncMock(final_output="Test findings")
        )

        result = await research_company(agent_id, context)

        assert result["company"] == "Test Corp"
        assert "findings" in result
```

### Integration Testing

```python
import pytest
import asyncio
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import agentexec as ax

@pytest.fixture
def test_engine():
    engine = create_engine("sqlite:///:memory:")
    ax.Base.metadata.create_all(engine)
    return engine

@pytest.mark.asyncio
async def test_task_lifecycle(test_engine):
    # Queue task
    task = await ax.enqueue("test_task", TestContext(data="test"))

    # Check initial status
    with Session(test_engine) as session:
        activity = ax.activity.detail(session, task.agent_id)
        assert activity.status == "QUEUED"
```

## Common Patterns

### Request-Response with Webhooks

```python
@pool.task("process_with_callback")
async def process_with_callback(agent_id: UUID, context: CallbackContext):
    result = await do_processing()

    # Send result to callback URL
    if context.callback_url:
        await httpx.post(context.callback_url, json={
            "agent_id": str(agent_id),
            "result": result
        })

    return result
```

### Scheduled Tasks

```python
# Use with a scheduler like APScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()

@scheduler.scheduled_job('cron', hour=0)  # Run at midnight
async def daily_report():
    await ax.enqueue("generate_daily_report", ReportContext())

scheduler.start()
```

### Task Chaining

```python
@pool.task("step_1")
async def step_1(agent_id: UUID, context: Step1Context) -> str:
    result = await do_step_1()

    # Queue next step
    await ax.enqueue("step_2", Step2Context(
        previous_result=result,
        original_request_id=context.request_id
    ))

    return result

@pool.task("step_2")
async def step_2(agent_id: UUID, context: Step2Context) -> str:
    return await do_step_2(context.previous_result)
```

## Next Steps

- [FastAPI Integration](fastapi-integration.md) - Build REST APIs
- [Pipelines](pipelines.md) - Multi-step workflows
- [OpenAI Runner](openai-runner.md) - Advanced agent configuration
