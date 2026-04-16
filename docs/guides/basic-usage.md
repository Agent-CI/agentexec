# Basic Usage Guide

This guide covers common patterns and best practices for using agentexec in
your applications.

## Project Structure

A typical agentexec project:

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
└── README.md
```

## Defining Contexts

Contexts are Pydantic models that define what data your task needs:

```python
# contexts.py
from pydantic import BaseModel, Field

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

from sqlalchemy.ext.asyncio import create_async_engine
from agents import Agent
import agentexec as ax

from .contexts import ResearchContext
from .db import DATABASE_URL

engine = create_async_engine(DATABASE_URL)
pool = ax.Pool(engine=engine)

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
        max_turns=15,
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

**Handle errors and reflect them in activity state:**

```python
@pool.task("api_task")
async def api_task(agent_id: UUID, context: APIContext) -> dict:
    try:
        result = await call_external_api(context.endpoint)
        return {"success": True, "data": result}
    except APIError as e:
        await ax.activity.error(agent_id, f"API error: {e.message}")
        return {"success": False, "error": str(e)}
```

**Use timeouts:**

```python
import asyncio

@pool.task("slow_task")
async def slow_task(agent_id: UUID, context: SlowContext) -> dict:
    try:
        return await asyncio.wait_for(
            slow_operation(),
            timeout=context.timeout_seconds,
        )
    except asyncio.TimeoutError:
        await ax.activity.update(agent_id, "Operation timed out, using fallback")
        return await fallback_operation()
```

## Enqueueing Tasks

### From Application Code

```python
import agentexec as ax
from .contexts import ResearchContext

async def start_research(company: str) -> str:
    task = await ax.enqueue(
        "research_company",
        ResearchContext(company=company, focus_areas=["products", "financials"]),
    )
    return str(task.agent_id)
```

### With Priority

```python
urgent_task = await ax.enqueue(
    "process_order",
    OrderContext(order_id="123"),
    priority=ax.Priority.HIGH,
)

background_task = await ax.enqueue(
    "generate_report",
    ReportContext(report_type="weekly"),
)
```

### With Metadata

Attach arbitrary key-value pairs to the activity record (useful for
multi-tenancy and filtering):

```python
task = await ax.enqueue(
    "send_email",
    EmailContext(to="user@example.com", subject="Hello"),
    metadata={"organization_id": "org-123"},
)
```

> **Prerequisite:** `ax.enqueue()` writes an activity record, so the
> process calling it needs an engine configured. `Pool.__init__` configures
> the engine on your behalf — any module that imports your worker module
> (like an API server or a producer script) picks this up for free. If you
> need to enqueue without importing the worker, call
> `agentexec.core.db.configure_engine(engine)` explicitly.

## Tracking Progress

### Basic Status Check

```python
import agentexec as ax

async def get_task_status(agent_id: str) -> dict:
    activity = await ax.activity.detail(agent_id=agent_id)
    if not activity:
        return {"error": "Task not found"}

    latest = activity.logs[-1] if activity.logs else None
    return {
        "status": latest.status.value if latest else None,
        "progress": latest.percentage if latest else None,
        "updated_at": activity.updated_at.isoformat(),
    }
```

### Polling for Completion

Prefer `ax.get_result()` for tasks that return a Pydantic result — it polls
Redis directly and is cheaper than reading the activity table:

```python
task = await ax.enqueue("research_company", ResearchContext(...))
result = await ax.get_result(task, timeout=300)
```

If you need the activity logs (not just the result), poll `activity.detail`:

```python
import asyncio
from agentexec.activity import Status

async def wait_for_completion(agent_id: str, timeout: int = 300) -> dict:
    start = asyncio.get_event_loop().time()
    terminal = {Status.COMPLETE, Status.ERROR, Status.CANCELED}

    while asyncio.get_event_loop().time() - start < timeout:
        activity = await ax.activity.detail(agent_id=agent_id)
        if activity and activity.logs and activity.logs[-1].status in terminal:
            return {
                "status": activity.logs[-1].status.value,
                "logs": [log.message for log in activity.logs],
            }
        await asyncio.sleep(2)

    return {"error": "Timeout waiting for task"}
```

## Running Workers

### Using the CLI (recommended)

```bash
uv run agentexec run myapp.worker:pool --create-tables
```

Key flags:

- `--create-tables` — run `Base.metadata.create_all` before starting (dev only)
- `--workers N`, `-w N` — override `CONF.num_workers`
- `--max-retries N` — override `CONF.max_task_retries`
- `--log-level LEVEL` — default `INFO`

### Programmatic Entry Point

If you need more control than the CLI provides:

```python
# worker.py (extension)
import asyncio
import agentexec as ax

# ... define pool and tasks as above ...

async def main():
    async with engine.begin() as conn:
        await conn.run_sync(ax.Base.metadata.create_all)
    await pool.start()

if __name__ == "__main__":
    asyncio.run(main())
```

## Database Setup

The preferred method for managing database tables is **Alembic migrations**.
This gives you version-controlled, reversible migrations that work well in
production. For quick prototyping, `--create-tables` (or
`Base.metadata.create_all`) is the simpler path.

### Recommended: Alembic Migrations

Alembic provides proper migration management for production applications.

**1. Initialize Alembic:**

```bash
uv add alembic
alembic init alembic
```

**2. Configure `alembic/env.py` to include agentexec models.** Alembic
typically runs synchronously, so for autogenerate you can either use a sync
URL for migrations or use Alembic's async template. A simple synchronous
setup pointed at the same database:

```python
# alembic/env.py
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from myapp.models import Base as AppBase
import agentexec as ax

config = context.config

# Migrations use a sync driver even when the app uses async.
# For SQLite: sqlite:///agents.db   (not sqlite+aiosqlite://)
# For Postgres: postgresql+psycopg://...   (not postgresql+asyncpg://)
DATABASE_URL = os.getenv("MIGRATIONS_DATABASE_URL", "sqlite:///agents.db")
config.set_main_option("sqlalchemy.url", DATABASE_URL)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

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
alembic revision --autogenerate -m "Add agentexec tables"
alembic upgrade head
```

See the
[examples/openai-agents-fastapi](https://github.com/Agent-CI/agentexec/tree/main/examples/openai-agents-fastapi)
directory for a complete Alembic setup.

### Quick Start: create_all()

For prototyping, let the CLI create tables on startup:

```bash
uv run agentexec run myapp.worker:pool --create-tables
```

Or do it inline before `pool.start()`:

```python
async with engine.begin() as conn:
    await conn.run_sync(ax.Base.metadata.create_all)
```

> **Note:** `create_all()` only creates tables that don't exist. It won't
> update existing tables when agentexec is upgraded. For production, use
> Alembic migrations.

## Next Steps

- [FastAPI Integration](fastapi-integration.md) — Build REST APIs
- [Pipelines](pipelines.md) — Multi-step workflows
- [OpenAI Runner](openai-runner.md) — Advanced agent configuration
