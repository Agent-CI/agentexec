# Quick Start with OpenAI Agents SDK

Get up and running with agentexec in 5 minutes. This guide walks you through
creating a simple AI agent task that runs in the background.

## Prerequisites

Before starting, ensure you have:
- [uv](https://docs.astral.sh/uv/) installed ([Installation Guide](installation.md))
- Redis running locally
- An OpenAI API key (if using the OpenAI runner)

## Step 1: Set Up Your Environment

Create a new project directory and set up your environment:

```bash
# Create a new project with uv
uv init my-agent-project
cd my-agent-project

# Add dependencies
uv add agentexec openai-agents
```

Create a `.env` file. Note that agentexec uses async database drivers —
`sqlite+aiosqlite://` or `postgresql+asyncpg://`:

```bash
REDIS_URL=redis://localhost:6379/0
DATABASE_URL=sqlite+aiosqlite:///agents.db
OPENAI_API_KEY=sk-your-key-here
```

## Step 2: Create Your Worker

Create a file called `worker.py`. Keep the module body free of side effects
(no `asyncio.run()`, no `create_all()`, no `enqueue()` calls) — it is
imported both by your running workers and by any producer (API server,
CLI script) that enqueues tasks.

```python
# worker.py
import os
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine
from agents import Agent
import agentexec as ax

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///agents.db")

# Async engine. Pool.__init__ calls configure_engine() so any caller
# that imports this module can use ax.enqueue().
engine = create_async_engine(DATABASE_URL)
pool = ax.Pool(engine=engine)


class SummarizeContext(BaseModel):
    text: str
    max_length: int = 100


class SummarizeResult(BaseModel):
    summary: str


@pool.task("summarize_text")
async def summarize_text(agent_id: UUID, context: SummarizeContext) -> SummarizeResult:
    """Summarize text using an AI agent."""
    runner = ax.OpenAIRunner(agent_id=agent_id)

    agent = Agent(
        name="Summarizer",
        instructions=(
            "You are a text summarizer.\n"
            f"Summarize the given text in {context.max_length} words or less.\n"
            f"Be concise and capture the key points.\n"
            f"{runner.prompts.report_status}"
        ),
        tools=[runner.tools.report_status],
        model="gpt-5",
        output_type=SummarizeResult,
    )

    result = await runner.run(
        agent,
        input=f"Please summarize this text:\n\n{context.text}",
        max_turns=5,
    )

    return result.final_output_as(SummarizeResult)
```

## Step 3: Queue Tasks

Create `queue_task.py`:

```python
# queue_task.py
import asyncio
import agentexec as ax

# Importing `worker` configures the engine as a side effect of creating
# the Pool. If you don't want to import the worker module (e.g. in a
# minimal client), call configure_engine() explicitly.
from worker import SummarizeContext

SAMPLE_TEXT = """
Artificial intelligence (AI) is intelligence demonstrated by machines,
as opposed to natural intelligence displayed by animals including humans.
AI research has been defined as the field of study of intelligent agents,
which refers to any system that perceives its environment and takes actions
that maximize its chance of achieving its goals.
"""


async def main():
    task = await ax.enqueue(
        "summarize_text",
        SummarizeContext(text=SAMPLE_TEXT, max_length=50),
    )

    print(f"Task queued!")
    print(f"Agent ID: {task.agent_id}")
    print(f"Task Name: {task.task_name}")

    activity = await ax.activity.detail(agent_id=task.agent_id)
    if activity:
        print(f"Task type: {activity.agent_type}")
        print(f"Created: {activity.created_at}")


if __name__ == "__main__":
    asyncio.run(main())
```

## Step 4: Run Everything

Open two terminal windows.

**Terminal 1 — start the workers** (the CLI imports your module, creates
the tables, and runs the pool):

```bash
uv run agentexec run worker:pool --create-tables
```

You should see:
```
[INFO/MainProcess] agentexec.worker.pool: Starting 4 worker processes
[INFO/MainProcess] agentexec.worker.pool: Started worker 0 (PID: ...)
[INFO/SpawnProcess-1] agentexec.worker.pool: Worker 0 starting
...
```

**Terminal 2 — queue a task:**

```bash
uv run python queue_task.py
```

You should see:
```
Task queued!
Agent ID: 550e8400-e29b-41d4-a716-446655440000
Task Name: summarize_text
```

Back in Terminal 1, you'll see the task being processed:
```
[INFO/SpawnProcess-1] agentexec.worker.pool: Worker 0 processing: summarize_text
[INFO/SpawnProcess-1] agentexec.worker.pool: Worker 0 completed: summarize_text
```

## Step 5: Check Results

Create `check_status.py` to view task status and logs:

```python
# check_status.py
import asyncio
import sys
import agentexec as ax

# Importing `worker` configures the engine.
import worker  # noqa: F401


async def check_activity(agent_id: str):
    activity = await ax.activity.detail(agent_id=agent_id)

    if not activity:
        print(f"No activity found for {agent_id}")
        return

    print(f"Activity: {activity.agent_id}")
    print(f"Task: {activity.agent_type}")
    print(f"Created: {activity.created_at}")
    print(f"Updated: {activity.updated_at}")
    print("\nLogs:")
    for log in activity.logs:
        print(f"  [{log.created_at}] {log.status} {log.message}")
        if log.percentage is not None:
            print(f"    Progress: {log.percentage}%")


async def list_recent():
    result = await ax.activity.list(page=1, page_size=10)
    print(f"Recent activities ({result.total} total):\n")
    for item in result.items:
        print(f"  {item.agent_id} - {item.agent_type} - {item.status}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        asyncio.run(list_recent())
    else:
        asyncio.run(check_activity(sys.argv[1]))
```

Run it:

```bash
# List recent activities
uv run python check_status.py

# Check a specific activity
uv run python check_status.py 550e8400-e29b-41d4-a716-446655440000
```

## What's Next?

You've successfully created a background task system with AI agents. Here's
what to explore next:

- [Configuration](configuration.md) — customize workers, queues, and more
- [Basic Usage Guide](../guides/basic-usage.md) — common patterns
- [FastAPI Integration](../guides/fastapi-integration.md) — build a REST API
- [Pipelines](../guides/pipelines.md) — orchestrate multi-step workflows
- [Architecture](../concepts/architecture.md) — understand how agentexec works

> **Note on Database Migrations:** This quickstart uses `--create-tables`
> for simplicity. For production, use [Alembic](https://alembic.sqlalchemy.org/).
> See the [Basic Usage Guide](../guides/basic-usage.md#database-setup).

## Complete Example

For a complete, production-ready example with FastAPI and Alembic
migrations, see the
[examples/openai-agents-fastapi](https://github.com/Agent-CI/agentexec/tree/main/examples/openai-agents-fastapi)
directory in the repository.
