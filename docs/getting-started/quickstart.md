# Quick Start

Get up and running with agentexec in 5 minutes. This guide walks you through creating a simple AI agent task that runs in the background.

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

Create a `.env` file:

```bash
REDIS_URL=redis://localhost:6379/0
DATABASE_URL=sqlite:///agents.db
OPENAI_API_KEY=sk-your-key-here
```

## Step 2: Create Your Worker

Create a file called `worker.py`:

```python
# worker.py
import os
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import create_engine
from agents import Agent

import agentexec as ax

# Load environment variables
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///agents.db")

# Create database engine and tables
engine = create_engine(DATABASE_URL)
ax.Base.metadata.create_all(engine)


# Define your task context - what data your task needs
class SummarizeContext(BaseModel):
    text: str
    max_length: int = 100


# Create the worker pool
pool = ax.WorkerPool(engine=engine, database_url=DATABASE_URL)


# Register a task
@pool.task("summarize_text")
async def summarize_text(agent_id: UUID, context: SummarizeContext) -> str:
    """Summarize text using an AI agent."""

    # Create a runner for this task
    runner = ax.OpenAIRunner(agent_id=agent_id)

    # Create the agent
    agent = Agent(
        name="Summarizer",
        instructions=f"""You are a text summarizer.
        Summarize the given text in {context.max_length} words or less.
        Be concise and capture the key points.""",
        model="gpt-4o-mini",
    )

    # Run the agent
    result = await runner.run(
        agent,
        input=f"Please summarize this text:\n\n{context.text}",
        max_turns=5,
    )

    return result.final_output


# Entry point for running workers
if __name__ == "__main__":
    print(f"Starting {ax.CONF.num_workers} workers...")
    pool.run()  # Blocks and runs workers
```

## Step 3: Queue Tasks

Create a file called `queue_task.py` to queue tasks:

```python
# queue_task.py
import asyncio
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import agentexec as ax

# Import your context from worker
from worker import SummarizeContext, engine

# Sample text to summarize
SAMPLE_TEXT = """
Artificial intelligence (AI) is intelligence demonstrated by machines,
as opposed to natural intelligence displayed by animals including humans.
AI research has been defined as the field of study of intelligent agents,
which refers to any system that perceives its environment and takes actions
that maximize its chance of achieving its goals. The term "artificial
intelligence" had previously been used to describe machines that mimic and
display "human" cognitive skills that are associated with the human mind,
such as "learning" and "problem-solving".
"""


async def main():
    # Queue a task
    task = await ax.enqueue(
        "summarize_text",
        SummarizeContext(text=SAMPLE_TEXT, max_length=50),
    )

    print(f"Task queued!")
    print(f"Agent ID: {task.agent_id}")
    print(f"Task Name: {task.task_name}")

    # You can check the activity status
    with Session(engine) as session:
        activity = ax.activity.detail(session, task.agent_id)
        if activity:
            print(f"Status: {activity.status}")
            print(f"Created: {activity.created_at}")


if __name__ == "__main__":
    asyncio.run(main())
```

## Step 4: Run Everything

Open two terminal windows.

**Terminal 1 - Start the workers:**

```bash
uv run python worker.py
```

You should see output like:
```
Starting 4 workers...
[Worker 0] Started
[Worker 1] Started
[Worker 2] Started
[Worker 3] Started
```

**Terminal 2 - Queue a task:**

```bash
uv run python queue_task.py
```

You should see:
```
Task queued!
Agent ID: 550e8400-e29b-41d4-a716-446655440000
Task Name: summarize_text
Status: QUEUED
```

Back in Terminal 1, you'll see the task being processed:
```
[Worker 0] Processing task: summarize_text
[Worker 0] Task completed: summarize_text
```

## Step 5: Check Results

Create `check_status.py` to view task status and results:

```python
# check_status.py
import sys
from sqlalchemy.orm import Session
import agentexec as ax
from worker import engine


def check_activity(agent_id: str):
    with Session(engine) as session:
        activity = ax.activity.detail(session, agent_id)

        if not activity:
            print(f"No activity found for {agent_id}")
            return

        print(f"Activity: {activity.agent_id}")
        print(f"Task: {activity.agent_type}")
        print(f"Status: {activity.status}")
        print(f"Created: {activity.created_at}")
        print(f"Updated: {activity.updated_at}")
        print("\nLogs:")
        for log in activity.logs:
            print(f"  [{log.created_at}] {log.message}")
            if log.completion_percentage is not None:
                print(f"    Progress: {log.completion_percentage}%")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        # List recent activities
        with Session(engine) as session:
            activities = ax.activity.list(session, page=1, page_size=10)
            print(f"Recent activities ({activities.total} total):\n")
            for item in activities.items:
                print(f"  {item.agent_id} - {item.agent_type} - {item.status}")
    else:
        check_activity(sys.argv[1])
```

Run it:
```bash
# List all activities
uv run python check_status.py

# Check specific activity
uv run python check_status.py 550e8400-e29b-41d4-a716-446655440000
```

## Step 6: Add Progress Reporting

Enhance your agent to report progress. Update `worker.py`:

```python
@pool.task("summarize_text")
async def summarize_text(agent_id: UUID, context: SummarizeContext) -> str:
    """Summarize text using an AI agent with progress reporting."""

    runner = ax.OpenAIRunner(
        agent_id=agent_id,
        max_turns_recovery=True,  # Continue if max turns exceeded
    )

    agent = Agent(
        name="Summarizer",
        instructions=f"""You are a text summarizer.
        Summarize the given text in {context.max_length} words or less.
        Be concise and capture the key points.

        {runner.prompts.report_status}""",  # Add reporting instructions
        tools=[runner.tools.report_status],  # Add reporting tool
        model="gpt-4o-mini",
    )

    result = await runner.run(
        agent,
        input=f"Please summarize this text:\n\n{context.text}",
        max_turns=10,
    )

    return result.final_output
```

Now your agent can report its progress, which you can see in the activity logs!

## What's Next?

You've successfully created a background task system with AI agents. Here's what to explore next:

- [Configuration](configuration.md) - Customize workers, queues, and more
- [Basic Usage Guide](../guides/basic-usage.md) - Common patterns and best practices
- [FastAPI Integration](../guides/fastapi-integration.md) - Build a REST API
- [Pipelines](../guides/pipelines.md) - Orchestrate multi-step workflows
- [Architecture](../concepts/architecture.md) - Understand how agentexec works

## Complete Example

For a complete, production-ready example with FastAPI, see the [examples/openai-agents-fastapi](https://github.com/Agent-CI/agentexec/tree/main/examples/openai-agents-fastapi) directory in the repository.
