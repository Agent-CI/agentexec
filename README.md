# `agentexec`

**Production-ready orchestration for OpenAI Agents SDK** with Redis-backed task queues, SQLAlchemy activity tracking, and multiprocessing worker pools.

Build reliable, scalable AI agent applications with automatic lifecycle management, progress tracking, and fault tolerance.

Running AI agents in production requires more than just the SDK. You need:

- **Background execution** - Agents can take minutes to complete; users shouldn't wait
- **Progress tracking** - Know what your agents are doing and when they finish
- **Fault tolerance** - Handle failures gracefully with automatic error tracking
- **Scalability** - Process multiple agent tasks concurrently across worker processes
- **Observability** - Full audit trail of agent activities and status updates

`agentexec` provides all of this out of the box, with a simple API that integrates seamlessly with the OpenAI Agents SDK (and the extensibility to continue adding support for other frameworks).

---

## Features

- **Multi-process worker pool** - True parallelism for concurrent agent execution
- **Redis task queue** - Reliable job distribution with priority support
- **Automatic activity tracking** - Full lifecycle management (QUEUED → RUNNING → COMPLETE/ERROR)
- **OpenAI Agents integration** - Drop-in runner with max turns recovery
- **Agent self-reporting** - Built-in tools for agents to report progress
- **SQLAlchemy-based storage** - Flexible database support (PostgreSQL, MySQL, SQLite)
- **Type-safe** - Full type annotations with Pydantic schemas
- **Production-ready** - Graceful shutdown, error handling, configurable timeouts

---

## Installation

```bash
uv add agentexec
```

**Requirements:**
- Python 3.11+
- Redis (for task queue)
- SQLAlchemy-compatible database (for activity tracking)
- Agents that you want to parallelize!

---

## Quick Start

### 1. Set Up Your Worker

```python
from uuid import UUID

from agents import Agent
from pydantic import BaseModel
from sqlalchemy import create_engine

import agentexec as ax


# Define typed context for your task
class ResearchContext(BaseModel):
    company: str


# Database for activity tracking (share with your app)
engine = create_engine("sqlite:///agents.db")

# Create worker pool
pool = ax.WorkerPool(engine=engine)


@pool.task("research_company")
async def research_company(agent_id: UUID, context: ResearchContext) -> None:
    """Background task that runs an AI agent."""
    runner = ax.OpenAIRunner(
        agent_id=agent_id,
        max_turns_recovery=True,
    )

    agent = Agent(
        name="Research Agent",
        instructions=(
            f"Research {context.company}.\n"  # Typed access!
            "\n"
            f"{runner.prompts.report_status}"
        ),
        tools=[
            runner.tools.report_status,
        ],
        model="gpt-4o",
    )

    result = await runner.run(
        agent,
        input="Start research",
        max_turns=15,
    )
    print(f"Done! {result.final_output}")


if __name__ == "__main__":
    pool.start()  # Start workers
```

### 2. Queue Tasks from Your Application

```python
import agentexec as ax
from pydantic import BaseModel


class ResearchContext(BaseModel):
    company: str


# Enqueue a task (from your async API handler, etc.)
task = await ax.enqueue(
    "research_company",
    ResearchContext(company="Anthropic"),
)

print(f"Task queued: {task.agent_id}")
```

### 3. Track Progress

```python
with Session(engine) as db:
    # list recent activities
    activities = ax.activity.list(db, page=1, page_size=10)
    for activity in activities:
        print(f"Agent {activity.agent_id} - Status: {activity.status}")

    # get activity with full log history
    activity = ax.activity.detail(db, agent_id=task.agent_id)
    print(f"Activity for {activity.agent_id}:")
    for log in activity.logs:
        print(f" - {log.created_at}: {log.message} ({log.status})")
```

---

## What You Get

### Automatic Activity Tracking

Every task gets full lifecycle tracking without manual updates:

```python
runner = ax.OpenAIRunner(agent_id=agent_id)
result = await runner.run(agent, input="...")

# Activity automatically transitions:
# QUEUED → RUNNING → COMPLETE (or ERROR on failure)
```

### Agent Self-Reporting

Agents can report their own progress using a built-in tool:

```python
agent = Agent(
    instructions=f"Do research. {runner.prompts.report_status}",
    tools=[runner.tools.report_status],  # Agent can call this
)

# Agent will report: "Gathering data" (40%), "Analyzing results" (80%), etc.
```

### Max Turns Recovery

Automatically handle conversation limits with graceful wrap-up:

```python
runner = ax.OpenAIRunner(
    agent_id=agent_id,
    max_turns_recovery=True,
    wrap_up_prompt="Please summarize your findings.",
)

# If agent hits max turns, runner automatically:
# 1. Catches MaxTurnsExceeded
# 2. Continues with wrap-up prompt
# 3. Returns final result
```

### Priority Queue

Control task execution order:

```python
# High priority - processed first
await ax.enqueue("urgent_task", context, priority=ax.Priority.HIGH)

# Low priority - processed later
await ax.enqueue("batch_job", context, priority=ax.Priority.LOW)
```

---

## Full Example: FastAPI Integration

See **[examples/openai-agents-fastapi/](examples/openai-agents-fastapi/)** for a complete production application showing:

- Background worker pool with task handlers
- FastAPI routes for queueing tasks and checking status
- Database session management with SQLAlchemy
- Custom agents with function tools
- Real-time progress monitoring
- Graceful shutdown with cleanup

---

## Configuration

Configure via environment variables or `.env` file:

```bash
# Redis connection (required)
REDIS_URL=redis://localhost:6379/0

# Worker settings
AGENTEXEC_NUM_WORKERS=4
AGENTEXEC_QUEUE_NAME=agentexec_tasks

# Database table prefix
AGENTEXEC_TABLE_PREFIX=agentexec_

# Activity messages (optional)
AGENTEXEC_ACTIVITY_MESSAGE_CREATE="Waiting to start."
AGENTEXEC_ACTIVITY_MESSAGE_STARTED="Task started."
AGENTEXEC_ACTIVITY_MESSAGE_COMPLETE="Task completed successfully."
AGENTEXEC_ACTIVITY_MESSAGE_ERROR="Task failed with error: {error}"
```
---

## Public API

### Task Queue

```python
# Enqueue task (async)
task = await ax.enqueue(task_name, context, priority=ax.Priority.LOW)
```

### Activity Tracking

```python
# Query activities
activities = ax.activity.list(session, page=1, page_size=50)
activity = ax.activity.detail(session, agent_id)
```

### Worker Pool

```python
from pydantic import BaseModel


class MyContext(BaseModel):
    param: str


pool = ax.WorkerPool(engine=engine)


@pool.task("task_name")
async def handler(agent_id: UUID, context: MyContext) -> None:
    # Task implementation - context is typed!
    print(context.param)


pool.start()  # Start worker processes
```

### OpenAI Runner

```python
runner = ax.OpenAIRunner(
    agent_id=agent_id,
    max_turns_recovery=True,
    wrap_up_prompt="Summarize...",
)

# Run agent
result = await runner.run(agent, input="...", max_turns=15)

# Streaming
result = await runner.run_streamed(agent, input="...", max_turns=15)
```

---

## Architecture

```
┌─────────────┐         ┌──────────┐         ┌─────────────┐
│ Your        │────────>│  Redis   │<────────│  Worker     │
│ Application │ enqueue │  Queue   │ dequeue │  Pool       │
└─────────────┘         └──────────┘         └─────────────┘
       │                                             │
       │                    Runner                   │
       │            (+ Activity Tracking)            │
       v                                             v
┌─────────────────────────────────────────────────────────-┐
│                    SQLAlchemy Database                   │
│               (Activities, Logs, Progress)               │
└─────────────────────────────────────────────────────────-┘
```

**Flow:**
1. Application enqueues task → Activity created (QUEUED)
2. Worker dequeues task → Executes with OpenAIRunner
3. Runner updates activity → RUNNING
4. Agent reports progress → Log entries created
5. Task completes → Activity marked COMPLETE/ERROR

---

## Database Models

AgentExec creates two tables (prefix configurable):

**`agentexec_activity`** - Main activity records
- `id` - Primary key (UUID)
- `agent_id` - Unique agent identifier (UUID)
- `agent_type` - Task name/type
- `created_at` - When activity was created
- `updated_at` - Last update timestamp

**`agentexec_activity_log`** - Status and progress logs
- `id` - Primary key (UUID)
- `activity_id` - Foreign key to activity
- `message` - Log message
- `status` - QUEUED, RUNNING, COMPLETE, ERROR, CANCELED
- `completion_percentage` - Progress (0-100)
- `created_at` - When log was created

---

## Development

```bash
# Clone repository
git clone https://github.com/Agent-CI/agentexec
cd agentexec

# Install dependencies
uv sync

# Run tests
uv run pytest

# Type checking
uv run mypy src/agentexec

# Linting
uv run ruff check src/

# Formatting
uv run ruff format src/
```



## License

MIT License - see [LICENSE](LICENSE) for details

---

## Links

- **Documentation**: See example application in `examples/openai-agents-fastapi/`
- **Issues**: [GitHub Issues](https://github.com/Agent-CI/agentexec/issues)
- **PyPI**: [agentexec](https://pypi.org/project/agentexec/)

