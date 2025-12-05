# `agentexec`

[![codecov](https://codecov.io/gh/Agent-CI/agentexec/branch/main/graph/badge.svg)](https://codecov.io/gh/Agent-CI/agentexec)

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


# agentexec uses Pydantic models for input and outuput schemas
class ResearchContext(BaseModel):
    company: str


# Database for activity tracking (share with the rest of your app)
engine = create_engine("sqlite:///agents.db")

# Create worker pool
pool = ax.WorkerPool(engine=engine)


@pool.task("research_company")
async def research_company(agent_id: UUID, context: ResearchContext) -> None:
    """Background task that runs an AI agent."""
    runner = ax.OpenAIRunner(
        agent_id=agent_id,
    )

    agent = Agent(
        name="Research Agent",
        instructions=(
            f"Research {context.company}.\n"
            runner.prompts.report_status
        ),
        tools=[
            runner.tools.report_status,  # Automatically associated with agent_id
        ],
        model="gpt-5",
    )

    result = await runner.run(
        agent,
        input="Start research",
    )
    print(f"Done! {result.final_output}")  # Native result object


if __name__ == "__main__":
    pool.start()  # Start worker process
```

### 2. Queue Tasks from Your Application

```python
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
    tools=[runner.tools.report_status],  # Agent passes a short message and percentage
)

# Agent will report: "Gathering data" (40%), "Analyzing results" (80%), etc.
```

### Explicit Reporting
Manually update activity status and progress from within your task:

```python
ax.activity.update(agent_id, "Starting data collection", percentage=10)
```

### Max Turns Recovery

Automatically handle conversation limits with graceful wrap-up:

```python
runner = ax.OpenAIRunner(
    agent_id=agent_id,
    max_turns_recovery=True,
    wrap_up_prompt="Please summarize your findings.",
)
agent = Agent(
    instructions="Research the topic thoroughly."
)
result = await runner.run(agent, max_turns=5)

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

### Pipelines

Orchestrate multi-step workflows with parallel task execution:

```python
pipeline = ax.Pipeline(pool)


class MyPipeline(pipeline.Base):
    @pipeline.step(0, "brand and market research")
    async def parallel_research(self, context: InputContext):
        """Run multiple tasks in parallel."""
        brand_task = await ax.enqueue("research_brand", context)
        market_task = await ax.enqueue("research_market", context)
        return await ax.gather(brand_task, market_task)

    @pipeline.step(1, "research analysis")
    async def analyze(self, brand_result: BrandResult, market_result: MarketResult) -> AnalysisResult:
        """Combine results from previous step."""
        task = await ax.enqueue("analyze_research", AnalysisContext(
            brand=brand_result,
            market=market_result,
        ))
        return await ax.get_result(task)


# Queue to worker (non-blocking, activity tracked automatically)
task = await pipeline.enqueue(context=InputContext(company="Anthropic"))

# Or run inline (blocking)
result = await pipeline.run(None, InputContext(company="Anthropic"))
```

See **[examples/openai-agents-fastapi/pipeline.py](examples/openai-agents-fastapi/pipeline.py)** for a complete example.

### Dynamic Fan-Out with Tracker

Coordinate tasks that are queued dynamically and trigger a follow-up when all complete:

```python
import uuid

tracker = ax.Tracker("research", uuid.uuid4())

@pool.task("research")
async def research(agent_id: UUID, context: ResearchContext) -> ResearchResult:
    runner = ax.OpenAIRunner(agent_id=agent_id)
    agent = Agent(
        name="Research Agent",
        instructions="...",
        tools=[save_research],
        output_type=ResearchResult,
    )
    return await runner.run(agent, input=f"Research {context.company}")


@pool.task("aggregate")
async def aggregate(agent_id: UUID, context: AggregateContext) -> None:
    # ... aggregate results ...

@function_tool
async def queue_research(company: str) -> None:
    tracker.incr()
    await ax.enqueue("research", ResearchContext(company=company, batch_id=batch_id))

@function_tool
async def save_research(context: ResearchResult) -> None:
    # save_research_to_database(context)
    tracker.decr()
    if tracker.complete:
        await ax.enqueue("aggregate", ...)


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
- `percentage` - Progress (0-100)
- `created_at` - When log was created

---

## Docker Deployment

Deploy workers using the official Docker image:

### 1. Create your worker Dockerfile

```dockerfile
FROM ghcr.io/agent-ci/agentexec-worker:latest

COPY ./src /app/src
ENV AGENTEXEC_WORKER_MODULE=src.worker
```

### 2. Create your worker module

```python
# src/worker.py
import os
import agentexec as ax

pool = ax.WorkerPool(database_url=os.environ["DATABASE_URL"])

@pool.task("my_task")
async def my_task(agent_id, context):
    # Your task implementation
    pass
```

### 3. Run with Docker

```bash
docker build -t my-worker .
docker run \
  -e DATABASE_URL=postgresql://... \
  -e REDIS_URL=redis://... \
  -e OPENAI_API_KEY=sk-... \
  my-worker
```

See **[docker/README.md](docker/README.md)** for full documentation including Docker Compose examples.

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

