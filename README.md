# `agentexec`

[![PyPI](https://img.shields.io/pypi/v/agentexec?color=blue)](https://pypi.org/project/agentexec/)
[![Python](https://img.shields.io/badge/python-3.12+-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Type Checked](https://img.shields.io/badge/type%20checked-ty-blue)](https://github.com/astral-sh/ty)
[![Code Style](https://img.shields.io/badge/code%20style-ruff-orange)](https://github.com/astral-sh/ruff)

**Production-ready orchestration for OpenAI Agents SDK** with Redis-backed task queues, SQLAlchemy activity tracking, and multiprocessing worker pools.

---

## The Problem

You've built an AI agent. It works great in development. Now you need to run it in production:

**Customer Support Automation** - A user submits a ticket. Your agent needs to research their account, check previous interactions, and draft a response. This takes 2-3 minutes. You can't block the HTTP request.

**Document Processing Pipeline** - Users upload contracts for analysis. Each document needs OCR, entity extraction, clause identification, and risk scoring. You need to process dozens concurrently while tracking progress.

**Research & Reporting** - Your agent researches companies, gathers data from multiple sources, and generates reports. Users need to see "Gathering financials... 40%" not just a spinning loader.

**Multi-Agent Workflows** - One agent discovers leads, fans out to research each one, then a final agent aggregates results. You need coordination, not chaos.

Running AI agents in production requires:

- **Background execution** - Agents take minutes; users shouldn't wait
- **Progress tracking** - Know what your agents are doing in real-time
- **Fault tolerance** - Handle failures gracefully with full error traces
- **Scalability** - Process multiple tasks across worker processes
- **Observability** - Complete audit trail of agent activities

`agentexec` provides all of this out of the box.

---

## Installation

```bash
uv add agentexec
```

**Requirements:**
- Python 3.12+
- Redis
- SQLAlchemy-compatible database (PostgreSQL, MySQL, SQLite)

---

## Quick Start

A typical agentexec application has a few files working together. Here's a complete working example showing each part:

### 1. Define Your Task

```python
# worker.py
from uuid import UUID
from pydantic import BaseModel
from agents import Agent
from sqlalchemy import create_engine
import agentexec as ax

class ResearchContext(BaseModel):
    company: str

engine = create_engine("postgresql://localhost/myapp")
ax.Base.metadata.create_all(engine)  # Creates activity tracking tables

pool = ax.Pool(engine=engine)

@pool.task("research_company")
async def research_company(agent_id: UUID, context: ResearchContext):
    runner = ax.OpenAIRunner(agent_id)

    agent = Agent(
        name="Research Agent",
        instructions=f"Research {context.company}. {runner.prompts.report_status}",
        tools=[runner.tools.report_status],  # Agent can report its own progress
        model="gpt-4o",
    )

    result = await runner.run(agent, input="Begin research")
    return result.final_output
```

### 2. Queue Tasks and Track Progress

```python
# views.py
from uuid import UUID
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
import agentexec as ax
from worker import ResearchContext
from db import get_db

router = APIRouter()

@router.post("/research")
async def start_research(company: str):
    task = await ax.enqueue("research_company", ResearchContext(company=company))
    return {"agent_id": str(task.agent_id), "status": "queued"}

@router.get("/research/{agent_id}")
def get_status(agent_id: UUID, db: Session = Depends(get_db)):
    return ax.activity.detail(db, agent_id=agent_id)
```

### 3. Run Workers

Add this to the bottom of your worker file:

```python
# worker.py
if __name__ == "__main__":
    pool.run()
```

```bash
python worker.py
```

That's it. Tasks are queued to Redis, workers process them in parallel, progress is tracked in your database, and your API stays responsive.

---

## Supported Patterns

### Automatic Activity Tracking

Every task gets full lifecycle tracking without manual updates:

```python
runner = ax.OpenAIRunner(agent_id=agent_id)
result = await runner.run(agent, input="...")

# Activity automatically transitions:
# QUEUED → RUNNING → COMPLETE (or ERROR on failure)
```

### Agent Self-Reporting

Agents can report their own progress:

```python
agent = Agent(
    instructions=f"Do research. {runner.prompts.report_status}",
    tools=[runner.tools.report_status],
)
# Agent calls: report_status("Analyzing financials", 60)
```

### Manual Progress Updates

Update progress explicitly from your task:

```python
ax.activity.update(agent_id, "Processing batch 3 of 10", percentage=30)
```

### Priority Queue

Control task execution order:

```python
await ax.enqueue("urgent_task", context, priority=ax.Priority.HIGH)  # Front of queue
await ax.enqueue("batch_job", context, priority=ax.Priority.LOW)     # Back of queue
```

### Max Turns Recovery

Gracefully handle conversation limits:

```python
runner = ax.OpenAIRunner(
    agent_id=agent_id,
    max_turns_recovery=True,
    wrap_up_prompt="Please summarize your findings.",
)
result = await runner.run(agent, max_turns=15)
# If agent hits max turns, automatically continues with wrap-up
```

### Multi-Step Pipelines

Orchestrate complex workflows with parallel execution:

```python
pipeline = ax.Pipeline(pool)

class ResearchPipeline(pipeline.Base):
    @pipeline.step(0, "parallel research")
    async def gather_data(self, context: InputContext):
        brand_task = await ax.enqueue("research_brand", context)
        market_task = await ax.enqueue("research_market", context)
        return await ax.gather(brand_task, market_task)  # Run in parallel

    @pipeline.step(1, "analysis")
    async def analyze(self, brand: BrandResult, market: MarketResult) -> FinalReport:
        task = await ax.enqueue("analyze", AnalysisContext(brand=brand, market=market))
        return await ax.get_result(task)

# Queue pipeline (non-blocking)
task = await pipeline.enqueue(context=InputContext(company="Anthropic"))

# Or run inline (blocking)
result = await pipeline.run(None, InputContext(company="Anthropic"))
```

### Dynamic Fan-Out with Tracker

Coordinate dynamically-queued tasks:

```python
tracker = ax.Tracker("research", batch_id)

@function_tool
async def queue_research(company: str) -> None:
    """Discovery agent calls this for each company found."""
    tracker.incr()
    await ax.enqueue("research", ResearchContext(company=company, batch_id=batch_id))

@function_tool
async def save_result(result: ResearchResult) -> None:
    """Research agent calls this when done."""
    save_to_database(result)
    if tracker.decr() == 0:  # Last one triggers aggregation
        await ax.enqueue("aggregate", AggregateContext(batch_id=batch_id))
```

---

## Integration

### Running Alongside Your Application

If you have an existing FastAPI/Flask/Django backend, run the worker pool in a separate process:

```python
# main.py - Your API server
from fastapi import FastAPI
import agentexec as ax

app = FastAPI()

@app.post("/process")
async def process(data: str):
    task = await ax.enqueue("process_data", ProcessContext(data=data))
    return {"agent_id": task.agent_id}
```

```python
# worker.py - Run separately
from myapp.tasks import pool

if __name__ == "__main__":
    pool.run()
```

**Terminal 1:** Start your API server

```bash
uvicorn main:app
```

**Terminal 2:** Start the workers

```bash
python worker.py
```

### As a Standalone Worker Service

```python
# worker.py
import os
from sqlalchemy import create_engine
import agentexec as ax

engine = create_engine(os.environ["DATABASE_URL"])
pool = ax.Pool(engine=engine)

@pool.task("my_task")
async def my_task(agent_id, context):
    # Your task implementation
    pass

if __name__ == "__main__":
    print(f"Starting {ax.CONF.num_workers} workers...")
    print(f"Queue: {ax.CONF.queue_name}")
    pool.run()
```

### Docker Deployment

**1. Create your worker Dockerfile:**

```dockerfile
# Dockerfile.worker
FROM ghcr.io/agent-ci/agentexec-worker:latest

COPY ./src /app/src
ENV AGENTEXEC_WORKER_MODULE=src.worker
```

**2. Create your worker module:**

```python
# src/worker.py
import os
import agentexec as ax

pool = ax.Pool(database_url=os.environ["DATABASE_URL"])

@pool.task("my_task")
async def my_task(agent_id, context):
    pass
```

**3. Build and run:**

```bash
docker build -f Dockerfile.worker -t my-worker .
```

```bash
docker run -e DATABASE_URL=... -e REDIS_URL=... -e OPENAI_API_KEY=... my-worker
```

---

## Backend Architecture

### Redis

agentexec uses Redis for task queuing, result storage, real-time log streaming, and coordination between workers. We chose Redis because it provides exactly the primitives we need (lists, pubsub, atomic counters) with minimal operational overhead.

**AWS Compatible:** Since we use standard Redis features, AWS ElastiCache works out of the box.

```bash
REDIS_URL=redis://localhost:6379/0
# or
REDIS_URL=redis://my-cluster.abc123.use1.cache.amazonaws.com:6379
```

### Extensible State Backend

The state backend is pluggable. We're adding support for additional backends (DynamoDB, PostgreSQL, in-memory for testing). You can also implement your own:

```bash
AGENTEXEC_STATE_BACKEND=agentexec.state.redis_backend  # Default
AGENTEXEC_STATE_BACKEND=myapp.state.dynamodb_backend   # Custom
```

### Database

Activity tracking uses SQLAlchemy with two tables:

**`agentexec_activity`** - Main activity records
- `agent_id` - Unique identifier (UUID)
- `agent_type` - Task name
- `created_at`, `updated_at` - Timestamps

**`agentexec_activity_log`** - Status and progress
- `activity_id` - Foreign key
- `message` - Log message
- `status` - QUEUED, RUNNING, COMPLETE, ERROR, CANCELED
- `percentage` - Progress (0-100)

---

## Building Your Own Interface

### Data Structures

The activity tracking module exposes Pydantic schemas for building APIs:

```python
from agentexec.activity.schemas import (
    ActivityListSchema,      # Paginated list response
    ActivityListItemSchema,  # Single item in list (lightweight)
    ActivityDetailSchema,    # Full activity with log history
    ActivityLogSchema,       # Single log entry
)
```

**List activities:**

```python
with Session(engine) as db:
    result = ax.activity.list(db, page=1, page_size=20)
    # Returns ActivityListSchema:
    # {
    #   "items": [...],      # List of ActivityListItemSchema
    #   "total": 150,
    #   "page": 1,
    #   "page_size": 20,
    #   "total_pages": 8
    # }
```

**Get activity detail:**

```python
activity = ax.activity.detail(db, agent_id=agent_id)
# Returns ActivityDetailSchema:
# {
#   "id": "...",
#   "agent_id": "...",
#   "agent_type": "research_company",
#   "created_at": "2024-01-15T10:30:00Z",
#   "updated_at": "2024-01-15T10:32:45Z",
#   "logs": [
#     {"status": "queued", "message": "Waiting to start", "percentage": 0, ...},
#     {"status": "running", "message": "Gathering data", "percentage": 30, ...},
#     {"status": "complete", "message": "Done", "percentage": 100, ...}
#   ]
# }
```

**Count active agents:**

```python
count = ax.activity.active_count(db)
# Returns number of agents with status QUEUED or RUNNING
```

### Building a CLI Monitor

```python
# cli_monitor.py
from rich.live import Live
from rich.table import Table
from sqlalchemy.orm import Session
import agentexec as ax

def build_table(db: Session) -> Table:
    table = Table(title=f"Active Agents: {ax.activity.active_count(db)}")
    table.add_column("Status")
    table.add_column("Task")
    table.add_column("Message")
    table.add_column("Progress")

    status_icon = {"queued": "⏳", "running": "🔄", "complete": "✅", "error": "❌"}
    for item in ax.activity.list(db, page=1, page_size=10).items:
        table.add_row(
            status_icon.get(item.status, "?"),
            item.agent_type,
            item.latest_log_message or "",
            f"{item.percentage}%",
        )
    return table

def monitor(engine):
    with Live(refresh_per_second=1) as live:
        while True:
            with Session(engine) as db:
                live.update(build_table(db))

if __name__ == "__main__":
    from db import engine
    monitor(engine)
```

---

## UI Components

The `agentexec-ui` package provides React components for building monitoring interfaces:

```bash
npm install agentexec-ui
```

### Components

```tsx
import {
  TaskList,
  TaskDetail,
  ActiveAgentsBadge,
  StatusBadge,
  ProgressBar,
} from 'agentexec-ui';

// Display paginated task list
<TaskList
  items={activities.items}
  loading={isLoading}
  onTaskClick={(agentId) => setSelected(agentId)}
  selectedAgentId={selectedId}
/>

// Full activity detail view
<TaskDetail
  activity={activityDetail}
  loading={isDetailLoading}
  error={error}
  onClose={() => setSelected(null)}
/>

// Active count badge
<ActiveAgentsBadge count={activeCount} loading={isLoading} />

// Individual status indicators
<StatusBadge status="running" />
<ProgressBar percentage={65} status="running" />
```

### TypeScript Types

```typescript
import type {
  Status,           // 'queued' | 'running' | 'complete' | 'error' | 'canceled'
  ActivityLog,
  ActivityDetail,
  ActivityListItem,
  ActivityList,
} from 'agentexec-ui';
```

The components are headless (no built-in styling) and work with any CSS framework. See `examples/openai-agents-fastapi/ui/` for a complete React app with TanStack Query integration.

---

## Module Reference

### `agentexec` (main package)

```python
import agentexec as ax

# Task Queue
task = await ax.enqueue(task_name, context, priority=ax.Priority.LOW)
result = await ax.get_result(task, timeout=300)
results = await ax.gather(task1, task2, task3)

# Worker Pool
pool = ax.Pool(engine=engine)

@pool.task("task_name")
async def handler(agent_id: UUID, context: MyContext) -> Result:
    pass

pool.run()

# Runners
runner = ax.OpenAIRunner(agent_id, max_turns_recovery=True)
result = await runner.run(agent, input="...", max_turns=15)

# Pipelines
pipeline = ax.Pipeline(pool)

class MyPipeline(pipeline.Base):
    @pipeline.step(0, "description")
    async def step_one(self, context): ...

# Tracker
tracker = ax.Tracker("name", batch_id)
tracker.incr()
if tracker.decr() == 0: ...  # All complete

# Configuration
ax.CONF.num_workers      # Number of worker processes
ax.CONF.queue_name       # Redis queue name
ax.CONF.redis_url        # Redis connection URL

# Database
ax.Base                  # SQLAlchemy declarative base
```

### `agentexec.activity`

```python
# Create/update activities
activity = ax.activity.create(db, agent_id, agent_type)
ax.activity.update(agent_id, message, percentage=50)
ax.activity.complete(agent_id, message="Done")
ax.activity.error(agent_id, error="Failed: ...")

# Query activities
activities = ax.activity.list(db, page=1, page_size=20)
activity = ax.activity.detail(db, agent_id=agent_id)
count = ax.activity.active_count(db)

# Cleanup
canceled = ax.activity.cancel_pending(db)
```

### Runners

```python
import agentexec as ax

runner = ax.OpenAIRunner(
    agent_id=agent_id,
    max_turns_recovery=True,
    wrap_up_prompt="Summarize...",
)

# Built-in prompts and tools
runner.prompts.report_status  # Instruction text for agents
runner.tools.report_status    # Pre-bound function tool

# Run agent
result = await runner.run(agent, input="...", max_turns=15)
result = await runner.run_streamed(agent, input="...", max_turns=15)

# Base class for custom runners
class MyRunner(ax.BaseAgentRunner):
    async def run(self, agent, input): ...
```

### Worker Pool

```python
import agentexec as ax

pool = ax.Pool(engine=engine)
pool = ax.Pool(database_url="postgresql://...")

@pool.task("name")
async def handler(agent_id, context): ...

pool.run()       # Blocking - runs workers
pool.start()     # Non-blocking - starts workers in background
pool.shutdown()  # Graceful shutdown
```

---

## Configuration

All settings via environment variables:

```bash
# Redis (required)
REDIS_URL=redis://localhost:6379/0
REDIS_POOL_SIZE=10
REDIS_POOL_TIMEOUT=5

# Workers
AGENTEXEC_NUM_WORKERS=4
AGENTEXEC_QUEUE_NAME=agentexec_tasks
AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT=300

# Database
AGENTEXEC_TABLE_PREFIX=agentexec_

# Results
AGENTEXEC_RESULT_TTL=3600

# State backend
AGENTEXEC_STATE_BACKEND=agentexec.state.redis_backend
AGENTEXEC_KEY_PREFIX=agentexec

# Activity messages (customizable)
AGENTEXEC_ACTIVITY_MESSAGE_CREATE="Waiting to start."
AGENTEXEC_ACTIVITY_MESSAGE_STARTED="Task started."
AGENTEXEC_ACTIVITY_MESSAGE_COMPLETE="Task completed successfully."
AGENTEXEC_ACTIVITY_MESSAGE_ERROR="Task failed with error: {error}"
```

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
uv run ty check

# Linting
uv run ruff check src/

# Formatting
uv run ruff format src/
```

### Project Structure

```
agentexec/
├── src/agentexec/
│   ├── activity/       # Activity tracking (models, schemas, tracker)
│   ├── core/           # Task queue, database, results
│   ├── runners/        # Agent runners (base, OpenAI)
│   ├── state/          # State backend (Redis, protocol)
│   ├── worker/         # Worker pool, multiprocessing
│   ├── pipeline.py     # Multi-step pipelines
│   ├── tracker.py      # Dynamic fan-out coordination
│   └── config.py       # Configuration
├── ui/                 # React component library
├── examples/           # Example applications
├── docs/               # Documentation
└── docker/             # Docker deployment files
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Run `uv run pytest` and `uv run ty check`
5. Submit a pull request

---

## License

MIT License - see [LICENSE](LICENSE) for details.

---

## Links

- **PyPI**: [agentexec](https://pypi.org/project/agentexec/)
- **npm**: [agentexec-ui](https://www.npmjs.com/package/agentexec-ui)
- **Documentation**: [docs/](docs/)
- **Example App**: [examples/openai-agents-fastapi/](examples/openai-agents-fastapi/)
- **Issues**: [GitHub Issues](https://github.com/Agent-CI/agentexec/issues)
