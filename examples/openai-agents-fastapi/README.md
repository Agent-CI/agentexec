# OpenAI Agents FastAPI Example

This example demonstrates a complete FastAPI application using **agentexec** to orchestrate OpenAI Agents SDK in production.

## What This Example Demonstrates

### Core Features

- **Background worker pool** (`worker.py`) - Multi-process task execution with Redis queue
- **OpenAIRunner integration** - Automatic activity tracking for agent lifecycle
- **Custom FastAPI routes** (`views.py`) - Building your own API on agentexec's public API
- **Database session management** (`main.py`) - Standard SQLAlchemy patterns with full control
- **Agent self-reporting** - Agents report progress via built-in `report_status` tool
- **Max turns recovery** - Automatic handling of conversation limits with wrap-up prompts

### Key Patterns Shown

**Task Registration:**
```python
@pool.task("research_company")
async def research_company(agent_id: UUID, payload: dict):
    runner = ax.OpenAIRunner(agent_id=agent_id, max_turns_recovery=True)
    # ... agent setup and execution
```

**Activity Tracking API:**
```python
# List activities with pagination
ax.activity.list(db, page=1, page_size=50)

# Get detailed activity with full log history
ax.activity.detail(db, agent_id)

# Cleanup on shutdown
ax.activity.cancel_pending(db)
```

**Queueing Tasks:**
```python
task = ax.enqueue(
    task_name="research_company",
    payload={"company_name": "Acme"},
    priority=ax.Priority.HIGH,
)
```

## Quick Start

```bash
# Install dependencies
cd examples/openai-agents-fastapi
uv sync

# Start Redis
docker run -d -p 6379:6379 redis:latest

# Set API key
export OPENAI_API_KEY="your-key"

# Run migrations
alembic upgrade head

# Start worker (terminal 1)
python -m openai_agents_fastapi.worker

# Start API server (terminal 2)
uvicorn openai_agents_fastapi.main:app --reload
```

## Try It

Queue a task:
```bash
curl -X POST "http://localhost:8000/api/tasks" \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "research_company",
    "payload": {"company_name": "Anthropic"}
  }'
```

Monitor progress:
```bash
# List all activities
curl "http://localhost:8000/api/agents/activity"

# Get specific agent details
curl "http://localhost:8000/api/agents/activity/{agent_id}"
```


## Configuration

Set via environment variables:

```bash
DATABASE_URL="sqlite:///agents.db"              # or postgresql://...
REDIS_URL="redis://localhost:6379/0"
QUEUE_NAME="agentexec:tasks"
NUM_WORKERS="4"
OPENAI_API_KEY="sk-..."
```

