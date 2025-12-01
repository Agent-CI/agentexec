# OpenAI Agents FastAPI Example

This example demonstrates a complete FastAPI application using **agentexec** to orchestrate OpenAI Agents SDK in production, including a React frontend for monitoring agents.

## What This Example Demonstrates

### Core Features

- **Background worker pool** (`worker.py`) - Multi-process task execution with Redis queue
- **OpenAIRunner integration** - Automatic activity tracking for agent lifecycle
- **Custom FastAPI routes** (`views.py`) - Building your own API on agentexec's public API
- **Database session management** (`main.py`) - Standard SQLAlchemy patterns with full control
- **Agent self-reporting** - Agents report progress via built-in `report_status` tool
- **Max turns recovery** - Automatic handling of conversation limits with wrap-up prompts
- **React Frontend** (`frontend/`) - GitHub-inspired dark mode UI for monitoring agents

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


## Frontend

The example includes a React frontend built with **agentexec-ui** components. The UI provides:

- Sidebar navigation with active agent count badge (updates every 15 seconds)
- Paginated task list showing status and progress
- Task detail panel with full activity log history
- GitHub-inspired dark mode styling

### Running the Frontend

**Development mode (with hot reload):**

```bash
# In one terminal - start the API
uvicorn main:app --reload

# In another terminal - start the frontend dev server
cd frontend
npm install
npm run dev
# Opens at http://localhost:3000 with API proxy to :8000
```

**Production mode (served by FastAPI):**

```bash
# Build the frontend
cd frontend
npm install
npm run build

# Start the API (serves frontend automatically)
uvicorn main:app
# Opens at http://localhost:8000
```

### Using agentexec-ui in Your Own Project

The frontend uses the `agentexec-ui` package which can be installed separately:

```bash
npm install agentexec-ui
```

```tsx
import { TaskList, TaskDetail, useActivityList } from 'agentexec-ui';
import 'agentexec-ui/styles';

function MyApp() {
  const { data } = useActivityList({ pollInterval: 15000 });
  return <TaskList items={data?.items || []} />;
}
```

See [agentexec-ui README](../../packages/agentexec-ui/README.md) for full documentation.

## Configuration

Set via environment variables:

```bash
DATABASE_URL="sqlite:///agents.db"              # or postgresql://...
REDIS_URL="redis://localhost:6379/0"
QUEUE_NAME="agentexec:tasks"
NUM_WORKERS="4"
OPENAI_API_KEY="sk-..."
SERVE_FRONTEND="true"                           # Set to "false" to disable frontend serving
```

