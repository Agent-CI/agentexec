# FastAPI Integration

This guide shows how to integrate agentexec with FastAPI to build REST APIs for AI agent tasks.

## Project Structure

```
my-fastapi-project/
├── pyproject.toml
├── .env
├── src/
│   └── myapp/
│       ├── __init__.py
│       ├── main.py         # FastAPI application
│       ├── worker.py       # Worker pool and tasks
│       ├── views.py        # API endpoints
│       ├── contexts.py     # Pydantic models
│       ├── db.py           # Database setup
│       └── deps.py         # FastAPI dependencies
└── tests/
```

## Basic Setup

### Database Configuration

For production applications, use **Alembic migrations** to manage your database schema. See the [Basic Usage Guide](basic-usage.md#database-setup) for complete Alembic setup instructions, or the [examples/openai-agents-fastapi](https://github.com/Agent-CI/agentexec/tree/main/examples/openai-agents-fastapi) directory for a working example.

For quick prototyping, you can use `create_all()`:

```python
# db.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
import agentexec as ax

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///agents.db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

# Quick start approach; use Alembic migrations for production
ax.Base.metadata.create_all(engine)
```

### FastAPI Dependencies

```python
# deps.py
from typing import Generator
from sqlalchemy.orm import Session
from .db import SessionLocal

def get_db() -> Generator[Session, None, None]:
    """Database session dependency."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
```

### Worker Pool

```python
# worker.py
from uuid import UUID
from pydantic import BaseModel
from agents import Agent
import agentexec as ax

from .db import engine, DATABASE_URL

# Create worker pool
pool = ax.WorkerPool(engine=engine, database_url=DATABASE_URL)

class ResearchContext(BaseModel):
    company: str
    focus_areas: list[str] = []

@pool.task("research_company")
async def research_company(agent_id: UUID, context: ResearchContext) -> dict:
    runner = ax.OpenAIRunner(agent_id=agent_id, max_turns_recovery=True)

    agent = Agent(
        name="Research Agent",
        instructions=f"""Research {context.company}.
        Focus on: {', '.join(context.focus_areas) or 'general info'}
        {runner.prompts.report_status}""",
        tools=[runner.tools.report_status],
        model="gpt-4o",
    )

    result = await runner.run(agent, input=f"Research {context.company}", max_turns=15)
    return {"company": context.company, "findings": result.final_output}

if __name__ == "__main__":
    pool.run()
```

### FastAPI Application

```python
# main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy.orm import Session
import agentexec as ax

from .db import engine
from .views import router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Clean up stale activities
    with Session(engine) as session:
        canceled = ax.activity.cancel_pending(session)
        if canceled > 0:
            print(f"Cleaned up {canceled} stale activities")

    yield

    # Shutdown: Close Redis connection
    await ax.close_redis()

app = FastAPI(
    title="Agent API",
    description="API for AI agent tasks",
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(router, prefix="/api")
```

## API Endpoints

### Task Endpoints

```python
# views.py
from typing import Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
import agentexec as ax

from .deps import get_db
from .worker import ResearchContext

router = APIRouter()

# Request/Response models
class TaskRequest(BaseModel):
    company: str
    focus_areas: list[str] = []

class TaskResponse(BaseModel):
    agent_id: str
    message: str

class StatusResponse(BaseModel):
    agent_id: str
    status: str
    progress: Optional[int]
    message: Optional[str]

# Endpoints
@router.post("/tasks/research", response_model=TaskResponse)
async def create_research_task(request: TaskRequest):
    """Queue a new research task."""
    task = await ax.enqueue(
        "research_company",
        ResearchContext(company=request.company, focus_areas=request.focus_areas),
    )

    return TaskResponse(
        agent_id=str(task.agent_id),
        message=f"Research task queued for {request.company}"
    )

@router.get("/tasks/{agent_id}/status", response_model=StatusResponse)
async def get_task_status(agent_id: str, db: Session = Depends(get_db)):
    """Get the status of a task."""
    activity = ax.activity.detail(db, agent_id)

    if not activity:
        raise HTTPException(status_code=404, detail="Task not found")

    return StatusResponse(
        agent_id=str(activity.agent_id),
        status=activity.status.value,
        progress=activity.latest_completion_percentage,
        message=activity.logs[-1].message if activity.logs else None
    )
```

### Activity Endpoints

```python
# views.py (continued)

class ActivityListResponse(BaseModel):
    items: list[dict]
    total: int
    page: int
    pages: int

class ActivityDetailResponse(BaseModel):
    agent_id: str
    task_type: str
    status: str
    progress: Optional[int]
    created_at: str
    updated_at: str
    logs: list[dict]

@router.get("/activities", response_model=ActivityListResponse)
async def list_activities(
    page: int = 1,
    page_size: int = 20,
    db: Session = Depends(get_db)
):
    """List all activities with pagination."""
    return ax.activity.list(db, page=page, page_size=page_size)

@router.get("/activities/{agent_id}", response_model=ActivityDetailResponse)
async def get_activity_detail(agent_id: str, db: Session = Depends(get_db)):
    """Get detailed activity with full log history."""
    activity = ax.activity.detail(db, agent_id)

    if not activity:
        raise HTTPException(status_code=404, detail="Activity not found")

    return activity
```

## Running the Application

### Development

**Terminal 1 - Start FastAPI:**
```bash
uv run uvicorn myapp.main:app --reload --port 8000
```

**Terminal 2 - Start Workers:**
```bash
uv run python -m myapp.worker
```

### Production

Use a process manager like systemd or Docker Compose:

```yaml
# docker-compose.yml
version: '3.8'

services:
  api:
    build: .
    command: uvicorn myapp.main:app --host 0.0.0.0 --port 8000
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@db/myapp
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - db
      - redis

  worker:
    build: .
    command: python -m myapp.worker
    environment:
      - DATABASE_URL=postgresql://user:pass@db/myapp
      - REDIS_URL=redis://redis:6379/0
      - AGENTEXEC_NUM_WORKERS=4
    depends_on:
      - db
      - redis

  db:
    image: postgres:16
    environment:
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
      - POSTGRES_DB=myapp

  redis:
    image: redis:7-alpine
```

## Next Steps

- [Pipelines](pipelines.md) - Multi-step workflows
- [Production Guide](../deployment/production.md) - Production deployment
- [Docker Deployment](../deployment/docker.md) - Containerization
