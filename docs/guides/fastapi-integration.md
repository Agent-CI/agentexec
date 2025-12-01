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

```python
# db.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
import agentexec as ax

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///agents.db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

# Create tables
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
    priority: str = "low"

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
    priority = ax.Priority.HIGH if request.priority == "high" else ax.Priority.LOW

    task = await ax.enqueue(
        "research_company",
        ResearchContext(company=request.company, focus_areas=request.focus_areas),
        priority=priority
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
    result = ax.activity.list(db, page=page, page_size=page_size)

    return ActivityListResponse(
        items=[{
            "agent_id": str(item.agent_id),
            "task_type": item.agent_type,
            "status": item.status.value,
            "progress": item.latest_completion_percentage,
            "message": item.latest_log,
            "updated_at": item.updated_at.isoformat()
        } for item in result.items],
        total=result.total,
        page=result.page,
        pages=result.pages
    )

@router.get("/activities/{agent_id}", response_model=ActivityDetailResponse)
async def get_activity_detail(agent_id: str, db: Session = Depends(get_db)):
    """Get detailed activity with full log history."""
    activity = ax.activity.detail(db, agent_id)

    if not activity:
        raise HTTPException(status_code=404, detail="Activity not found")

    return ActivityDetailResponse(
        agent_id=str(activity.agent_id),
        task_type=activity.agent_type,
        status=activity.status.value,
        progress=activity.latest_completion_percentage,
        created_at=activity.created_at.isoformat(),
        updated_at=activity.updated_at.isoformat(),
        logs=[{
            "message": log.message,
            "status": log.status.value,
            "progress": log.completion_percentage,
            "created_at": log.created_at.isoformat()
        } for log in activity.logs]
    )
```

### Result Endpoints

```python
# views.py (continued)

@router.get("/tasks/{agent_id}/result")
async def get_task_result(agent_id: str, timeout: int = 30):
    """
    Get the result of a completed task.
    Waits up to `timeout` seconds for the task to complete.
    """
    try:
        result = await ax.get_result(agent_id, timeout=timeout)
        return {"agent_id": agent_id, "result": result}
    except TimeoutError:
        raise HTTPException(
            status_code=408,
            detail=f"Task not completed within {timeout} seconds"
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Result not found")
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

## Advanced Patterns

### WebSocket Progress Updates

```python
# views.py
from fastapi import WebSocket, WebSocketDisconnect
import asyncio

@router.websocket("/ws/tasks/{agent_id}")
async def task_progress_websocket(websocket: WebSocket, agent_id: str):
    """WebSocket endpoint for real-time task progress."""
    await websocket.accept()

    try:
        while True:
            with Session(engine) as db:
                activity = ax.activity.detail(db, agent_id)

                if not activity:
                    await websocket.send_json({"error": "Task not found"})
                    break

                await websocket.send_json({
                    "status": activity.status.value,
                    "progress": activity.latest_completion_percentage,
                    "message": activity.logs[-1].message if activity.logs else None
                })

                if activity.status.value in ("COMPLETE", "ERROR", "CANCELED"):
                    break

            await asyncio.sleep(1)

    except WebSocketDisconnect:
        pass
```

### Background Task Integration

Start tasks from FastAPI's background tasks:

```python
from fastapi import BackgroundTasks

async def cleanup_old_activities():
    """Background task to clean up old activities."""
    with Session(engine) as db:
        # Delete activities older than 30 days
        from datetime import datetime, timedelta
        cutoff = datetime.utcnow() - timedelta(days=30)

        db.execute(
            "DELETE FROM agentexec_activity WHERE updated_at < :cutoff",
            {"cutoff": cutoff}
        )
        db.commit()

@router.post("/admin/cleanup")
async def trigger_cleanup(background_tasks: BackgroundTasks):
    """Trigger cleanup of old activities."""
    background_tasks.add_task(cleanup_old_activities)
    return {"message": "Cleanup scheduled"}
```

### Rate Limiting

```python
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

@router.post("/tasks/research")
@limiter.limit("10/minute")
async def create_research_task(request: Request, task: TaskRequest):
    ...
```

### Authentication

```python
from fastapi import Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer()

async def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    token = credentials.credentials
    # Verify token (JWT, API key, etc.)
    if not is_valid_token(token):
        raise HTTPException(status_code=401, detail="Invalid token")
    return token

@router.post("/tasks/research")
async def create_research_task(
    request: TaskRequest,
    token: str = Depends(verify_token)
):
    ...
```

## Error Handling

### Global Exception Handler

```python
# main.py
from fastapi import Request
from fastapi.responses import JSONResponse

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc) if app.debug else "An error occurred"
        }
    )
```

### Custom Exception Classes

```python
# exceptions.py
class TaskNotFoundError(Exception):
    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        super().__init__(f"Task {agent_id} not found")

class TaskTimeoutError(Exception):
    def __init__(self, agent_id: str, timeout: int):
        self.agent_id = agent_id
        self.timeout = timeout
        super().__init__(f"Task {agent_id} did not complete within {timeout}s")

# main.py
@app.exception_handler(TaskNotFoundError)
async def task_not_found_handler(request: Request, exc: TaskNotFoundError):
    return JSONResponse(
        status_code=404,
        content={"error": "Task not found", "agent_id": exc.agent_id}
    )
```

## Testing

### Test Client Setup

```python
# tests/conftest.py
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import agentexec as ax

from myapp.main import app
from myapp.deps import get_db

# Test database
TEST_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(TEST_DATABASE_URL)
TestingSessionLocal = sessionmaker(bind=engine)

ax.Base.metadata.create_all(bind=engine)

def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db

@pytest.fixture
def client():
    return TestClient(app)
```

### Testing Endpoints

```python
# tests/test_views.py
from unittest.mock import patch, AsyncMock

def test_create_task(client):
    with patch("myapp.views.ax.enqueue") as mock_enqueue:
        mock_enqueue.return_value = AsyncMock(
            agent_id="test-uuid"
        )

        response = client.post("/api/tasks/research", json={
            "company": "Test Corp",
            "focus_areas": ["products"]
        })

        assert response.status_code == 200
        assert "agent_id" in response.json()

def test_get_status_not_found(client):
    response = client.get("/api/tasks/nonexistent/status")
    assert response.status_code == 404
```

## API Documentation

FastAPI automatically generates OpenAPI documentation. Access it at:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### Adding Descriptions

```python
@router.post(
    "/tasks/research",
    response_model=TaskResponse,
    summary="Create Research Task",
    description="Queue a new company research task for AI agent processing.",
    responses={
        200: {"description": "Task queued successfully"},
        429: {"description": "Rate limit exceeded"},
    }
)
async def create_research_task(request: TaskRequest):
    """
    Create a new research task for an AI agent.

    - **company**: Name of the company to research
    - **focus_areas**: Specific areas to focus on (optional)
    - **priority**: Task priority (low or high)
    """
    ...
```

## Next Steps

- [Pipelines](pipelines.md) - Multi-step workflows
- [Production Guide](../deployment/production.md) - Production deployment
- [Docker Deployment](../deployment/docker.md) - Containerization
