import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
import agentexec as ax

from main import get_db


router = APIRouter()


class TaskRequest(BaseModel):
    """Request to queue a new task."""

    task_name: str
    payload: dict
    priority: ax.Priority = ax.Priority.LOW


class TaskResponse(BaseModel):
    """Response after queuing a task."""

    agent_id: uuid.UUID
    message: str


@router.post(
    "/api/tasks",
    response_model=TaskResponse,
)
async def queue_task(request: TaskRequest, db: Session = Depends(get_db)):
    """Queue a new background task.

    The task will be picked up by a worker and executed asynchronously.
    Use the agent_id to track progress via the activity endpoints.
    """
    task = ax.enqueue(
        request.task_name,
        request.payload,
        priority=request.priority,
    )

    return TaskResponse(
        agent_id=task.agent_id,
        message=f"Task queued successfully. Track progress at /api/agents/activity/{task.agent_id}",
    )


@router.get(
    "/api/agents/activity",
    response_model=ax.activity.ActivityListSchema,
)
async def list_agents(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
    db: Session = Depends(get_db),
):
    """List all activities with pagination.

    Uses agentexec's public API: activity.list()
    The database session is automatically managed by FastAPI dependency injection.
    """
    return ax.activity.list(db, page=page, page_size=page_size)


@router.get(
    "/api/agents/activity/{agent_id}",
    response_model=ax.activity.ActivityDetailSchema,
)
async def get_agent(agent_id: str, db: Session = Depends(get_db)):
    """Get detailed information about a specific agent including full log history.

    Uses agentexec's public API: activity.detail()
    The database session is automatically managed by FastAPI dependency injection.
    """
    activity_obj = ax.activity.detail(db, agent_id)

    if not activity_obj:
        raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")

    return activity_obj
