"""Agent Runner - Production-ready orchestration for OpenAI Agents.

This library provides:
- Background worker pool with Redis-backed task queue
- Activity tracking with pluggable storage backends
- OpenAI Agents SDK runner with max turns recovery
- Type-safe task context via Pydantic models

Example:
    from uuid import UUID
    from pydantic import BaseModel
    import agentexec as ax

    # Create worker pool
    engine = create_engine("sqlite:///agents.db")
    pool = ax.WorkerPool(engine=engine)

    # Define typed context for your task
    class ResearchContext(BaseModel):
        company_name: str

    # Register task with pool
    @pool.task("research_company")
    async def research_company(agent_id: UUID, context: ResearchContext):
        print(f"Researching {context.company_name}")  # Typed!

    # Enqueue from anywhere (e.g., web handler)
    task = ax.enqueue("research_company", ResearchContext(company_name="Acme"))

    # Start worker pool
    pool.start()
"""

from importlib.metadata import PackageNotFoundError, version

from agentexec.config import CONF
from agentexec.core.db import Base
from agentexec.core.queue import Priority, enqueue
from agentexec.core.task import Task, TaskDefinition, TaskHandler, TaskHandlerKwargs
from agentexec.core.worker import WorkerPool
from agentexec.runners import BaseAgentRunner

try:
    __version__ = version("agent-runner")
except PackageNotFoundError:
    __version__ = "0.0.0.dev"

__all__ = [
    "CONF",
    "Base",
    "WorkerPool",
    "Task",
    "TaskDefinition",
    "TaskHandler",
    "TaskHandlerKwargs",
    "Priority",
    "enqueue",
    "BaseAgentRunner",
]

# OpenAI runner is only available if agents package is installed
try:
    from agentexec.runners import OpenAIRunner

    __all__.append("OpenAIRunner")
except ImportError:
    pass
