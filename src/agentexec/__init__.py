"""`agentexec` - Background task orchestration for AI Agents.

Example:
    from uuid import UUID
    from pydantic import BaseModel
    import agentexec as ax

    pool = ax.Pool(database_url="sqlite:///tasks.db")

    class Input(BaseModel):
        query: str

    class Output(BaseModel):
        answer: str

    @pool.task("search")
    async def search(agent_id: UUID, context: Input) -> Output:
        return Output(answer=f"Result for {context.query}")

    # Enqueue from anywhere (returns Task with agent_id for tracking)
    task = await ax.enqueue("search", Input(query="hello"))

    pool.run()  # Blocks and processes tasks
"""

from importlib.metadata import PackageNotFoundError, version

from agentexec.config import CONF
from agentexec.core.db import Base
from agentexec.core.queue import Priority, enqueue
from agentexec.core.results import gather, get_result
from agentexec.core.task import Task
from agentexec import activity
from agentexec.worker import Pool
from agentexec.pipeline import Pipeline
from agentexec.runners import BaseAgentRunner
from agentexec.tracker import Tracker

try:
    __version__ = version("agentexec")
except PackageNotFoundError:
    __version__ = "0.0.0.dev"

__all__ = [
    "CONF",
    "Base",
    "BaseAgentRunner",
    "Pipeline",
    "Tracker",
    "Pool",
    "Task",
    "Priority",
    "activity",
    "enqueue",
    "gather",
    "get_result",
]

try:
    from agentexec.runners.openai import OpenAIRunner  # type: ignore[possibly-missing-import]

    __all__.append("OpenAIRunner")
except ImportError:
    pass
