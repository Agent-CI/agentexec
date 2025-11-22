"""Agent Runner - Production-ready orchestration for OpenAI Agents.

This library provides:
- Background worker pool with Redis-backed task queue
- Activity tracking with pluggable storage backends
- OpenAI Agents SDK runner with max turns recovery
- Coordination primitives (counters, locks, barriers)
- Workflow engine for complex agent orchestration

Example:
    from agents import Agent
    import agentexec as ax


    # create a pool to manage background tasks
    pool = ax.Pool()

    # register a background task
    @pool.task("research_company")
    async def research_company(payload: dict, agent_id: str):
        runner = ax.OpenAIRunner(
            status_tracking=True,
            max_turns=5,
        )
        agent = Agent(tools=[runner.tools.report_status], ...)
        result = await runner.run(
            agent,
            input=f"Research {payload['company_name']}",
        )
        return result

    # fork and start the worker pool
    pool.start()

    # queue a task from anywhere
    task = ax.Task(task_type="research_company", payload={"company_name": "Acme Corp"})
    agent_id = ax.enqueue(task)
"""

from importlib.metadata import PackageNotFoundError, version

from agentexec.config import CONF
from agentexec.core.models import Base
from agentexec.core.queue import Priority, dequeue, enqueue
from agentexec.core.task import Task, TaskHandler, TaskHandlerKwargs
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
    "TaskHandler",
    "Task",
    "TaskHandlerKwargs",
    "Priority",
    "enqueue",
    "dequeue",
    "BaseAgentRunner",
]

# OpenAI runner is only available if agents package is installed
try:
    from agentexec.runners import OpenAIRunner

    __all__.append("OpenAIRunner")
except ImportError:
    pass
