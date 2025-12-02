# agentexec

**Production-ready task orchestration for OpenAI Agents SDK**

agentexec is a Python library that provides robust, scalable infrastructure for running AI agents in production environments. It combines Redis-backed task queues, multi-process worker pools, and comprehensive activity tracking to make deploying and monitoring AI agents straightforward and reliable.

## Key Features

- **Multi-Process Worker Pool** - Spawn N worker processes that poll a Redis queue and execute tasks in parallel
- **Redis Task Queue** - Reliable job distribution with priority support (HIGH/LOW)
- **Automatic Activity Tracking** - Full lifecycle management (QUEUED → RUNNING → COMPLETE/ERROR) with progress logging
- **OpenAI Agents Integration** - Drop-in runner with max turns recovery and status reporting
- **Agent Self-Reporting** - Built-in tools for agents to report progress during execution
- **Type-Safe Contexts** - Pydantic BaseModel for task context with IDE autocomplete
- **Graceful Shutdown** - Timeout-based worker shutdown with signal handling
- **Pipelines** - Multi-step workflow orchestration with parallel task execution
- **Result Storage** - Results cached in Redis with configurable TTL for pipeline coordination

## Why agentexec?

Building production AI agent systems requires more than just calling an LLM API. You need:

- **Reliable task execution** - Tasks should survive process restarts and be retried on failure
- **Observability** - Know what your agents are doing, when they started, and how they're progressing
- **Scalability** - Run multiple agents in parallel across multiple worker processes
- **Type safety** - Catch errors at development time, not in production
- **Graceful degradation** - Handle failures without bringing down your entire system

agentexec provides all of this out of the box, letting you focus on building your agents rather than infrastructure.

## Quick Example

```python
from uuid import UUID
from pydantic import BaseModel
from agents import Agent
import agentexec as ax

# Define input and output schemas
class ResearchContext(BaseModel):
    company: str
    focus_areas: list[str]

class ResearchResult(BaseModel):
    summary: str
    insights: list[str]

# Create a worker pool
pool = ax.WorkerPool(engine=engine)

# Register a task
@pool.task("research_company")
async def research_company(agent_id: UUID, context: ResearchContext) -> ResearchResult:
    runner = ax.OpenAIRunner(agent_id=agent_id)

    agent = Agent(
        name="Research Agent",
        instructions=(
            f"Research {context.company} focusing on {context.focus_areas}"
            f"{runner.prompts.report_status}"
        ),
        tools=[runner.tools.report_status],
        model="gpt-5",
        output_type=ResearchResult,
    )

    result = await runner.run(agent, input="Begin research", max_turns=15)
    return result.final_output_as(ResearchResult)

# Start workers
pool.run()

# Queue a task (from anywhere in your app)
task = await ax.enqueue("research_company", ResearchContext(
    company="Anthropic",
    focus_areas=["AI safety", "product offerings"]
))
result = await ax.get_result(task)
```

## Documentation

### Getting Started

- [Installation](getting-started/installation.md) - Install agentexec and its dependencies
- [Quick Start](getting-started/quickstart.md) - Get up and running in 5 minutes
- [Configuration](getting-started/configuration.md) - Configure agentexec for your environment

### Concepts

- [Architecture](concepts/architecture.md) - Understand how agentexec works
- [Task Lifecycle](concepts/task-lifecycle.md) - Learn about task states and transitions
- [Worker Pool](concepts/worker-pool.md) - Multi-process execution model
- [Activity Tracking](concepts/activity-tracking.md) - Monitor and log agent progress

### Guides

- [Basic Usage](guides/basic-usage.md) - Common patterns and best practices
- [FastAPI Integration](guides/fastapi-integration.md) - Build APIs with agentexec
- [Pipelines](guides/pipelines.md) - Orchestrate multi-step workflows
- [OpenAI Runner](guides/openai-runner.md) - Advanced runner configuration
- [Custom Runners](guides/custom-runners.md) - Extend agentexec for other frameworks

### API Reference

- [Core API](api-reference/core.md) - Task, queue, and configuration
- [Activity API](api-reference/activity.md) - Activity tracking functions
- [Runner API](api-reference/runner.md) - Agent runner classes
- [Pipeline API](api-reference/pipeline.md) - Pipeline orchestration

### Deployment

- [Docker Deployment](deployment/docker.md) - Deploy with Docker
- [Production Guide](deployment/production.md) - Production best practices

### Community

- [Contributing](contributing.md) - How to contribute to agentexec
- [Changelog](https://github.com/Agent-CI/agentexec/blob/main/CHANGELOG.md) - Release history

## Requirements

- Python 3.11+
- Redis 7.0+
- SQLAlchemy-compatible database (PostgreSQL, MySQL, SQLite)

## License

agentexec is released under the [MIT License](https://github.com/Agent-CI/agentexec/blob/main/LICENSE).
