# LangChain Agents FastAPI Example

This example demonstrates a complete FastAPI application using **agentexec** to orchestrate LangChain agents in production.

## What This Example Demonstrates

### Core Features

- **Background worker pool** (`worker.py`) - Multi-process task execution with Redis queue
- **LangChainRunner integration** - Automatic activity tracking for agent lifecycle
- **ReAct agent pattern** - Using LangChain's create_react_agent for reasoning
- **Custom FastAPI routes** (`views.py`) - Building your own API on agentexec's public API
- **Database session management** (`main.py`) - Standard SQLAlchemy patterns with full control
- **Agent self-reporting** - Agents report progress via built-in `report_activity` tool
- **Max iterations recovery** - Automatic handling of conversation limits with wrap-up prompts

### Key Patterns Shown

**Task Registration:**
```python
@pool.task("research_company")
async def research_company(
    agent_id: UUID,
    context: ResearchCompanyContext,
) -> ResearchCompanyResult:
    # Create LangChain agent
    llm = ChatOpenAI(model="gpt-4o-mini")
    agent = create_react_agent(llm, tools, prompt)
    agent_executor = AgentExecutor(agent=agent, tools=tools)

    # Wrap with agentexec runner
    runner = ax.LangChainRunner(
        agent_id=agent_id,
        agent_executor=agent_executor,
        max_turns_recovery=True
    )

    # Execute with activity tracking
    result = await runner.run(context.input_prompt, max_iterations=15)
    return ResearchCompanyResult(summary=result["output"])
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
context = ResearchCompanyContext(company_name="Acme")
task = await ax.enqueue(
    "research_company",
    context,
    priority=ax.Priority.HIGH,
)
```

## Quick Start

```bash
# Install dependencies
cd examples/langchain-agents-fastapi
uv sync

# Start Redis
docker run -d -p 6379:6379 redis:latest

# Set API key
export OPENAI_API_KEY="your-key"

# Run migrations
alembic upgrade head

# Start worker (terminal 1)
python -m langchain_agents_fastapi.worker

# Start API server (terminal 2)
uvicorn langchain_agents_fastapi.main:app --reload
```

## Try It

Queue a task:
```bash
curl -X POST "http://localhost:8000/api/tasks/research_company" \
  -H "Content-Type: application/json" \
  -d '{
    "company_name": "Anthropic",
    "input_prompt": "Focus on their AI safety research"
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

## LangChain-Specific Features

### Agent Types Supported

This example demonstrates the **ReAct agent** pattern, but agentexec's LangChainRunner supports:

- **ReAct agents** (`create_react_agent`) - Reasoning and acting
- **Tool-calling agents** (`create_tool_calling_agent`) - Structured tool use
- **Structured chat agents** (`create_structured_chat_agent`) - Multi-turn conversations
- **LangGraph agents** - Durable execution with state persistence

### Streaming Support

LangChain agents support streaming via `astream_events`:

```python
async for event in runner.run_streamed(input_prompt, max_iterations=15):
    if event["event"] == "on_chat_model_stream":
        print(event["data"]["chunk"].content, end="")
```

### Custom Tools

Tools in LangChain use the `@tool` decorator:

```python
from langchain_core.tools import tool

@tool
def search_company_info(company_name: str, query_type: str) -> str:
    """Search for company information."""
    return f"Information about {company_name}..."
```

## Differences from OpenAI Agents SDK

| Feature | OpenAI Agents SDK | LangChain |
|---------|------------------|-----------|
| Agent creation | `Agent(name, instructions, tools)` | `create_react_agent(llm, tools, prompt)` |
| Tool decorator | `@function_tool` | `@tool` |
| Execution | `Runner.run()` | `AgentExecutor.invoke()` |
| Streaming | `run_streamed()` | `astream_events()` |
| Max iterations | `MaxTurnsExceeded` exception | Max iterations in config |
| LLM providers | OpenAI only | OpenAI, Anthropic, local models, etc. |

## Learn More

- [LangChain Documentation](https://python.langchain.com/)
- [AgentExec Documentation](https://github.com/Agent-CI/agentexec)
- [ReAct Agent Pattern](https://python.langchain.com/docs/modules/agents/agent_types/react/)
