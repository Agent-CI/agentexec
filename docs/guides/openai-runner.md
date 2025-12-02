# OpenAI Runner

The OpenAI Runner integrates agentexec with the [OpenAI Agents SDK](https://github.com/openai/openai-agents-python), providing automatic lifecycle management, progress reporting, and error recovery.

## Overview

```python
from uuid import UUID
from agents import Agent
import agentexec as ax

@pool.task("my_task")
async def my_task(agent_id: UUID, context: MyContext):
    # Create runner with agent_id for tracking
    runner = ax.OpenAIRunner(agent_id=agent_id)

    # Create your agent
    agent = Agent(
        name="My Agent",
        instructions="You are a helpful assistant.",
        model="gpt-4o",
    )

    # Run the agent - lifecycle is managed automatically
    result = await runner.run(agent, input="Hello!", max_turns=10)

    return result.final_output
```

## Creating a Runner

### Basic Usage

```python
runner = ax.OpenAIRunner(agent_id=agent_id)
```

### With All Options

```python
runner = ax.OpenAIRunner(
    agent_id=agent_id,
    max_turns_recovery=True,          # Continue if max turns exceeded
    wrap_up_prompt="Please wrap up.", # Custom wrap-up message
    recovery_turns=5,                 # Turns for recovery phase
    report_status_prompt=None,        # Custom status reporting instructions
)
```

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `agent_id` | `UUID` | required | Task identifier for activity tracking |
| `max_turns_recovery` | `bool` | `False` | Enable recovery when max turns exceeded |
| `wrap_up_prompt` | `str \| None` | `None` | Custom prompt for wrap-up phase |
| `recovery_turns` | `int` | `5` | Additional turns for recovery |
| `report_status_prompt` | `str \| None` | `None` | Custom instructions for status reporting |

## Running Agents

### Basic Run

```python
result = await runner.run(
    agent,
    input="What is the capital of France?",
    max_turns=10
)

print(result.final_output)  # "The capital of France is Paris."
```

### With Context

```python
from agents import RunContext

result = await runner.run(
    agent,
    input="Process this data",
    max_turns=15,
    context=RunContext(
        custom_data={"key": "value"}
    )
)
```

### Run Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `agent` | `Agent` | The OpenAI agent to run |
| `input` | `str` | Initial input message |
| `max_turns` | `int` | Maximum conversation turns |
| `context` | `RunContext \| None` | Optional context for the run |

## Lifecycle Management

The runner automatically manages the activity lifecycle:

```
runner.run() called
        │
        ▼
┌───────────────────┐
│ Update activity   │
│ status: RUNNING   │
│ message: "Started"│
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│   Execute agent   │
│   (may report     │
│    progress)      │
└─────────┬─────────┘
          │
    ┌─────┴─────┐
    ▼           ▼
Success      Error
    │           │
    ▼           ▼
┌─────────┐ ┌─────────┐
│COMPLETE │ │  ERROR  │
└─────────┘ └─────────┘
```

## Progress Reporting

### Built-in Tool

The runner provides a tool for agents to report progress:

```python
runner = ax.OpenAIRunner(agent_id=agent_id)

agent = Agent(
    name="Reporter",
    instructions=f"""
    You are a research assistant.
    {runner.prompts.report_status}
    """,
    tools=[runner.tools.report_status],
    model="gpt-4o",
)
```

### How It Works

1. `runner.tools.report_status` is a function tool the agent can call
2. When called, it updates the activity with the message and percentage
3. Progress is immediately visible via the activity API

### Agent Tool Call

The agent calls the tool like this:

```json
{
  "name": "report_activity",
  "arguments": {
    "message": "Found 5 relevant articles",
    "percentage": 50
  }
}
```

### Custom Reporting Prompt

Customize how you instruct the agent to report:

```python
runner = ax.OpenAIRunner(
    agent_id=agent_id,
    report_status_prompt="""
    PROGRESS REPORTING:
    - Use report_activity after each major step
    - Include specific numbers (e.g., "Processed 5/10 items")
    - Estimate completion percentage (0-100)
    - Report at least every 2-3 actions
    """
)

agent = Agent(
    instructions=f"""
    You are a data processor.
    {runner.prompts.report_status}
    """,
    tools=[runner.tools.report_status],
    ...
)
```

## Max Turns Recovery

When `max_turns_recovery=True`, the runner handles `MaxTurnsExceeded` gracefully:

```python
runner = ax.OpenAIRunner(
    agent_id=agent_id,
    max_turns_recovery=True,
    wrap_up_prompt="Please provide your final answer based on what you've done so far.",
    recovery_turns=5
)

result = await runner.run(agent, input="Complex task...", max_turns=10)
# If agent exceeds 10 turns, it gets 5 more turns to wrap up
```

### Recovery Flow

```
Agent running (max_turns=10)
          │
          ▼
MaxTurnsExceeded raised
          │
          ▼
┌───────────────────────────┐
│ Continue with wrap_up     │
│ prompt for recovery_turns │
│ (5 additional turns)      │
└─────────────┬─────────────┘
              │
              ▼
    Agent completes wrap-up
              │
              ▼
        Return result
```

### Custom Wrap-up Prompt

```python
runner = ax.OpenAIRunner(
    agent_id=agent_id,
    max_turns_recovery=True,
    wrap_up_prompt="""
    You've reached the turn limit. Please:
    1. Summarize what you've accomplished
    2. List any remaining steps
    3. Provide your best current answer
    """
)
```

## Prompt Templates

Access built-in prompt templates:

```python
runner = ax.OpenAIRunner(agent_id=agent_id)

# Status reporting instructions
print(runner.prompts.report_status)
# "Use the report_activity tool to report your progress..."

# Wrap-up instructions (when max turns recovery is enabled)
print(runner.prompts.wrap_up)
# "Please wrap up your current task and provide your final output..."
```

### Using Templates

```python
agent = Agent(
    name="Research Agent",
    instructions=f"""
    You are a research assistant.

    Your task is to research the given topic thoroughly.

    {runner.prompts.report_status}
    """,
    tools=[runner.tools.report_status],
    model="gpt-4o",
)
```

## Error Handling

### Automatic Error Recording

Errors are automatically recorded to the activity:

```python
@pool.task("risky_task")
async def risky_task(agent_id: UUID, context: MyContext):
    runner = ax.OpenAIRunner(agent_id=agent_id)

    # If this raises any exception:
    # 1. Activity status is set to ERROR
    # 2. Error message is logged
    # 3. Exception is re-raised
    result = await runner.run(agent, input="...", max_turns=10)
```

### Manual Error Handling

```python
@pool.task("handled_task")
async def handled_task(agent_id: UUID, context: MyContext):
    runner = ax.OpenAIRunner(agent_id=agent_id)

    try:
        result = await runner.run(agent, input="...", max_turns=10)
        return result.final_output
    except MaxTurnsExceeded:
        ax.activity.update(agent_id, "Task incomplete - max turns reached")
        return {"status": "incomplete", "reason": "max_turns"}
    except Exception as e:
        ax.activity.error(agent_id, f"Unexpected error: {e}")
        raise
```

## Advanced Usage

### Multiple Agents in One Task

```python
@pool.task("multi_agent")
async def multi_agent(agent_id: UUID, context: MyContext):
    runner = ax.OpenAIRunner(agent_id=agent_id)

    # First agent: Research
    ax.activity.update(agent_id, "Starting research phase", 0)
    research_agent = Agent(name="Researcher", ...)
    research_result = await runner.run(research_agent, input="Research...", max_turns=10)

    # Second agent: Analysis
    ax.activity.update(agent_id, "Starting analysis phase", 50)
    analysis_agent = Agent(name="Analyst", ...)
    analysis_result = await runner.run(
        analysis_agent,
        input=f"Analyze: {research_result.final_output}",
        max_turns=10
    )

    ax.activity.complete(agent_id, "Both phases complete", 100)
    return analysis_result.final_output
```

### Custom Tools with Runner

```python
from agents import function_tool

@function_tool
def search_database(query: str) -> str:
    """Search the database for relevant records."""
    # Your search logic
    return "Search results..."

@pool.task("search_task")
async def search_task(agent_id: UUID, context: SearchContext):
    runner = ax.OpenAIRunner(agent_id=agent_id)

    agent = Agent(
        name="Search Agent",
        instructions=f"""
        Search for information using the available tools.
        {runner.prompts.report_status}
        """,
        tools=[
            search_database,
            runner.tools.report_status,
        ],
        model="gpt-4o",
    )

    result = await runner.run(agent, input=context.query, max_turns=15)
    return result.final_output
```

### Handoffs Between Agents

```python
from agents import Agent, handoff

@pool.task("handoff_task")
async def handoff_task(agent_id: UUID, context: MyContext):
    runner = ax.OpenAIRunner(agent_id=agent_id)

    # Define specialist agents
    researcher = Agent(name="Researcher", instructions="Research topics", model="gpt-4o")
    writer = Agent(name="Writer", instructions="Write content", model="gpt-4o")

    # Main agent with handoffs
    coordinator = Agent(
        name="Coordinator",
        instructions=f"""
        Coordinate research and writing tasks.
        Hand off to specialists as needed.
        {runner.prompts.report_status}
        """,
        tools=[runner.tools.report_status],
        handoffs=[
            handoff(researcher, description="Hand off research tasks"),
            handoff(writer, description="Hand off writing tasks"),
        ],
        model="gpt-4o",
    )

    result = await runner.run(coordinator, input=context.task, max_turns=20)
    return result.final_output
```

## Streaming (Experimental)

The runner supports streaming output:

```python
async def stream_task(agent_id: UUID, context: MyContext):
    runner = ax.OpenAIRunner(agent_id=agent_id)

    async for event in runner.run_streamed(agent, input="...", max_turns=10):
        if event.type == "text":
            print(event.text, end="", flush=True)
        elif event.type == "tool_call":
            print(f"\nCalling tool: {event.tool_name}")
```

> **Note**: Streaming support is experimental and may change in future versions.

## Best Practices

### 1. Always Use agent_id

Always pass the task's `agent_id` to the runner:

```python
# Good
runner = ax.OpenAIRunner(agent_id=agent_id)

# Bad - no tracking
runner = ax.OpenAIRunner(agent_id=uuid4())  # New UUID loses tracking
```

### 2. Include Progress Reporting

For long-running tasks, always include progress reporting:

```python
agent = Agent(
    instructions=f"""
    {your_instructions}

    {runner.prompts.report_status}
    """,
    tools=[runner.tools.report_status, ...],
)
```

### 3. Set Appropriate Max Turns

Choose `max_turns` based on task complexity:

```python
# Simple Q&A
result = await runner.run(agent, input="...", max_turns=5)

# Research task
result = await runner.run(agent, input="...", max_turns=15)

# Complex multi-step task
result = await runner.run(agent, input="...", max_turns=30)
```

### 4. Enable Recovery for Important Tasks

```python
# For tasks that shouldn't fail on turn limits
runner = ax.OpenAIRunner(
    agent_id=agent_id,
    max_turns_recovery=True,
    recovery_turns=5
)
```

### 5. Handle Results Appropriately

```python
result = await runner.run(agent, input="...", max_turns=10)

# Access the final output
output = result.final_output

# Check if complete
if result.is_complete:
    return {"status": "complete", "output": output}
else:
    return {"status": "incomplete", "partial": output}
```

## Troubleshooting

### Agent Not Reporting Progress

1. Verify the tool is included:
   ```python
   tools=[runner.tools.report_status]
   ```

2. Verify instructions include the prompt:
   ```python
   instructions=f"...{runner.prompts.report_status}"
   ```

3. Make instructions explicit about when to report

### Max Turns Exceeded Frequently

1. Increase `max_turns`
2. Simplify agent instructions
3. Enable `max_turns_recovery`
4. Break task into smaller steps

### Activity Not Updating

1. Verify `agent_id` matches the task
2. Check database connection
3. Ensure `ax.Base.metadata.create_all()` was called

## Next Steps

- [Custom Runners](custom-runners.md) - Create runners for other frameworks
- [Activity Tracking](../concepts/activity-tracking.md) - Understand activity system
- [Pipelines](pipelines.md) - Multi-step workflows
