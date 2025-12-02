# Runner API Reference

This document covers the agent runner APIs for integrating with AI agent frameworks.

## OpenAIRunner

Runner for OpenAI Agents SDK with automatic lifecycle management.

```python
class OpenAIRunner(BaseAgentRunner):
    def __init__(
        self,
        agent_id: UUID,
        max_turns_recovery: bool = False,
        wrap_up_prompt: str | None = None,
        recovery_turns: int = 5,
        report_status_prompt: str | None = None,
    )
```

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `agent_id` | `UUID` | required | Task identifier for activity tracking |
| `max_turns_recovery` | `bool` | `False` | Enable recovery when max turns exceeded |
| `wrap_up_prompt` | `str \| None` | `None` | Custom prompt for wrap-up phase |
| `recovery_turns` | `int` | `5` | Additional turns for recovery |
| `report_status_prompt` | `str \| None` | `None` | Custom status reporting instructions |

### Example

```python
from uuid import UUID
from agents import Agent
import agentexec as ax

@pool.task("my_task")
async def my_task(agent_id: UUID, context: MyContext):
    runner = ax.OpenAIRunner(
        agent_id=agent_id,
        max_turns_recovery=True,
        recovery_turns=5
    )

    agent = Agent(
        name="My Agent",
        instructions=f"Do the task.\n{runner.prompts.report_status}",
        tools=[runner.tools.report_status],
        model="gpt-4o",
    )

    result = await runner.run(agent, input="Start", max_turns=15)
    return result.final_output
```

---

## run()

Execute an agent with lifecycle management.

```python
async def run(
    self,
    agent: Agent,
    input: str,
    max_turns: int = 10,
    context: RunContext | None = None,
) -> RunResult
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `agent` | `Agent` | required | OpenAI Agent instance |
| `input` | `str` | required | Initial input message |
| `max_turns` | `int` | `10` | Maximum conversation turns |
| `context` | `RunContext \| None` | `None` | Optional run context |

### Returns

`RunResult` - The OpenAI Agents SDK result object.

### RunResult Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `final_output` | `str` | The agent's final response |
| `is_complete` | `bool` | Whether agent completed normally |
| `messages` | `list` | Conversation history |

### Example

```python
result = await runner.run(
    agent,
    input="Research the topic",
    max_turns=15
)

print(result.final_output)
print(f"Complete: {result.is_complete}")
```

---

## run_streamed()

Execute an agent with streaming output.

```python
async def run_streamed(
    self,
    agent: Agent,
    input: str,
    max_turns: int = 10,
    context: RunContext | None = None,
) -> AsyncIterator[StreamEvent]
```

### Parameters

Same as `run()`.

### Yields

`StreamEvent` - Events during execution:
- `TextEvent` - Text chunks
- `ToolCallEvent` - Tool invocations
- `CompleteEvent` - Completion

### Example

```python
async for event in runner.run_streamed(agent, input="...", max_turns=10):
    if event.type == "text":
        print(event.text, end="", flush=True)
```

> **Note**: Streaming support is experimental.

---

## prompts

Access built-in prompt templates.

```python
runner.prompts: _RunnerPrompts
```

### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `report_status` | `str` | Instructions for status reporting |
| `wrap_up` | `str` | Instructions for wrapping up |

### Example

```python
runner = ax.OpenAIRunner(agent_id=agent_id)

# Use in agent instructions
agent = Agent(
    instructions=f"""
    You are a research assistant.

    {runner.prompts.report_status}
    """,
    ...
)
```

### Default report_status Prompt

```
Use the report_activity tool to report your progress on the current task.
Call it periodically with a message describing what you're doing and
an estimated completion percentage (0-100).
```

### Default wrap_up Prompt

```
You've reached the maximum number of turns. Please wrap up your current
task and provide your final output based on what you've accomplished so far.
```

---

## tools

Access built-in tools.

```python
runner.tools: _RunnerTools
```

### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `report_status` | `FunctionTool` | Tool for agents to report progress |

### Example

```python
runner = ax.OpenAIRunner(agent_id=agent_id)

agent = Agent(
    tools=[
        runner.tools.report_status,
        my_other_tool,
    ],
    ...
)
```

### report_status Tool

The tool function signature:

```python
def report_activity(message: str, percentage: int) -> str:
    """
    Report progress on the current task.

    Args:
        message: Description of current progress
        percentage: Estimated completion (0-100)

    Returns:
        Confirmation message
    """
```

When the agent calls this tool, it updates the activity:

```python
ax.activity.update(agent_id, message, completion_percentage=percentage)
```

---

## BaseAgentRunner

Abstract base class for creating custom runners.

```python
class BaseAgentRunner(ABC):
    def __init__(
        self,
        agent_id: UUID,
        report_status_prompt: str | None = None,
    )
```

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `agent_id` | `UUID` | required | Task identifier |
| `report_status_prompt` | `str \| None` | `None` | Custom status prompt |

### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `agent_id` | `UUID` | Task identifier |
| `prompts` | `_RunnerPrompts` | Prompt templates |
| `tools` | `_RunnerTools` | Available tools |

### Abstract Methods

```python
@abstractmethod
async def run(self, agent: Any, input: str, max_turns: int = 10, **kwargs) -> Any:
    """Execute the agent."""
    pass

@abstractmethod
async def run_streamed(self, agent: Any, input: str, max_turns: int = 10, **kwargs):
    """Execute with streaming."""
    pass
```

### Example Subclass

```python
from agentexec.runners.base import BaseAgentRunner

class MyRunner(BaseAgentRunner):
    async def run(self, agent, input: str, max_turns: int = 10, **kwargs):
        ax.activity.update(self.agent_id, ax.CONF.activity_message_started)

        try:
            result = await self._execute(agent, input, max_turns)
            ax.activity.complete(self.agent_id, ax.CONF.activity_message_complete)
            return result
        except Exception as e:
            ax.activity.error(self.agent_id, str(e))
            raise

    async def run_streamed(self, agent, input: str, **kwargs):
        raise NotImplementedError()
```

---

## _RunnerPrompts

Container for prompt templates.

```python
class _RunnerPrompts:
    def __init__(
        self,
        report_status: str | None = None,
        wrap_up: str | None = None,
    )
```

### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `report_status` | `str` | Status reporting instructions |
| `wrap_up` | `str` | Wrap-up instructions |

---

## _RunnerTools

Container for runner tools.

```python
class _RunnerTools:
    def __init__(self, agent_id: UUID)
```

### Properties

#### report_status

```python
@property
def report_status(self) -> FunctionTool
```

Returns a function tool that updates activity when called.

---

## Max Turns Recovery

When `max_turns_recovery=True`:

1. Agent runs until `max_turns` is reached
2. If `MaxTurnsExceeded` is raised, runner catches it
3. Agent continues with `wrap_up_prompt` for `recovery_turns` more
4. Final result is returned

### Example

```python
runner = ax.OpenAIRunner(
    agent_id=agent_id,
    max_turns_recovery=True,
    wrap_up_prompt="Please summarize your findings and provide a final answer.",
    recovery_turns=5
)

# Agent gets 15 turns, then 5 recovery turns if needed
result = await runner.run(agent, input="Complex task", max_turns=15)
```

### Custom Wrap-up Prompt

```python
runner = ax.OpenAIRunner(
    agent_id=agent_id,
    max_turns_recovery=True,
    wrap_up_prompt="""
    You've reached the turn limit. Please:
    1. Summarize what you've accomplished
    2. List any remaining steps that would be needed
    3. Provide your best answer given the work completed
    """
)
```

---

## Error Handling

The runner automatically handles errors:

```python
# Errors are logged to activity
@pool.task("my_task")
async def my_task(agent_id: UUID, context: MyContext):
    runner = ax.OpenAIRunner(agent_id=agent_id)

    # If run() raises:
    # 1. Activity status → ERROR
    # 2. Error message logged
    # 3. Exception re-raised
    result = await runner.run(agent, input="...", max_turns=10)
```

### Manual Error Handling

```python
@pool.task("my_task")
async def my_task(agent_id: UUID, context: MyContext):
    runner = ax.OpenAIRunner(agent_id=agent_id)

    try:
        result = await runner.run(agent, input="...", max_turns=10)
        return result.final_output
    except MaxTurnsExceeded:
        ax.activity.update(agent_id, "Max turns reached, returning partial")
        return {"partial": True}
    except Exception as e:
        ax.activity.error(agent_id, f"Custom error: {e}")
        raise CustomError(str(e))
```
