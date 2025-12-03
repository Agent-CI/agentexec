# Custom Runners

agentexec provides a `BaseAgentRunner` class that you can extend to create runners for agent frameworks other than OpenAI. This guide shows how to create custom runners while maintaining activity tracking and progress reporting.

## Overview

The runner architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                      BaseAgentRunner                        │
├─────────────────────────────────────────────────────────────┤
│  • agent_id: UUID                                           │
│  • prompts: _RunnerPrompts (report_status, wrap_up)        │
│  • tools: _RunnerTools (report_status tool)                │
├─────────────────────────────────────────────────────────────┤
│  Methods:                                                   │
│  • run() - abstract, implement in subclass                 │
│  • run_streamed() - abstract, implement in subclass        │
└─────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │                   │
           ┌────────▼────────┐  ┌───────▼───────┐
           │  OpenAIRunner   │  │ YourRunner    │
           │  (built-in)     │  │ (custom)      │
           └─────────────────┘  └───────────────┘
```

## Creating a Custom Runner

### Basic Structure

```python
from uuid import UUID
from typing import Any
import agentexec as ax
from agentexec.runners.base import BaseAgentRunner

class MyFrameworkRunner(BaseAgentRunner):
    """Runner for MyFramework agents."""

    def __init__(
        self,
        agent_id: UUID,
        # Add your custom parameters
        api_key: str | None = None,
        **kwargs
    ):
        super().__init__(agent_id, **kwargs)
        self.api_key = api_key

    async def run(
        self,
        agent: Any,  # Your framework's agent type
        input: str,
        max_turns: int = 10,
        **kwargs
    ) -> Any:
        """
        Run the agent with lifecycle management.

        Args:
            agent: Your framework's agent instance
            input: Initial input message
            max_turns: Maximum conversation turns

        Returns:
            Your framework's result type
        """
        # 1. Update status to RUNNING
        ax.activity.update(
            self.agent_id,
            ax.CONF.activity_message_started
        )

        try:
            # 2. Execute your framework's agent
            result = await self._execute_agent(agent, input, max_turns)

            # 3. Mark as complete
            ax.activity.complete(
                self.agent_id,
                ax.CONF.activity_message_complete
            )

            return result

        except Exception as e:
            # 4. Mark as error
            ax.activity.error(
                self.agent_id,
                ax.CONF.activity_message_error.format(error=str(e))
            )
            raise

    async def _execute_agent(self, agent, input: str, max_turns: int):
        """Execute the agent - implement your framework's logic."""
        # Your framework-specific execution
        pass

    async def run_streamed(self, agent: Any, input: str, max_turns: int = 10, **kwargs):
        """Streaming version - optional."""
        raise NotImplementedError("Streaming not supported")
```

## Complete Example: LangChain Runner

Here's a complete example for a LangChain-style framework:

```python
from uuid import UUID
from typing import Any, AsyncIterator
import agentexec as ax
from agentexec.runners.base import BaseAgentRunner

class LangChainRunner(BaseAgentRunner):
    """Runner for LangChain agents with agentexec integration."""

    def __init__(
        self,
        agent_id: UUID,
        verbose: bool = False,
        **kwargs
    ):
        super().__init__(agent_id, **kwargs)
        self.verbose = verbose

    async def run(
        self,
        agent,  # AgentExecutor
        input: str,
        max_iterations: int = 15,
        **kwargs
    ):
        """
        Run a LangChain agent with activity tracking.

        Args:
            agent: LangChain AgentExecutor instance
            input: Input string or dict
            max_iterations: Maximum agent iterations

        Returns:
            Agent output dict
        """
        # Update status to RUNNING
        ax.activity.update(
            self.agent_id,
            ax.CONF.activity_message_started
        )

        try:
            # Prepare input
            if isinstance(input, str):
                input_dict = {"input": input}
            else:
                input_dict = input

            # Add callbacks for progress tracking
            callbacks = self._create_callbacks()

            # Run the agent
            result = await agent.ainvoke(
                input_dict,
                config={
                    "callbacks": callbacks,
                    "max_iterations": max_iterations,
                }
            )

            # Mark complete
            ax.activity.complete(
                self.agent_id,
                ax.CONF.activity_message_complete
            )

            return result

        except Exception as e:
            ax.activity.error(
                self.agent_id,
                ax.CONF.activity_message_error.format(error=str(e))
            )
            raise

    def _create_callbacks(self):
        """Create LangChain callbacks for progress tracking."""
        from langchain.callbacks.base import AsyncCallbackHandler

        agent_id = self.agent_id

        class ProgressCallback(AsyncCallbackHandler):
            def __init__(self):
                self.step_count = 0

            async def on_agent_action(self, action, **kwargs):
                self.step_count += 1
                ax.activity.update(
                    agent_id,
                    f"Executing action: {action.tool}",
                    percentage=min(self.step_count * 10, 90)
                )

            async def on_tool_end(self, output, **kwargs):
                ax.activity.update(
                    agent_id,
                    f"Tool completed",
                )

        return [ProgressCallback()]

    async def run_streamed(self, agent, input: str, **kwargs) -> AsyncIterator:
        """Stream agent execution."""
        ax.activity.update(self.agent_id, ax.CONF.activity_message_started)

        try:
            input_dict = {"input": input} if isinstance(input, str) else input

            async for event in agent.astream_events(input_dict, version="v1"):
                yield event

            ax.activity.complete(self.agent_id, ax.CONF.activity_message_complete)

        except Exception as e:
            ax.activity.error(
                self.agent_id,
                ax.CONF.activity_message_error.format(error=str(e))
            )
            raise
```

### Using the LangChain Runner

```python
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain_openai import ChatOpenAI
from langchain.tools import tool

@tool
def search(query: str) -> str:
    """Search for information."""
    return f"Results for: {query}"

@pool.task("langchain_research")
async def langchain_research(agent_id: UUID, context: ResearchContext):
    runner = LangChainRunner(agent_id=agent_id)

    llm = ChatOpenAI(model="gpt-4o")
    agent = create_openai_tools_agent(llm, [search], prompt)
    executor = AgentExecutor(agent=agent, tools=[search])

    result = await runner.run(executor, input=context.query)
    return result["output"]
```

## Example: Anthropic Claude Runner

```python
from uuid import UUID
from anthropic import AsyncAnthropic
import agentexec as ax
from agentexec.runners.base import BaseAgentRunner

class ClaudeRunner(BaseAgentRunner):
    """Runner for Anthropic Claude with tool use."""

    def __init__(
        self,
        agent_id: UUID,
        model: str = "claude-3-opus-20240229",
        **kwargs
    ):
        super().__init__(agent_id, **kwargs)
        self.model = model
        self.client = AsyncAnthropic()

    async def run(
        self,
        system_prompt: str,
        input: str,
        tools: list = None,
        max_turns: int = 10,
        **kwargs
    ):
        """Run Claude with tool use loop."""
        ax.activity.update(self.agent_id, ax.CONF.activity_message_started)

        messages = [{"role": "user", "content": input}]
        tools = tools or []

        try:
            for turn in range(max_turns):
                # Call Claude
                response = await self.client.messages.create(
                    model=self.model,
                    max_tokens=4096,
                    system=system_prompt,
                    tools=tools,
                    messages=messages,
                )

                # Check for tool use
                if response.stop_reason == "tool_use":
                    # Process tool calls
                    tool_results = await self._process_tools(response, tools)
                    messages.append({"role": "assistant", "content": response.content})
                    messages.append({"role": "user", "content": tool_results})

                    # Report progress
                    progress = int((turn + 1) / max_turns * 100)
                    ax.activity.update(
                        self.agent_id,
                        f"Turn {turn + 1}: Processing tool calls",
                        percentage=min(progress, 90)
                    )
                else:
                    # Complete
                    ax.activity.complete(
                        self.agent_id,
                        ax.CONF.activity_message_complete
                    )
                    return self._extract_text(response)

            # Max turns reached
            ax.activity.complete(
                self.agent_id,
                "Max turns reached"
            )
            return self._extract_text(response)

        except Exception as e:
            ax.activity.error(
                self.agent_id,
                ax.CONF.activity_message_error.format(error=str(e))
            )
            raise

    async def _process_tools(self, response, tools):
        """Process tool calls and return results."""
        results = []
        for block in response.content:
            if block.type == "tool_use":
                # Find and execute tool
                tool = next((t for t in tools if t["name"] == block.name), None)
                if tool and "handler" in tool:
                    result = await tool["handler"](**block.input)
                else:
                    result = f"Tool {block.name} not found"

                results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": str(result)
                })

        return results

    def _extract_text(self, response):
        """Extract text from response."""
        for block in response.content:
            if hasattr(block, "text"):
                return block.text
        return ""

    async def run_streamed(self, **kwargs):
        raise NotImplementedError()
```

## Adding Custom Tools

### Report Status Tool

The base runner provides a report_status tool:

```python
class BaseAgentRunner:
    @property
    def tools(self):
        return _RunnerTools(self.agent_id)

class _RunnerTools:
    def __init__(self, agent_id: UUID):
        self.agent_id = agent_id

    @property
    def report_status(self):
        """Return a tool for reporting status."""
        from agents import function_tool

        agent_id = self.agent_id

        @function_tool
        def report_activity(message: str, percentage: int) -> str:
            """Report progress on the current task."""
            ax.activity.update(agent_id, message, percentage=percentage)
            return f"Status reported: {message} ({percentage}%)"

        return report_activity
```

### Custom Tools for Your Framework

Add framework-specific tools:

```python
class MyRunner(BaseAgentRunner):

    def get_tools(self):
        """Get tools formatted for your framework."""
        return {
            "report_status": {
                "name": "report_status",
                "description": "Report task progress",
                "parameters": {
                    "message": {"type": "string"},
                    "percentage": {"type": "integer", "minimum": 0, "maximum": 100}
                },
                "handler": self._report_status
            }
        }

    async def _report_status(self, message: str, percentage: int):
        ax.activity.update(self.agent_id, message, percentage=percentage)
        return "Status updated"
```

## Error Handling Patterns

### Retry Logic

```python
class RetryRunner(BaseAgentRunner):

    async def run(self, agent, input: str, max_turns: int = 10, retries: int = 3):
        ax.activity.update(self.agent_id, ax.CONF.activity_message_started)

        last_error = None
        for attempt in range(retries):
            try:
                result = await self._execute(agent, input, max_turns)
                ax.activity.complete(self.agent_id, ax.CONF.activity_message_complete)
                return result
            except TransientError as e:
                last_error = e
                ax.activity.update(
                    self.agent_id,
                    f"Retry {attempt + 1}/{retries}: {e}"
                )
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

        ax.activity.error(
            self.agent_id,
            f"Failed after {retries} retries: {last_error}"
        )
        raise last_error
```

### Graceful Degradation

```python
class FallbackRunner(BaseAgentRunner):

    def __init__(self, agent_id: UUID, fallback_model: str = "gpt-3.5-turbo"):
        super().__init__(agent_id)
        self.fallback_model = fallback_model

    async def run(self, agent, input: str, **kwargs):
        ax.activity.update(self.agent_id, ax.CONF.activity_message_started)

        try:
            result = await self._run_primary(agent, input, **kwargs)
            ax.activity.complete(self.agent_id, ax.CONF.activity_message_complete)
            return result
        except PrimaryModelError as e:
            ax.activity.update(
                self.agent_id,
                f"Primary model failed, using fallback: {e}"
            )
            result = await self._run_fallback(input, **kwargs)
            ax.activity.complete(
                self.agent_id,
                "Completed with fallback model"
            )
            return result
```

## Testing Custom Runners

```python
import pytest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

@pytest.mark.asyncio
async def test_custom_runner_lifecycle():
    agent_id = uuid4()
    runner = MyRunner(agent_id=agent_id)

    with patch("agentexec.activity.update") as mock_update:
        with patch("agentexec.activity.complete") as mock_complete:
            mock_agent = AsyncMock()
            mock_agent.run.return_value = {"output": "test"}

            result = await runner.run(mock_agent, input="test")

            # Verify lifecycle calls
            mock_update.assert_called()  # RUNNING status
            mock_complete.assert_called()  # COMPLETE status

@pytest.mark.asyncio
async def test_custom_runner_error_handling():
    agent_id = uuid4()
    runner = MyRunner(agent_id=agent_id)

    with patch("agentexec.activity.error") as mock_error:
        mock_agent = AsyncMock()
        mock_agent.run.side_effect = RuntimeError("Test error")

        with pytest.raises(RuntimeError):
            await runner.run(mock_agent, input="test")

        mock_error.assert_called()  # ERROR status
```

## Best Practices

### 1. Always Update Activity Status

```python
async def run(self, ...):
    # START: Update to RUNNING
    ax.activity.update(self.agent_id, ax.CONF.activity_message_started)

    try:
        result = await self._execute(...)
        # SUCCESS: Mark COMPLETE
        ax.activity.complete(self.agent_id, ax.CONF.activity_message_complete)
        return result
    except Exception as e:
        # FAILURE: Mark ERROR
        ax.activity.error(self.agent_id, str(e))
        raise
```

### 2. Report Progress for Long Operations

```python
async def _execute(self, agent, input, max_turns):
    for turn in range(max_turns):
        # Report progress each turn
        progress = int((turn + 1) / max_turns * 100)
        ax.activity.update(
            self.agent_id,
            f"Turn {turn + 1}/{max_turns}",
            percentage=min(progress, 90)  # Leave 10% for completion
        )
        ...
```

### 3. Use Configuration Messages

```python
# Use configurable messages for consistency
ax.activity.update(self.agent_id, ax.CONF.activity_message_started)
ax.activity.complete(self.agent_id, ax.CONF.activity_message_complete)
ax.activity.error(self.agent_id, ax.CONF.activity_message_error.format(error=str(e)))
```

### 4. Support Cancellation

```python
async def run(self, agent, input, **kwargs):
    ax.activity.update(self.agent_id, ax.CONF.activity_message_started)

    try:
        result = await asyncio.wait_for(
            self._execute(agent, input),
            timeout=kwargs.get("timeout", 300)
        )
        ax.activity.complete(self.agent_id, ax.CONF.activity_message_complete)
        return result
    except asyncio.TimeoutError:
        ax.activity.error(self.agent_id, "Task timed out")
        raise
    except asyncio.CancelledError:
        ax.activity.update(self.agent_id, "Task cancelled")
        raise
```

## Next Steps

- [OpenAI Runner](openai-runner.md) - Reference implementation
- [Activity Tracking](../concepts/activity-tracking.md) - Activity system details
- [API Reference](../api-reference/runner.md) - Runner API documentation
