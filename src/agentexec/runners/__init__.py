"""Agent runners with activity tracking and lifecycle management."""

from agentexec.runners.base import BaseAgentRunner

__all__ = ["BaseAgentRunner"]

# OpenAI runner is only available if agents package is installed
try:
    from agentexec.runners.openai import OpenAIRunner

    __all__.append("OpenAIRunner")
except ImportError:
    pass

# Pydantic AI runner is only available if pydantic-ai package is installed
try:
    from agentexec.runners.pydantic_ai import PydanticAIRunner

    __all__.append("PydanticAIRunner")
except ImportError:
    pass
