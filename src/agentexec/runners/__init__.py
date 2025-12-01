"""Agent runners with activity tracking and lifecycle management."""

from agentexec.runners.base import BaseAgentRunner

__all__ = ["BaseAgentRunner"]

# OpenAI runner is only available if agents package is installed
try:
    from agentexec.runners.openai import OpenAIRunner

    __all__.append("OpenAIRunner")
except ImportError:
    pass

# Google ADK runner is only available if google-adk package is installed
try:
    from agentexec.runners.google_adk import GoogleADKRunner

    __all__.append("GoogleADKRunner")
except ImportError:
    pass
