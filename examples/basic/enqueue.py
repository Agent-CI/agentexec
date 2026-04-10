"""Enqueue tasks for the worker pool.

Simulates what an API server would do when a user submits work.

Usage:
    uv run python enqueue.py
"""

import asyncio

import agentexec as ax
from worker import ResearchContext, GreetContext, ErrorContext


async def main():
    await ax.enqueue("research", ResearchContext(company="Anthropic"))
    await ax.enqueue("research", ResearchContext(company="OpenAI"))
    await ax.enqueue("greet", GreetContext(name="World"))
    await ax.enqueue("error", ErrorContext())

    print("Enqueued 4 tasks")
    return


if __name__ == "__main__":
    asyncio.run(main())
