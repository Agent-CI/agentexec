"""Task definitions for the basic example.

This module is imported by both the run script and the spawned worker
processes. Keep it free of side effects — no asyncio.run(), no table
creation, no enqueue calls.
"""

import asyncio
import logging
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine

import agentexec as ax

logger = logging.getLogger(__name__)

engine = create_async_engine("sqlite+aiosqlite:///agents.db")
pool = ax.Pool(engine=engine)


class ResearchContext(BaseModel):
    company: str


class GreetContext(BaseModel):
    name: str


class ErrorContext(BaseModel):
    pass


@pool.task("research")
async def research(agent_id: UUID, context: ResearchContext):
    logger.info(f"Researching {context.company}...")
    await asyncio.sleep(1)
    logger.info(f"Done researching {context.company}")


@pool.task("greet")
async def greet(agent_id: UUID, context: GreetContext):
    logger.info(f"Hello, {context.name}!")


@pool.task("error")
async def handle_error(agent_id: UUID, context: ErrorContext) -> None:
    logger.info("This will throw an error")
    raise Exception("Intentional error")
