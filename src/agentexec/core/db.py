from __future__ import annotations

from typing import TypeAlias

from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Session, scoped_session, sessionmaker


__all__ = [
    "Base",
    "DatabaseEngine",
    "DatabaseSession",
    "get_global_async_session",
    "get_global_session",
    "is_async_session",
    "remove_global_async_session",
    "remove_global_session",
    "set_global_async_session",
    "set_global_session",
]


DatabaseEngine: TypeAlias = Engine | AsyncEngine
"""Union type for sync and async SQLAlchemy engines."""

DatabaseSession: TypeAlias = Session | AsyncSession
"""Union type for sync and async SQLAlchemy sessions."""


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models in agent-runner.

    Example:
        # In alembic/env.py
        import agentexec as ax
        target_metadata = ax.Base.metadata
    """

    pass


# ---------------------------------------------------------------------------
# Sync session management
# ---------------------------------------------------------------------------

# We need one session per worker process with a shared engine across the application.
# SQLAlchemy's scoped_session provides process-local session management out of the box.
_session_factory: scoped_session[Session] = scoped_session(sessionmaker())


def set_global_session(engine: Engine) -> None:
    """Configure the global session factory with an engine.

    Called by workers on startup to bind the session to their database.

    Args:
        engine: SQLAlchemy engine to bind sessions to.
    """
    _session_factory.configure(bind=engine)


def get_global_session() -> Session:
    """Get the worker's process-local session.

    This is distinct from request-scoped sessions used in API handlers.
    Use this for background task execution within workers.

    Returns:
        A session bound to the configured engine.

    Raises:
        RuntimeError: If set_global_session() hasn't been called.
    """
    return _session_factory()


def remove_global_session() -> None:
    """Close and remove the worker's process-local session.

    Called during worker cleanup to close the session and return
    connections to the pool.
    """
    _session_factory.remove()


# ---------------------------------------------------------------------------
# Async session management
# ---------------------------------------------------------------------------

_async_session_factory: async_sessionmaker[AsyncSession] | None = None


def set_global_async_session(engine: AsyncEngine) -> None:
    """Configure the global async session factory with an async engine.

    Args:
        engine: SQLAlchemy async engine to bind sessions to.
    """
    global _async_session_factory
    _async_session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)


def get_global_async_session() -> AsyncSession:
    """Create a new async session from the global factory.

    Unlike the sync ``get_global_session()`` which returns a scoped
    (process-local) session, each call here returns a **new** session.
    Callers are responsible for closing it when done.

    Returns:
        A new ``AsyncSession`` bound to the configured async engine.

    Raises:
        RuntimeError: If ``set_global_async_session()`` hasn't been called.
    """
    if _async_session_factory is None:
        raise RuntimeError(
            "Async session not configured. Call set_global_async_session() first."
        )
    return _async_session_factory()


def remove_global_async_session() -> None:
    """Remove the global async session factory.

    Resets the factory to ``None``. Existing sessions created from it
    should be closed individually.
    """
    global _async_session_factory
    _async_session_factory = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def is_async_session(session: DatabaseSession) -> bool:
    """Return ``True`` if *session* is an ``AsyncSession``."""
    return isinstance(session, AsyncSession)
