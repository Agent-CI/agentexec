from sqlalchemy import Engine
from sqlalchemy.orm import DeclarativeBase, Session, scoped_session, sessionmaker


__all__ = [
    "Base",
    "get_global_session",
    "set_global_session",
    "remove_global_session",
]


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models in agent-runner.

    Example:
        # In alembic/env.py
        import agentexec as ax
        target_metadata = ax.Base.metadata
    """

    pass


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
