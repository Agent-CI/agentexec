from sqlalchemy import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


__all__ = [
    "Base",
    "configure_engine",
    "get_session",
]


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models.

    Example::

        # In alembic/env.py
        import agentexec as ax
        target_metadata = ax.Base.metadata
    """
    pass


_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def configure_engine(engine: Engine) -> None:
    """Set the shared engine for the application.

    Called once during Pool initialization. Workers inherit the engine
    via multiprocessing.
    """
    global _engine, _session_factory
    _engine = engine
    _session_factory = sessionmaker(bind=engine)


def get_session() -> Session:
    """Create a new session from the shared engine.

    Use as a context manager::

        with get_session() as db:
            db.query(...)

    Raises:
        RuntimeError: If ``configure_engine()`` hasn't been called.
    """
    if _session_factory is None:
        raise RuntimeError("Database engine not configured. Call configure_engine() first.")
    return _session_factory()
