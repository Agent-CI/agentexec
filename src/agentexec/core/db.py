from sqlalchemy import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


__all__ = [
    "Base",
    "configure_engine",
    "get_session",
]


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""
    pass


_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def configure_engine(engine: Engine) -> None:
    """Set the shared engine for the application."""
    global _engine, _session_factory
    _engine = engine
    _session_factory = sessionmaker(bind=engine)


def get_session() -> Session:
    """Create a new session from the shared engine.

    Use with a context manager:
        with get_session() as db:
            db.query(...)
    """
    if _session_factory is None:
        raise RuntimeError("Database engine not configured. Call configure_engine() first.")
    return _session_factory()
