from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase


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


_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def configure_engine(engine: AsyncEngine) -> None:
    """Set the shared async engine for the application.

    Called once during Pool initialization. Workers inherit the engine
    via multiprocessing.
    """
    global _engine, _session_factory
    _engine = engine
    _session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)


def get_session() -> AsyncSession:
    """Create a new async session from the shared engine.

    Use as an async context manager::

        async with get_session() as db:
            result = await db.execute(...)

    Raises:
        RuntimeError: If ``configure_engine()`` hasn't been called.
    """
    if _session_factory is None:
        raise RuntimeError("Database engine not configured. Call configure_engine() first.")
    return _session_factory()


async def dispose_engine() -> None:
    """Dispose the async engine, closing all connections and threads.

    Called during pool shutdown to ensure the aiosqlite/asyncpg
    background thread exits cleanly.
    """
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
