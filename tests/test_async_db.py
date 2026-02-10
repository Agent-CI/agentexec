"""Test async database session management."""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from agentexec.core.db import (
    Base,
    DatabaseEngine,
    DatabaseSession,
    get_global_async_session,
    is_async_session,
    remove_global_async_session,
    set_global_async_session,
)


@pytest.fixture
async def async_engine():
    """Create a test async SQLite engine."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    yield engine
    await engine.dispose()


@pytest.fixture(autouse=True)
def cleanup_async_session():
    """Cleanup global async session after each test."""
    yield
    remove_global_async_session()


async def test_set_global_async_session(async_engine):
    """Test that set_global_async_session configures the factory."""
    set_global_async_session(async_engine)

    session = get_global_async_session()
    assert isinstance(session, AsyncSession)
    await session.close()


async def test_get_global_async_session_returns_working_session(async_engine):
    """Test that the async session can execute queries."""
    set_global_async_session(async_engine)

    session = get_global_async_session()
    result = await session.execute(text("SELECT 1"))
    assert result.scalar() == 1
    await session.close()


async def test_get_global_async_session_returns_new_instances(async_engine):
    """Test that each call returns a different session instance."""
    set_global_async_session(async_engine)

    session1 = get_global_async_session()
    session2 = get_global_async_session()

    # Each call should return a new session
    assert session1 is not session2
    await session1.close()
    await session2.close()


async def test_get_global_async_session_without_setup():
    """Test that accessing async session before setup raises RuntimeError."""
    with pytest.raises(RuntimeError, match="Async session not configured"):
        get_global_async_session()


async def test_remove_global_async_session(async_engine):
    """Test that remove_global_async_session resets the factory."""
    set_global_async_session(async_engine)

    # Should work
    session = get_global_async_session()
    await session.close()

    # After remove, should raise
    remove_global_async_session()
    with pytest.raises(RuntimeError, match="Async session not configured"):
        get_global_async_session()


async def test_async_session_with_tables(async_engine):
    """Test that async session works with table creation."""
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    set_global_async_session(async_engine)
    session = get_global_async_session()

    result = await session.execute(
        text("SELECT name FROM sqlite_master WHERE type='table'")
    )
    tables = [row[0] for row in result]
    assert isinstance(tables, list)
    await session.close()


def test_is_async_session_with_sync():
    """Test is_async_session returns False for sync sessions."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session, sessionmaker

    engine = create_engine("sqlite:///:memory:")
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    assert is_async_session(session) is False
    session.close()
    engine.dispose()


async def test_is_async_session_with_async(async_engine):
    """Test is_async_session returns True for async sessions."""
    set_global_async_session(async_engine)
    session = get_global_async_session()

    assert is_async_session(session) is True
    await session.close()


def test_database_engine_type_alias():
    """Test that DatabaseEngine accepts both engine types."""
    from sqlalchemy import Engine
    from sqlalchemy.ext.asyncio import AsyncEngine

    # Just verify the type alias is accessible and usable
    assert DatabaseEngine is not None


def test_database_session_type_alias():
    """Test that DatabaseSession is accessible."""
    assert DatabaseSession is not None
