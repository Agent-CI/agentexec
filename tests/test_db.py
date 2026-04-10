import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from agentexec.core.db import Base, configure_engine, get_session


@pytest.fixture
async def test_engine():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    configure_engine(engine)
    yield engine
    await engine.dispose()


async def test_configure_engine(test_engine):
    """configure_engine makes get_session available."""
    session = get_session()
    assert session is not None
    await session.close()


async def test_get_session_context_manager(test_engine):
    """get_session works as a context manager."""
    async with get_session() as session:
        assert session is not None


def test_get_session_without_configure_raises():
    """get_session raises if configure_engine hasn't been called."""
    import agentexec.core.db as db_module
    old_factory = db_module._session_factory
    db_module._session_factory = None
    try:
        with pytest.raises(RuntimeError, match="Database engine not configured"):
            get_session()
    finally:
        db_module._session_factory = old_factory
