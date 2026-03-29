import pytest
from sqlalchemy import create_engine

from agentexec.core.db import Base, configure_engine, get_session


@pytest.fixture
def test_engine():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    configure_engine(engine)
    yield engine
    engine.dispose()


def test_configure_engine(test_engine):
    """configure_engine makes get_session available."""
    session = get_session()
    assert session is not None
    session.close()


def test_get_session_context_manager(test_engine):
    """get_session works as a context manager."""
    with get_session() as session:
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
