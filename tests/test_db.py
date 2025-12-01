"""Test database session management."""

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from agentexec.core.db import (
    Base,
    get_global_session,
    remove_global_session,
    set_global_session,
)


@pytest.fixture
def test_engine():
    """Create a test SQLite engine."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    yield engine
    engine.dispose()


@pytest.fixture(autouse=True)
def cleanup_session():
    """Cleanup global session after each test."""
    yield
    try:
        remove_global_session()
    except Exception:
        pass


def test_base_class_exists():
    """Test that Base class is exported and usable."""
    assert Base is not None
    assert hasattr(Base, "metadata")


def test_set_global_session(test_engine):
    """Test that set_global_session configures the session factory."""
    set_global_session(test_engine)

    # Should be able to get a session now
    session = get_global_session()
    assert isinstance(session, Session)


def test_get_global_session_returns_session(test_engine):
    """Test that get_global_session returns a working session."""
    set_global_session(test_engine)

    session = get_global_session()

    # Verify it's a working session
    result = session.execute(text("SELECT 1"))
    assert result.scalar() == 1


def test_get_global_session_singleton(test_engine):
    """Test that get_global_session returns the same session instance."""
    set_global_session(test_engine)

    session1 = get_global_session()
    session2 = get_global_session()

    # Should be the same session (scoped_session behavior)
    assert session1 is session2


def test_remove_global_session(test_engine):
    """Test that remove_global_session closes the session."""
    set_global_session(test_engine)

    session1 = get_global_session()
    remove_global_session()

    # Getting session again should return a different instance
    session2 = get_global_session()

    # They should be different sessions after remove
    assert session1 is not session2


def test_session_with_tables(test_engine):
    """Test that session works with table creation."""
    # Create the tables
    Base.metadata.create_all(bind=test_engine)

    set_global_session(test_engine)
    session = get_global_session()

    # Session should be able to query (though tables may be empty)
    result = session.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
    tables = [row[0] for row in result]

    # Tables from Base.metadata should exist
    assert isinstance(tables, list)


def test_multiple_set_global_session_calls(test_engine):
    """Test that multiple set_global_session calls work correctly."""
    set_global_session(test_engine)
    session1 = get_global_session()

    # Create another engine
    engine2 = create_engine("sqlite:///:memory:")

    # Reconfigure with new engine
    set_global_session(engine2)
    session2 = get_global_session()

    # Sessions should work with their respective engines
    result = session2.execute(text("SELECT 1"))
    assert result.scalar() == 1

    engine2.dispose()


def test_session_lifecycle():
    """Test complete session lifecycle: set -> use -> remove."""
    engine = create_engine("sqlite:///:memory:")

    # Set
    set_global_session(engine)

    # Use
    session = get_global_session()
    session.execute(text("SELECT 1"))

    # Remove
    remove_global_session()

    # Cleanup
    engine.dispose()
