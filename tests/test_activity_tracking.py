"""Tests for activity tracking functionality."""

import uuid

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session, sessionmaker

from agentexec import activity
from agentexec.activity.models import Activity, Base, Status


@pytest.fixture
def db_session():
    """Set up an in-memory SQLite database for testing."""
    # Create engine and session factory (users manage their own)
    engine = create_engine("sqlite:///:memory:", echo=False)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    # Create tables
    Base.metadata.create_all(bind=engine)

    # Provide a session for the test
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()


def test_create_activity(db_session: Session):
    """Test creating a new activity record."""
    agent_id = activity.create(
        task_name="test_task",
        message="Task queued for testing",
        session=db_session,
    )

    assert agent_id is not None
    assert isinstance(agent_id, uuid.UUID)

    # Verify the activity was created in database
    activity_record = Activity.get_by_agent_id(db_session, agent_id)
    assert activity_record is not None
    assert activity_record.agent_type == "test_task"
    assert len(activity_record.logs) == 1
    assert activity_record.logs[0].message == "Task queued for testing"
    assert activity_record.logs[0].status == Status.QUEUED
    assert activity_record.logs[0].completion_percentage == 0


def test_generate_agent_id():
    """Test the generate_agent_id helper function."""
    agent_id = activity.generate_agent_id()
    assert isinstance(agent_id, uuid.UUID)


def test_normalize_agent_id():
    """Test the normalize_agent_id helper function."""
    # Test with UUID object
    uuid_obj = uuid.uuid4()
    result = activity.normalize_agent_id(uuid_obj)
    assert result == uuid_obj
    assert isinstance(result, uuid.UUID)

    # Test with string UUID
    uuid_str = str(uuid.uuid4())
    result = activity.normalize_agent_id(uuid_str)
    assert str(result) == uuid_str
    assert isinstance(result, uuid.UUID)


def test_database_tables_created():
    """Test that database tables are created correctly."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)

    # Verify tables exist
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    assert "agentexec_activity" in table_names
    assert "agentexec_activity_log" in table_names

    engine.dispose()
