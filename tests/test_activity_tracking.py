"""Tests for activity tracking functionality."""

import uuid

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session, sessionmaker

from agentexec import activity
from agentexec.activity.models import Activity, ActivityLog, Base, Status
from agentexec.activity.tracker import normalize_agent_id


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
    assert activity_record.logs[0].percentage == 0


def test_normalize_agent_id():
    """Test the normalize_agent_id helper function."""
    # Test with UUID object
    uuid_obj = uuid.uuid4()
    result = normalize_agent_id(uuid_obj)
    assert result == uuid_obj
    assert isinstance(result, uuid.UUID)

    # Test with string UUID
    uuid_str = str(uuid.uuid4())
    result = normalize_agent_id(uuid_str)
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


def test_update_activity(db_session: Session):
    """Test updating an activity with a new log message."""
    # First create an activity
    agent_id = activity.create(
        task_name="test_task",
        message="Initial message",
        session=db_session,
    )

    # Update the activity
    result = activity.update(
        agent_id=agent_id,
        message="Processing...",
        percentage=50,
        session=db_session,
    )

    assert result is True

    # Verify the update
    activity_record = Activity.get_by_agent_id(db_session, agent_id)
    assert len(activity_record.logs) == 2
    assert activity_record.logs[1].message == "Processing..."
    assert activity_record.logs[1].status == Status.RUNNING
    assert activity_record.logs[1].percentage == 50


def test_update_activity_with_custom_status(db_session: Session):
    """Test updating an activity with a custom status."""
    agent_id = activity.create(
        task_name="test_task",
        message="Initial",
        session=db_session,
    )

    activity.update(
        agent_id=agent_id,
        message="Custom status update",
        status=Status.RUNNING,
        percentage=25,
        session=db_session,
    )

    activity_record = Activity.get_by_agent_id(db_session, agent_id)
    latest_log = activity_record.logs[-1]
    assert latest_log.status == Status.RUNNING


def test_complete_activity(db_session: Session):
    """Test marking an activity as complete."""
    agent_id = activity.create(
        task_name="test_task",
        message="Started",
        session=db_session,
    )

    result = activity.complete(
        agent_id=agent_id,
        message="Successfully completed",
        session=db_session,
    )

    assert result is True

    activity_record = Activity.get_by_agent_id(db_session, agent_id)
    latest_log = activity_record.logs[-1]
    assert latest_log.message == "Successfully completed"
    assert latest_log.status == Status.COMPLETE
    assert latest_log.percentage == 100


def test_complete_activity_custom_percentage(db_session: Session):
    """Test marking an activity complete with custom percentage."""
    agent_id = activity.create(
        task_name="test_task",
        message="Started",
        session=db_session,
    )

    activity.complete(
        agent_id=agent_id,
        message="Done",
        percentage=95,
        session=db_session,
    )

    activity_record = Activity.get_by_agent_id(db_session, agent_id)
    latest_log = activity_record.logs[-1]
    assert latest_log.percentage == 95


def test_error_activity(db_session: Session):
    """Test marking an activity as errored."""
    agent_id = activity.create(
        task_name="test_task",
        message="Started",
        session=db_session,
    )

    result = activity.error(
        agent_id=agent_id,
        message="Task failed: connection timeout",
        session=db_session,
    )

    assert result is True

    activity_record = Activity.get_by_agent_id(db_session, agent_id)
    latest_log = activity_record.logs[-1]
    assert latest_log.message == "Task failed: connection timeout"
    assert latest_log.status == Status.ERROR
    assert latest_log.percentage == 100


def test_cancel_pending_activities(db_session: Session):
    """Test canceling all pending activities."""
    # Create some activities in different states
    queued_id = activity.create(
        task_name="queued_task",
        message="Waiting",
        session=db_session,
    )

    running_id = activity.create(
        task_name="running_task",
        message="Started",
        session=db_session,
    )
    activity.update(
        agent_id=running_id,
        message="Running...",
        status=Status.RUNNING,
        session=db_session,
    )

    complete_id = activity.create(
        task_name="complete_task",
        message="Started",
        session=db_session,
    )
    activity.complete(agent_id=complete_id, session=db_session)

    # Cancel pending activities
    canceled_count = activity.cancel_pending(session=db_session)

    # Should have canceled the queued and running activities
    assert canceled_count == 2

    # Verify the states
    queued_record = Activity.get_by_agent_id(db_session, queued_id)
    running_record = Activity.get_by_agent_id(db_session, running_id)
    complete_record = Activity.get_by_agent_id(db_session, complete_id)

    assert queued_record.logs[-1].status == Status.CANCELED
    assert running_record.logs[-1].status == Status.CANCELED
    assert complete_record.logs[-1].status == Status.COMPLETE  # Not changed


def test_list_activities(db_session: Session):
    """Test listing activities with pagination."""
    # Create several activities
    for i in range(5):
        activity.create(
            task_name=f"task_{i}",
            message=f"Message {i}",
            session=db_session,
        )

    # List activities
    result = activity.list(db_session, page=1, page_size=3)

    assert len(result.items) == 3
    assert result.total == 5
    assert result.page == 1
    assert result.page_size == 3


def test_list_activities_second_page(db_session: Session):
    """Test listing activities on second page."""
    for i in range(5):
        activity.create(
            task_name=f"task_{i}",
            message=f"Message {i}",
            session=db_session,
        )

    result = activity.list(db_session, page=2, page_size=3)

    assert len(result.items) == 2  # Remaining items
    assert result.total == 5
    assert result.page == 2


def test_detail_activity(db_session: Session):
    """Test getting activity detail with all logs."""
    agent_id = activity.create(
        task_name="detailed_task",
        message="Initial",
        session=db_session,
    )
    activity.update(
        agent_id=agent_id,
        message="Processing",
        percentage=50,
        session=db_session,
    )
    activity.complete(agent_id=agent_id, session=db_session)

    result = activity.detail(db_session, agent_id)

    assert result is not None
    assert result.agent_id == agent_id
    assert result.agent_type == "detailed_task"
    assert len(result.logs) == 3
    assert result.logs[0].message == "Initial"
    assert result.logs[1].message == "Processing"
    assert result.logs[2].status == Status.COMPLETE


def test_detail_activity_not_found(db_session: Session):
    """Test getting detail for non-existent activity returns None."""
    fake_id = uuid.uuid4()
    result = activity.detail(db_session, fake_id)

    assert result is None


def test_detail_activity_with_string_id(db_session: Session):
    """Test getting activity detail with string agent_id."""
    agent_id = activity.create(
        task_name="string_id_task",
        message="Test",
        session=db_session,
    )

    # Use string ID
    result = activity.detail(db_session, str(agent_id))

    assert result is not None
    assert result.agent_id == agent_id


def test_create_activity_with_custom_agent_id(db_session: Session):
    """Test creating activity with a custom agent_id."""
    custom_id = uuid.uuid4()
    agent_id = activity.create(
        task_name="custom_id_task",
        message="Test",
        agent_id=custom_id,
        session=db_session,
    )

    assert agent_id == custom_id

    activity_record = Activity.get_by_agent_id(db_session, custom_id)
    assert activity_record is not None


def test_create_activity_with_string_agent_id(db_session: Session):
    """Test creating activity with a string agent_id."""
    custom_id = uuid.uuid4()
    agent_id = activity.create(
        task_name="string_agent_id_task",
        message="Test",
        agent_id=str(custom_id),
        session=db_session,
    )

    assert agent_id == custom_id
