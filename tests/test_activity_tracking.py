import uuid

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session, sessionmaker

from agentexec import activity
from agentexec.activity.models import Activity, ActivityLog, Base, Status
from agentexec.activity import normalize_agent_id


@pytest.fixture(autouse=True)
def direct_activity_writes(monkeypatch):
    """Bypass multiprocessing queue — write directly to Postgres
    when the producer sends activity update events."""
    from agentexec.activity import producer
    from agentexec.worker.pool import ActivityUpdated

    def direct_send(message):
        from agentexec.activity.models import Activity
        from agentexec.activity.status import Status
        from agentexec.core.db import get_global_session

        match message:
            case ActivityUpdated(agent_id=agent_id, message=msg, status=status, percentage=pct):
                db = get_global_session()
                Activity.append_log(session=db, agent_id=agent_id, message=msg, status=Status(status), percentage=pct)

    monkeypatch.setattr(producer, "_send", direct_send)


@pytest.fixture
def db_session():
    """Set up an in-memory SQLite database for testing."""
    from agentexec.core.db import set_global_session, remove_global_session

    engine = create_engine("sqlite:///:memory:", echo=False)

    # Create tables
    Base.metadata.create_all(bind=engine)

    # Set up the global session so backend functions can find it
    set_global_session(engine)

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        remove_global_session()
        engine.dispose()


async def test_create_activity(db_session: Session):
    """Test creating a new activity record."""
    agent_id = await activity.create(
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


async def test_update_activity(db_session: Session):
    """Test updating an activity with a new log message."""
    # First create an activity
    agent_id = await activity.create(
        task_name="test_task",
        message="Initial message",
        session=db_session,
    )

    # Update the activity
    result = await activity.update(
        agent_id=agent_id,
        message="Processing...",
        percentage=50,
        session=db_session,
    )

    assert result is True

    # Verify the update
    activity_record = Activity.get_by_agent_id(db_session, agent_id)
    assert activity_record is not None
    assert len(activity_record.logs) == 2
    assert activity_record.logs[1].message == "Processing..."
    assert activity_record.logs[1].status == Status.RUNNING
    assert activity_record.logs[1].percentage == 50


async def test_update_activity_with_custom_status(db_session: Session):
    """Test updating an activity with a custom status."""
    agent_id = await activity.create(
        task_name="test_task",
        message="Initial",
        session=db_session,
    )

    await activity.update(
        agent_id=agent_id,
        message="Custom status update",
        status=Status.RUNNING,
        percentage=25,
        session=db_session,
    )

    activity_record = Activity.get_by_agent_id(db_session, agent_id)
    assert activity_record is not None
    latest_log = activity_record.logs[-1]
    assert latest_log.status == Status.RUNNING


async def test_complete_activity(db_session: Session):
    """Test marking an activity as complete."""
    agent_id = await activity.create(
        task_name="test_task",
        message="Started",
        session=db_session,
    )

    result = await activity.complete(
        agent_id=agent_id,
        message="Successfully completed",
        session=db_session,
    )

    assert result is True

    activity_record = Activity.get_by_agent_id(db_session, agent_id)
    assert activity_record is not None
    latest_log = activity_record.logs[-1]
    assert latest_log.message == "Successfully completed"
    assert latest_log.status == Status.COMPLETE
    assert latest_log.percentage == 100


async def test_complete_activity_custom_percentage(db_session: Session):
    """Test marking an activity complete with custom percentage."""
    agent_id = await activity.create(
        task_name="test_task",
        message="Started",
        session=db_session,
    )

    await activity.complete(
        agent_id=agent_id,
        message="Done",
        percentage=95,
        session=db_session,
    )

    activity_record = Activity.get_by_agent_id(db_session, agent_id)
    assert activity_record is not None
    latest_log = activity_record.logs[-1]
    assert latest_log.percentage == 95


async def test_error_activity(db_session: Session):
    """Test marking an activity as errored."""
    agent_id = await activity.create(
        task_name="test_task",
        message="Started",
        session=db_session,
    )

    result = await activity.error(
        agent_id=agent_id,
        message="Task failed: connection timeout",
        session=db_session,
    )

    assert result is True

    activity_record = Activity.get_by_agent_id(db_session, agent_id)
    assert activity_record is not None
    latest_log = activity_record.logs[-1]
    assert latest_log.message == "Task failed: connection timeout"
    assert latest_log.status == Status.ERROR
    assert latest_log.percentage == 100


async def test_cancel_pending_activities(db_session: Session):
    """Test canceling all pending activities."""
    # Create some activities in different states
    queued_id = await activity.create(
        task_name="queued_task",
        message="Waiting",
        session=db_session,
    )

    running_id = await activity.create(
        task_name="running_task",
        message="Started",
        session=db_session,
    )
    await activity.update(
        agent_id=running_id,
        message="Running...",
        status=Status.RUNNING,
        session=db_session,
    )

    complete_id = await activity.create(
        task_name="complete_task",
        message="Started",
        session=db_session,
    )
    await activity.complete(agent_id=complete_id, session=db_session)

    # Cancel pending activities
    canceled_count = await activity.cancel_pending(session=db_session)

    # Should have canceled the queued and running activities
    assert canceled_count == 2

    # Verify the states
    queued_record = Activity.get_by_agent_id(db_session, queued_id)
    running_record = Activity.get_by_agent_id(db_session, running_id)
    complete_record = Activity.get_by_agent_id(db_session, complete_id)

    assert queued_record is not None
    assert running_record is not None
    assert complete_record is not None
    assert queued_record.logs[-1].status == Status.CANCELED
    assert running_record.logs[-1].status == Status.CANCELED
    assert complete_record.logs[-1].status == Status.COMPLETE  # Not changed


async def test_list_activities(db_session: Session):
    """Test listing activities with pagination."""
    # Create several activities
    for i in range(5):
        await activity.create(
            task_name=f"task_{i}",
            message=f"Message {i}",
            session=db_session,
        )

    # List activities
    result = await activity.list(db_session, page=1, page_size=3)

    assert len(result.items) == 3
    assert result.total == 5
    assert result.page == 1
    assert result.page_size == 3


async def test_list_activities_second_page(db_session: Session):
    """Test listing activities on second page."""
    for i in range(5):
        await activity.create(
            task_name=f"task_{i}",
            message=f"Message {i}",
            session=db_session,
        )

    result = await activity.list(db_session, page=2, page_size=3)

    assert len(result.items) == 2  # Remaining items
    assert result.total == 5
    assert result.page == 2


async def test_detail_activity(db_session: Session):
    """Test getting activity detail with all logs."""
    agent_id = await activity.create(
        task_name="detailed_task",
        message="Initial",
        session=db_session,
    )
    await activity.update(
        agent_id=agent_id,
        message="Processing",
        percentage=50,
        session=db_session,
    )
    await activity.complete(agent_id=agent_id, session=db_session)

    result = await activity.detail(db_session, agent_id)

    assert result is not None
    assert result.agent_id == agent_id
    assert result.agent_type == "detailed_task"
    assert len(result.logs) == 3
    assert result.logs[0].message == "Initial"
    assert result.logs[1].message == "Processing"
    assert result.logs[2].status == Status.COMPLETE


async def test_detail_activity_not_found(db_session: Session):
    """Test getting detail for non-existent activity returns None."""
    fake_id = uuid.uuid4()
    result = await activity.detail(db_session, fake_id)

    assert result is None


async def test_detail_activity_with_string_id(db_session: Session):
    """Test getting activity detail with string agent_id."""
    agent_id = await activity.create(
        task_name="string_id_task",
        message="Test",
        session=db_session,
    )

    # Use string ID
    result = await activity.detail(db_session, str(agent_id))

    assert result is not None
    assert result.agent_id == agent_id


async def test_create_activity_with_custom_agent_id(db_session: Session):
    """Test creating activity with a custom agent_id."""
    custom_id = uuid.uuid4()
    agent_id = await activity.create(
        task_name="custom_id_task",
        message="Test",
        agent_id=custom_id,
        session=db_session,
    )

    assert agent_id == custom_id

    activity_record = Activity.get_by_agent_id(db_session, custom_id)
    assert activity_record is not None


async def test_create_activity_with_string_agent_id(db_session: Session):
    """Test creating activity with a string agent_id."""
    custom_id = uuid.uuid4()
    agent_id = await activity.create(
        task_name="string_agent_id_task",
        message="Test",
        agent_id=str(custom_id),
        session=db_session,
    )

    assert agent_id == custom_id


async def test_create_activity_with_metadata(db_session: Session):
    """Test creating activity with metadata."""
    agent_id = await activity.create(
        task_name="metadata_task",
        message="Test with metadata",
        session=db_session,
        metadata={"organization_id": "org-123", "user_id": "user-456"},
    )

    activity_record = Activity.get_by_agent_id(db_session, agent_id)
    assert activity_record is not None
    assert activity_record.metadata_ == {"organization_id": "org-123", "user_id": "user-456"}


async def test_create_activity_without_metadata(db_session: Session):
    """Test that metadata is None by default."""
    agent_id = await activity.create(
        task_name="no_metadata_task",
        message="Test without metadata",
        session=db_session,
    )

    activity_record = Activity.get_by_agent_id(db_session, agent_id)
    assert activity_record is not None
    assert activity_record.metadata_ is None


async def test_list_activities_with_metadata_filter(db_session: Session):
    """Test filtering activities by metadata."""
    # Create activities for different organizations
    await activity.create(
        task_name="task_org_a",
        message="Org A task",
        session=db_session,
        metadata={"organization_id": "org-A"},
    )
    await activity.create(
        task_name="task_org_a_2",
        message="Org A task 2",
        session=db_session,
        metadata={"organization_id": "org-A"},
    )
    await activity.create(
        task_name="task_org_b",
        message="Org B task",
        session=db_session,
        metadata={"organization_id": "org-B"},
    )

    # Filter by org-A
    result = await activity.list(
        db_session,
        metadata_filter={"organization_id": "org-A"},
    )
    assert result.total == 2
    assert len(result.items) == 2
    for item in result.items:
        assert item.metadata is not None
        assert item.metadata["organization_id"] == "org-A"

    # Filter by org-B
    result = await activity.list(
        db_session,
        metadata_filter={"organization_id": "org-B"},
    )
    assert result.total == 1
    assert result.items[0].metadata is not None
    assert result.items[0].metadata["organization_id"] == "org-B"

    # Filter by non-existent org
    result = await activity.list(
        db_session,
        metadata_filter={"organization_id": "org-C"},
    )
    assert result.total == 0


async def test_list_activities_with_multiple_metadata_filters(db_session: Session):
    """Test filtering activities by multiple metadata fields."""
    await activity.create(
        task_name="task_1",
        message="User 1 in Org A",
        session=db_session,
        metadata={"organization_id": "org-A", "user_id": "user-1"},
    )
    await activity.create(
        task_name="task_2",
        message="User 2 in Org A",
        session=db_session,
        metadata={"organization_id": "org-A", "user_id": "user-2"},
    )

    # Filter by both org and user
    result = await activity.list(
        db_session,
        metadata_filter={"organization_id": "org-A", "user_id": "user-1"},
    )
    assert result.total == 1


async def test_detail_activity_with_metadata(db_session: Session):
    """Test getting activity detail includes metadata."""
    agent_id = await activity.create(
        task_name="detailed_metadata_task",
        message="Test",
        session=db_session,
        metadata={"organization_id": "org-123"},
    )

    result = await activity.detail(db_session, agent_id)
    assert result is not None
    assert result.metadata == {"organization_id": "org-123"}


async def test_detail_activity_with_metadata_filter_match(db_session: Session):
    """Test detail returns activity when metadata filter matches."""
    agent_id = await activity.create(
        task_name="filter_match_task",
        message="Test",
        session=db_session,
        metadata={"organization_id": "org-A"},
    )

    result = await activity.detail(
        db_session,
        agent_id,
        metadata_filter={"organization_id": "org-A"},
    )
    assert result is not None
    assert result.agent_id == agent_id


async def test_detail_activity_with_metadata_filter_no_match(db_session: Session):
    """Test detail returns None when metadata filter doesn't match."""
    agent_id = await activity.create(
        task_name="filter_no_match_task",
        message="Test",
        session=db_session,
        metadata={"organization_id": "org-A"},
    )

    # Try to access with wrong organization
    result = await activity.detail(
        db_session,
        agent_id,
        metadata_filter={"organization_id": "org-B"},
    )
    assert result is None


async def test_detail_activity_no_metadata_with_filter(db_session: Session):
    """Test detail returns None when activity has no metadata but filter is applied."""
    agent_id = await activity.create(
        task_name="no_metadata_with_filter",
        message="Test",
        session=db_session,
    )

    result = await activity.detail(
        db_session,
        agent_id,
        metadata_filter={"organization_id": "org-A"},
    )
    assert result is None


async def test_list_metadata_accessible_as_attribute(db_session: Session):
    """Test that metadata is accessible as an attribute on schema objects."""
    await activity.create(
        task_name="list_metadata_task",
        message="Test",
        session=db_session,
        metadata={"key1": "value1", "key2": "value2"},
    )

    result = await activity.list(db_session)
    assert result.total == 1
    # Metadata is accessible as attribute for programmatic use
    assert result.items[0].metadata == {"key1": "value1", "key2": "value2"}


async def test_metadata_excluded_from_serialization(db_session: Session):
    """Test that metadata is excluded from JSON/dict serialization by default.

    This prevents accidental leakage of tenant info through API responses.
    Users who want metadata in responses should explicitly include it.
    """
    agent_id = await activity.create(
        task_name="serialization_test",
        message="Test",
        session=db_session,
        metadata={"organization_id": "org-123", "secret": "sensitive"},
    )

    # List view - metadata excluded from serialization
    result = await activity.list(db_session)
    item_dict = result.items[0].model_dump()
    assert "metadata" not in item_dict

    # Detail view - metadata excluded from serialization
    detail = await activity.detail(db_session, agent_id)
    assert detail is not None
    detail_dict = detail.model_dump()
    assert "metadata" not in detail_dict

    # But still accessible as attribute for internal use
    assert result.items[0].metadata == {"organization_id": "org-123", "secret": "sensitive"}
    assert detail.metadata == {"organization_id": "org-123", "secret": "sensitive"}
