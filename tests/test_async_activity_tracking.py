"""Tests for async activity tracking functionality."""

import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from agentexec import activity
from agentexec.activity.models import Activity, ActivityLog, Base, Status


@pytest.fixture
async def async_db_session():
    """Set up an async in-memory SQLite database for testing."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    await engine.dispose()


async def test_async_create_activity(async_db_session: AsyncSession):
    """Test creating a new activity record via async session."""
    agent_id = await activity.create(
        task_name="test_task",
        message="Task queued for testing",
        session=async_db_session,
    )

    assert agent_id is not None
    assert isinstance(agent_id, uuid.UUID)

    # Verify the activity was created in database
    activity_record = await Activity.aget_by_agent_id(async_db_session, agent_id)
    assert activity_record is not None
    assert activity_record.agent_type == "test_task"
    assert len(activity_record.logs) == 1
    assert activity_record.logs[0].message == "Task queued for testing"
    assert activity_record.logs[0].status == Status.QUEUED
    assert activity_record.logs[0].percentage == 0


async def test_async_create_with_custom_agent_id(async_db_session: AsyncSession):
    """Test creating activity with a custom agent_id via async."""
    custom_id = uuid.uuid4()
    agent_id = await activity.create(
        task_name="custom_id_task",
        message="Test",
        agent_id=custom_id,
        session=async_db_session,
    )

    assert agent_id == custom_id

    activity_record = await Activity.aget_by_agent_id(async_db_session, custom_id)
    assert activity_record is not None


async def test_async_create_with_string_agent_id(async_db_session: AsyncSession):
    """Test creating activity with a string agent_id via async."""
    custom_id = uuid.uuid4()
    agent_id = await activity.create(
        task_name="string_id_task",
        message="Test",
        agent_id=str(custom_id),
        session=async_db_session,
    )

    assert agent_id == custom_id


async def test_async_create_with_metadata(async_db_session: AsyncSession):
    """Test creating activity with metadata via async."""
    agent_id = await activity.create(
        task_name="metadata_task",
        message="Test",
        session=async_db_session,
        metadata={"organization_id": "org-123", "user_id": "user-456"},
    )

    activity_record = await Activity.aget_by_agent_id(async_db_session, agent_id)
    assert activity_record is not None
    assert activity_record.metadata_ == {"organization_id": "org-123", "user_id": "user-456"}


async def test_async_update_activity(async_db_session: AsyncSession):
    """Test updating an activity with a new log message via async."""
    agent_id = await activity.create(
        task_name="test_task",
        message="Initial message",
        session=async_db_session,
    )

    result = await activity.update(
        agent_id=agent_id,
        message="Processing...",
        percentage=50,
        session=async_db_session,
    )

    assert result is True

    activity_record = await Activity.aget_by_agent_id(async_db_session, agent_id)
    assert len(activity_record.logs) == 2
    assert activity_record.logs[1].message == "Processing..."
    assert activity_record.logs[1].status == Status.RUNNING
    assert activity_record.logs[1].percentage == 50


async def test_async_update_with_custom_status(async_db_session: AsyncSession):
    """Test updating an activity with a custom status via async."""
    agent_id = await activity.create(
        task_name="test_task",
        message="Initial",
        session=async_db_session,
    )

    await activity.update(
        agent_id=agent_id,
        message="Custom status update",
        status=Status.RUNNING,
        percentage=25,
        session=async_db_session,
    )

    activity_record = await Activity.aget_by_agent_id(async_db_session, agent_id)
    latest_log = activity_record.logs[-1]
    assert latest_log.status == Status.RUNNING


async def test_async_complete_activity(async_db_session: AsyncSession):
    """Test marking an activity as complete via async."""
    agent_id = await activity.create(
        task_name="test_task",
        message="Started",
        session=async_db_session,
    )

    result = await activity.complete(
        agent_id=agent_id,
        message="Successfully completed",
        session=async_db_session,
    )

    assert result is True

    activity_record = await Activity.aget_by_agent_id(async_db_session, agent_id)
    latest_log = activity_record.logs[-1]
    assert latest_log.message == "Successfully completed"
    assert latest_log.status == Status.COMPLETE
    assert latest_log.percentage == 100


async def test_async_error_activity(async_db_session: AsyncSession):
    """Test marking an activity as errored via async."""
    agent_id = await activity.create(
        task_name="test_task",
        message="Started",
        session=async_db_session,
    )

    result = await activity.error(
        agent_id=agent_id,
        message="Task failed: connection timeout",
        session=async_db_session,
    )

    assert result is True

    activity_record = await Activity.aget_by_agent_id(async_db_session, agent_id)
    latest_log = activity_record.logs[-1]
    assert latest_log.message == "Task failed: connection timeout"
    assert latest_log.status == Status.ERROR
    assert latest_log.percentage == 100


async def test_async_cancel_pending(async_db_session: AsyncSession):
    """Test canceling all pending activities via async."""
    # Create activities in different states
    queued_id = await activity.create(
        task_name="queued_task",
        message="Waiting",
        session=async_db_session,
    )

    running_id = await activity.create(
        task_name="running_task",
        message="Started",
        session=async_db_session,
    )
    await activity.update(
        agent_id=running_id,
        message="Running...",
        status=Status.RUNNING,
        session=async_db_session,
    )

    complete_id = await activity.create(
        task_name="complete_task",
        message="Started",
        session=async_db_session,
    )
    await activity.complete(agent_id=complete_id, session=async_db_session)

    # Cancel pending
    canceled_count = await activity.cancel_pending(session=async_db_session)
    assert canceled_count == 2

    # Verify states
    queued_record = await Activity.aget_by_agent_id(async_db_session, queued_id)
    running_record = await Activity.aget_by_agent_id(async_db_session, running_id)
    complete_record = await Activity.aget_by_agent_id(async_db_session, complete_id)

    assert queued_record.logs[-1].status == Status.CANCELED
    assert running_record.logs[-1].status == Status.CANCELED
    assert complete_record.logs[-1].status == Status.COMPLETE


async def test_async_list_activities(async_db_session: AsyncSession):
    """Test listing activities with pagination via async."""
    for i in range(5):
        await activity.create(
            task_name=f"task_{i}",
            message=f"Message {i}",
            session=async_db_session,
        )

    result = await activity.list(async_db_session, page=1, page_size=3)

    assert len(result.items) == 3
    assert result.total == 5
    assert result.page == 1
    assert result.page_size == 3


async def test_async_list_second_page(async_db_session: AsyncSession):
    """Test listing activities on second page via async."""
    for i in range(5):
        await activity.create(
            task_name=f"task_{i}",
            message=f"Message {i}",
            session=async_db_session,
        )

    result = await activity.list(async_db_session, page=2, page_size=3)

    assert len(result.items) == 2
    assert result.total == 5
    assert result.page == 2


async def test_async_list_with_metadata_filter(async_db_session: AsyncSession):
    """Test filtering activities by metadata via async."""
    await activity.create(
        task_name="task_org_a",
        message="Org A",
        session=async_db_session,
        metadata={"organization_id": "org-A"},
    )
    await activity.create(
        task_name="task_org_a_2",
        message="Org A 2",
        session=async_db_session,
        metadata={"organization_id": "org-A"},
    )
    await activity.create(
        task_name="task_org_b",
        message="Org B",
        session=async_db_session,
        metadata={"organization_id": "org-B"},
    )

    result = await activity.list(
        async_db_session,
        metadata_filter={"organization_id": "org-A"},
    )
    assert result.total == 2

    result = await activity.list(
        async_db_session,
        metadata_filter={"organization_id": "org-B"},
    )
    assert result.total == 1


async def test_async_detail_activity(async_db_session: AsyncSession):
    """Test getting activity detail with all logs via async."""
    agent_id = await activity.create(
        task_name="detailed_task",
        message="Initial",
        session=async_db_session,
    )
    await activity.update(
        agent_id=agent_id,
        message="Processing",
        percentage=50,
        session=async_db_session,
    )
    await activity.complete(agent_id=agent_id, session=async_db_session)

    result = await activity.detail(async_db_session, agent_id)

    assert result is not None
    assert result.agent_id == agent_id
    assert result.agent_type == "detailed_task"
    assert len(result.logs) == 3
    assert result.logs[0].message == "Initial"
    assert result.logs[1].message == "Processing"
    assert result.logs[2].status == Status.COMPLETE


async def test_async_detail_not_found(async_db_session: AsyncSession):
    """Test getting detail for non-existent activity via async."""
    fake_id = uuid.uuid4()
    result = await activity.detail(async_db_session, fake_id)
    assert result is None


async def test_async_detail_with_string_id(async_db_session: AsyncSession):
    """Test getting activity detail with string agent_id via async."""
    agent_id = await activity.create(
        task_name="string_id_task",
        message="Test",
        session=async_db_session,
    )

    result = await activity.detail(async_db_session, str(agent_id))
    assert result is not None
    assert result.agent_id == agent_id


async def test_async_detail_with_metadata_filter(async_db_session: AsyncSession):
    """Test detail with metadata filter via async."""
    agent_id = await activity.create(
        task_name="filter_task",
        message="Test",
        session=async_db_session,
        metadata={"organization_id": "org-A"},
    )

    # Should find with matching filter
    result = await activity.detail(
        async_db_session,
        agent_id,
        metadata_filter={"organization_id": "org-A"},
    )
    assert result is not None

    # Should not find with non-matching filter
    result = await activity.detail(
        async_db_session,
        agent_id,
        metadata_filter={"organization_id": "org-B"},
    )
    assert result is None


async def test_async_count_active(async_db_session: AsyncSession):
    """Test counting active agents via async."""
    # Start with zero
    count = await activity.count_active(async_db_session)
    assert count == 0

    # Create a queued activity
    await activity.create(
        task_name="task_1",
        message="Queued",
        session=async_db_session,
    )
    count = await activity.count_active(async_db_session)
    assert count == 1

    # Create and complete another
    agent_id = await activity.create(
        task_name="task_2",
        message="Started",
        session=async_db_session,
    )
    await activity.complete(agent_id=agent_id, session=async_db_session)

    count = await activity.count_active(async_db_session)
    assert count == 1  # Only the first (queued) one is active
