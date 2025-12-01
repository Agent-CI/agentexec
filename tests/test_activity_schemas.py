"""Test activity schema validation and computed fields."""

import uuid
from datetime import datetime, timedelta, UTC

import pytest

from agentexec.activity.models import Status
from agentexec.activity.schemas import (
    ActivityDetailSchema,
    ActivityListItemSchema,
    ActivityListSchema,
    ActivityLogSchema,
)


def test_activity_log_schema():
    """Test ActivityLogSchema validation."""
    log = ActivityLogSchema(
        id=uuid.uuid4(),
        message="Test message",
        status=Status.RUNNING,
        completion_percentage=50,
        created_at=datetime.now(UTC),
    )

    assert log.message == "Test message"
    assert log.status == Status.RUNNING
    assert log.completion_percentage == 50


def test_activity_log_schema_default_percentage():
    """Test ActivityLogSchema default completion percentage."""
    log = ActivityLogSchema(
        id=uuid.uuid4(),
        message="Test",
        status=Status.QUEUED,
        created_at=datetime.now(UTC),
    )

    assert log.completion_percentage == 0


def test_activity_detail_schema():
    """Test ActivityDetailSchema validation."""
    now = datetime.now(UTC)
    detail = ActivityDetailSchema(
        id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        agent_type="test_agent",
        created_at=now,
        updated_at=now,
        logs=[
            ActivityLogSchema(
                id=uuid.uuid4(),
                message="Log entry",
                status=Status.RUNNING,
                completion_percentage=25,
                created_at=now,
            )
        ],
    )

    assert detail.agent_type == "test_agent"
    assert len(detail.logs) == 1


def test_activity_detail_schema_empty_logs():
    """Test ActivityDetailSchema with no logs."""
    now = datetime.now(UTC)
    detail = ActivityDetailSchema(
        id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        agent_type="test_agent",
        created_at=now,
        updated_at=now,
    )

    assert detail.logs == []


def test_activity_list_item_schema():
    """Test ActivityListItemSchema validation."""
    now = datetime.now(UTC)
    item = ActivityListItemSchema(
        agent_id=uuid.uuid4(),
        agent_type="test_agent",
        status=Status.RUNNING,
        latest_log_message="Processing",
        latest_log_timestamp=now,
        completion_percentage=75,
        started_at=now - timedelta(seconds=30),
    )

    assert item.status == Status.RUNNING
    assert item.completion_percentage == 75


def test_activity_list_item_elapsed_time_computed():
    """Test elapsed_time_seconds computed field."""
    started = datetime.now(UTC) - timedelta(seconds=120)
    latest = datetime.now(UTC)

    item = ActivityListItemSchema(
        agent_id=uuid.uuid4(),
        agent_type="test_agent",
        status=Status.RUNNING,
        latest_log_message="Running",
        latest_log_timestamp=latest,
        completion_percentage=50,
        started_at=started,
    )

    # Should be approximately 120 seconds (with a small tolerance)
    assert 119 <= item.elapsed_time_seconds <= 121


def test_activity_list_item_elapsed_time_no_timestamps():
    """Test elapsed_time_seconds returns 0 when timestamps are None."""
    item = ActivityListItemSchema(
        agent_id=uuid.uuid4(),
        agent_type="test_agent",
        status=Status.QUEUED,
        latest_log_message=None,
        latest_log_timestamp=None,
        started_at=None,
    )

    assert item.elapsed_time_seconds == 0


def test_activity_list_item_elapsed_time_missing_started_at():
    """Test elapsed_time_seconds returns 0 when started_at is None."""
    item = ActivityListItemSchema(
        agent_id=uuid.uuid4(),
        agent_type="test_agent",
        status=Status.RUNNING,
        latest_log_timestamp=datetime.now(UTC),
        started_at=None,
    )

    assert item.elapsed_time_seconds == 0


def test_activity_list_schema():
    """Test ActivityListSchema validation."""
    items = [
        ActivityListItemSchema(
            agent_id=uuid.uuid4(),
            agent_type=f"agent_{i}",
            status=Status.COMPLETE,
        )
        for i in range(3)
    ]

    list_schema = ActivityListSchema(
        items=items,
        total=10,
        page=1,
        page_size=3,
    )

    assert len(list_schema.items) == 3
    assert list_schema.total == 10


def test_activity_list_total_pages_computed():
    """Test total_pages computed field."""
    list_schema = ActivityListSchema(
        items=[],
        total=25,
        page=1,
        page_size=10,
    )

    assert list_schema.total_pages == 3  # ceil(25/10) = 3


def test_activity_list_total_pages_exact_division():
    """Test total_pages when total is exactly divisible by page_size."""
    list_schema = ActivityListSchema(
        items=[],
        total=20,
        page=1,
        page_size=10,
    )

    assert list_schema.total_pages == 2


def test_activity_list_total_pages_single_page():
    """Test total_pages when all items fit on one page."""
    list_schema = ActivityListSchema(
        items=[],
        total=5,
        page=1,
        page_size=10,
    )

    assert list_schema.total_pages == 1


def test_activity_list_total_pages_empty():
    """Test total_pages when there are no items."""
    list_schema = ActivityListSchema(
        items=[],
        total=0,
        page=1,
        page_size=10,
    )

    assert list_schema.total_pages == 0


def test_activity_log_schema_all_status_values():
    """Test ActivityLogSchema with all status values."""
    for status in Status:
        log = ActivityLogSchema(
            id=uuid.uuid4(),
            message=f"Status: {status}",
            status=status,
            created_at=datetime.now(UTC),
        )
        assert log.status == status
