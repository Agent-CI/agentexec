from __future__ import annotations
from enum import Enum as PyEnum
import uuid
from datetime import UTC, datetime

from typing import Any

from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    Uuid,
    case,
    func,
    insert,
    select,
)
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Mapped, Session, aliased, mapped_column, relationship, declared_attr

from agentexec.config import CONF
from agentexec.core.db import Base


class Status(str, PyEnum):
    """Agent execution status."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETE = "complete"
    ERROR = "error"
    CANCELED = "canceled"


class Activity(Base):
    """Tracks background agent execution sessions.

    Each record represents a single agent run. The current status is inferred
    from the latest log message.
    """

    @declared_attr.directive
    def __tablename__(cls) -> str:
        return f"{CONF.table_prefix}activity"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    agent_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False, unique=True, index=True)
    agent_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )
    metadata_: Mapped[dict[str, Any] | None] = mapped_column(
        "metadata",
        JSON,
        nullable=True,
        default=None,
    )

    logs: Mapped[list[ActivityLog]] = relationship(
        "ActivityLog",
        back_populates="activity",
        cascade="all, delete-orphan",
        order_by="ActivityLog.created_at",
    )

    @classmethod
    def append_log(
        cls,
        session: Session,
        agent_id: uuid.UUID,
        message: str,
        status: Status,
        percentage: int | None = None,
    ) -> None:
        """Append a log entry to the activity for the given agent_id.

        This uses a single query to look up the activity_id and insert the log,
        avoiding the need to load the Activity record first.

        Args:
            session: SQLAlchemy session
            agent_id: The agent_id to append the log to
            message: Log message
            status: Current status of the agent
            percentage: Optional completion percentage (0-100)

        Raises:
            ValueError: If agent_id not found (foreign key constraint will fail)
        """
        # Scalar subquery to get activity.id from agent_id
        activity_id_subq = select(cls.id).where(cls.agent_id == agent_id).scalar_subquery()

        # Insert the log using the subquery for activity_id
        stmt = insert(ActivityLog).values(
            activity_id=activity_id_subq,
            message=message,
            status=status,
            percentage=percentage,
        )

        try:
            session.execute(stmt)
            session.commit()
        except Exception as e:
            session.rollback()
            raise ValueError(f"Failed to append log for agent_id {agent_id}") from e

    @classmethod
    def get_by_agent_id(
        cls,
        session: Session,
        agent_id: str | uuid.UUID,
        metadata_filter: dict[str, Any] | None = None,
    ) -> Activity | None:
        """Get an activity by agent_id.

        Args:
            session: SQLAlchemy session
            agent_id: The agent_id to look up (string or UUID)
            metadata_filter: Optional dict of key-value pairs to filter by.
                If provided and the activity's metadata doesn't match,
                returns None (same as if not found).

        Returns:
            Activity object or None if not found or metadata doesn't match

        Example:
            activity = Activity.get_by_agent_id(session, "abc-123")
            # Or with UUID object
            activity = Activity.get_by_agent_id(session, uuid.UUID("abc-123..."))
            if activity:
                print(f"Found activity: {activity.agent_type}")

            # With metadata filter (for multi-tenancy)
            activity = Activity.get_by_agent_id(
                session,
                agent_id,
                metadata_filter={"organization_id": "org-123"}
            )
        """
        # Normalize to UUID if string
        if isinstance(agent_id, str):
            agent_id = uuid.UUID(agent_id)

        query = session.query(cls).filter_by(agent_id=agent_id)

        # Apply metadata filtering if provided
        if metadata_filter:
            for key, value in metadata_filter.items():
                query = query.filter(cls.metadata_[key].as_string() == str(value))

        return query.first()

    @classmethod
    def get_list(
        cls,
        session: Session,
        page: int = 1,
        page_size: int = 50,
        metadata_filter: dict[str, Any] | None = None,
    ) -> list[RowMapping]:
        """Get a paginated list of activities with summary information.

        Args:
            session: SQLAlchemy session to use for the query
            page: Page number (1-indexed)
            page_size: Number of items per page
            metadata_filter: Optional dict of key-value pairs to filter by.
                Activities must have metadata containing all specified keys
                with exactly matching values.

        Returns:
            List of RowMapping objects (dict-like) with keys matching ActivitySummarySchema:
            agent_id, agent_type, latest_log_message, status, latest_log_timestamp,
            percentage, started_at, metadata

        Example:
            results = Activity.get_list(session, page=1, page_size=20)
            for row in results:
                print(f"{row['agent_id']}: {row['latest_log_message']}")

            # Filter by organization
            results = Activity.get_list(
                session,
                metadata_filter={"organization_id": "org-123"}
            )
        """
        # Subquery to get the latest log for each agent
        latest_log_subq = select(
            ActivityLog.activity_id,
            ActivityLog.message,
            ActivityLog.status,
            ActivityLog.created_at,
            ActivityLog.percentage,
            func.row_number()
            .over(
                partition_by=ActivityLog.activity_id,
                order_by=ActivityLog.created_at.desc(),
            )
            .label("rn"),
        ).subquery()

        # Subquery to get start time (first log timestamp)
        started_at_subq = (
            select(
                ActivityLog.activity_id,
                func.min(ActivityLog.created_at).label("started_at"),
            )
            .group_by(ActivityLog.activity_id)
            .subquery()
        )

        # Alias for the subqueries
        latest_log = aliased(latest_log_subq)
        started_at = aliased(started_at_subq)

        # Build base query - select only the columns we need with aliases matching schema
        query = (
            select(
                cls.agent_id,
                cls.agent_type,
                latest_log.c.message.label("latest_log_message"),
                latest_log.c.status,
                latest_log.c.created_at.label("latest_log_timestamp"),
                latest_log.c.percentage,
                started_at.c.started_at,
                cls.metadata_.label("metadata"),
            )
            .outerjoin(
                latest_log,
                (cls.id == latest_log.c.activity_id) & (latest_log.c.rn == 1),
            )
            .outerjoin(started_at, cls.id == started_at.c.activity_id)
        )

        # Apply metadata filtering if provided
        if metadata_filter:
            for key, value in metadata_filter.items():
                # Use JSON path extraction for exact string matching
                # This works across SQLite (for testing) and PostgreSQL (for production)
                query = query.where(cls.metadata_[key].as_string() == str(value))

        # Custom ordering: active agents (running, queued) at the top
        is_active = case(
            (latest_log.c.status.in_([Status.RUNNING, Status.QUEUED]), 0),
            else_=1,
        )
        active_priority = case(
            (latest_log.c.status == Status.RUNNING, 1),
            (latest_log.c.status == Status.QUEUED, 2),
            else_=3,
        )
        query = query.order_by(
            is_active, active_priority, started_at.c.started_at.desc().nullslast()
        )

        # Apply pagination and execute
        offset = (page - 1) * page_size
        return list(session.execute(query.offset(offset).limit(page_size)).mappings().all())

    @classmethod
    def get_pending_ids(cls, session: Session) -> list[uuid.UUID]:
        """Get agent_ids for all activities with QUEUED or RUNNING status.

        Args:
            session: SQLAlchemy session to use for the query

        Returns:
            List of agent_id UUIDs for pending (queued or running) activities

        Example:
            pending_ids = Activity.get_pending_ids(session)
            for agent_id in pending_ids:
                print(f"Pending agent: {agent_id}")
        """
        # Subquery to get the latest log status for each activity
        latest_log_subq = select(
            ActivityLog.activity_id,
            ActivityLog.status,
            func.row_number()
            .over(
                partition_by=ActivityLog.activity_id,
                order_by=ActivityLog.created_at.desc(),
            )
            .label("rn"),
        ).subquery()

        # Query for agent_ids where latest status is queued or running
        result = (
            session.query(cls.agent_id)
            .join(
                latest_log_subq,
                (cls.id == latest_log_subq.c.activity_id) & (latest_log_subq.c.rn == 1),
            )
            .filter(latest_log_subq.c.status.in_([Status.QUEUED, Status.RUNNING]))
            .all()
        )

        # Extract UUIDs from result tuples
        return [agent_id for (agent_id,) in result]

    @classmethod
    def get_active_count(cls, session: Session) -> int:
        """Get count of activities with QUEUED or RUNNING status.

        Args:
            session: SQLAlchemy session to use for the query

        Returns:
            Count of active (queued or running) activities

        Example:
            count = Activity.get_active_count(session)
            print(f"Active agents: {count}")
        """
        # Subquery to get the latest log status for each activity
        latest_log_subq = select(
            ActivityLog.activity_id,
            ActivityLog.status,
            func.row_number()
            .over(
                partition_by=ActivityLog.activity_id,
                order_by=ActivityLog.created_at.desc(),
            )
            .label("rn"),
        ).subquery()

        # Count activities where latest status is queued or running
        result = (
            session.query(func.count(cls.id))
            .join(
                latest_log_subq,
                (cls.id == latest_log_subq.c.activity_id) & (latest_log_subq.c.rn == 1),
            )
            .filter(latest_log_subq.c.status.in_([Status.QUEUED, Status.RUNNING]))
            .scalar()
        )

        return result or 0


class ActivityLog(Base):
    """Individual log messages from background agents.

    Each log entry represents a single update/message from an agent
    during its execution, including the agent's status at that point in time.
    """

    @declared_attr.directive
    def __tablename__(cls) -> str:
        return f"{CONF.table_prefix}activity_log"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    activity_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("agentexec_activity.id"), nullable=False, index=True
    )
    message: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[Status] = mapped_column(Enum(Status), nullable=False, index=True)
    percentage: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )

    # Relationship back to activity
    activity: Mapped[Activity] = relationship("Activity", back_populates="logs")
