from __future__ import annotations

import logging
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
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, aliased, mapped_column, relationship, declared_attr, selectinload

from agentexec.activity.status import Status
from agentexec.config import CONF
from agentexec.core.db import Base

logger = logging.getLogger(__name__)


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
    async def create(
        cls,
        session: AsyncSession,
        agent_id: uuid.UUID,
        task_name: str,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> Activity:
        """Create a new activity record with an initial queued log entry.

        Args:
            session: Async SQLAlchemy session.
            agent_id: Unique agent identifier.
            task_name: The registered task name.
            message: Initial log message.
            metadata: Optional metadata dict.

        Returns:
            The created Activity record.
        """
        record = cls(
            agent_id=agent_id,
            agent_type=task_name,
            metadata_=metadata,
        )
        session.add(record)
        await session.flush()
        session.add(
            ActivityLog(
                activity_id=record.id,
                message=message,
                status=Status.QUEUED,
                percentage=0,
            )
        )
        await session.commit()
        return record

    @classmethod
    async def append_log(
        cls,
        session: AsyncSession,
        agent_id: uuid.UUID,
        message: str,
        status: Status,
        percentage: int | None = None,
    ) -> None:
        """Append a log entry to the activity for the given agent_id.

        Looks up the activity by agent_id and inserts a log entry. If no
        activity record exists (e.g. stale task from a previous session),
        logs a warning and returns without raising.

        Args:
            session: Async SQLAlchemy session.
            agent_id: The agent_id to append the log to.
            message: Log message.
            status: Current status of the agent.
            percentage: Optional completion percentage (0-100).
        """
        result = await session.execute(select(cls.id).where(cls.agent_id == agent_id))
        activity_id = result.scalar_one_or_none()

        if activity_id is None:
            logger.warning(
                f"No activity record for agent_id {agent_id}, skipping log append. "
                f"This can happen when a stale task from a previous session is picked up."
            )
            return

        await session.execute(
            insert(ActivityLog).values(
                activity_id=activity_id,
                message=message,
                status=status,
                percentage=percentage,
            )
        )
        await session.commit()

    @classmethod
    async def get_by_agent_id(
        cls,
        session: AsyncSession,
        agent_id: str | uuid.UUID,
        metadata_filter: dict[str, Any] | None = None,
    ) -> Activity | None:
        """Get an activity by agent_id.

        Args:
            session: Async SQLAlchemy session.
            agent_id: The agent_id to look up (string or UUID).
            metadata_filter: Optional dict of key-value pairs to filter by.

        Returns:
            Activity object or None if not found or metadata doesn't match.
        """
        if isinstance(agent_id, str):
            agent_id = uuid.UUID(agent_id)

        query = select(cls).options(selectinload(cls.logs)).where(cls.agent_id == agent_id)

        if metadata_filter:
            for key, value in metadata_filter.items():
                query = query.where(cls.metadata_[key].as_string() == str(value))

        result = await session.execute(query)
        return result.scalar_one_or_none()

    @classmethod
    async def get_list(
        cls,
        session: AsyncSession,
        page: int = 1,
        page_size: int = 50,
        metadata_filter: dict[str, Any] | None = None,
    ) -> list[RowMapping]:
        """Get a paginated list of activities with summary information.

        Args:
            session: Async SQLAlchemy session.
            page: Page number (1-indexed).
            page_size: Number of items per page.
            metadata_filter: Optional dict of key-value pairs to filter by.

        Returns:
            List of RowMapping objects with activity summary fields.
        """
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

        started_at_subq = (
            select(
                ActivityLog.activity_id,
                func.min(ActivityLog.created_at).label("started_at"),
            )
            .group_by(ActivityLog.activity_id)
            .subquery()
        )

        latest_log = aliased(latest_log_subq)
        started_at = aliased(started_at_subq)

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

        if metadata_filter:
            for key, value in metadata_filter.items():
                query = query.where(cls.metadata_[key].as_string() == str(value))

        is_active = case(
            (latest_log.c.status.in_([Status.RUNNING, Status.QUEUED]), 0),
            else_=1,
        )
        active_priority = case(
            (latest_log.c.status == Status.RUNNING, 1),
            (latest_log.c.status == Status.QUEUED, 2),
            else_=3,
        )
        query = query.order_by(is_active, active_priority, started_at.c.started_at.desc().nullslast())

        offset = (page - 1) * page_size
        result = await session.execute(query.offset(offset).limit(page_size))
        return list(result.mappings().all())

    @classmethod
    async def get_pending_ids(cls, session: AsyncSession) -> list[uuid.UUID]:
        """Get agent_ids for all activities with QUEUED or RUNNING status.

        Args:
            session: Async SQLAlchemy session.

        Returns:
            List of agent_id UUIDs for pending (queued or running) activities.
        """
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

        query = (
            select(cls.agent_id)
            .join(
                latest_log_subq,
                (cls.id == latest_log_subq.c.activity_id) & (latest_log_subq.c.rn == 1),
            )
            .where(latest_log_subq.c.status.in_([Status.QUEUED, Status.RUNNING]))
        )

        result = await session.execute(query)
        return [row[0] for row in result.all()]

    @classmethod
    async def get_active_count(cls, session: AsyncSession) -> int:
        """Get count of activities with QUEUED or RUNNING status.

        Args:
            session: Async SQLAlchemy session.

        Returns:
            Count of active (queued or running) activities.
        """
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

        query = (
            select(func.count(cls.id))
            .join(
                latest_log_subq,
                (cls.id == latest_log_subq.c.activity_id) & (latest_log_subq.c.rn == 1),
            )
            .where(latest_log_subq.c.status.in_([Status.QUEUED, Status.RUNNING]))
        )

        result = await session.execute(query)
        return result.scalar() or 0


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

    activity: Mapped[Activity] = relationship("Activity", back_populates="logs")
