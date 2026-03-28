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
    Select,
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
from sqlalchemy.orm import (
    Mapped,
    Session,
    aliased,
    mapped_column,
    relationship,
    declared_attr,
    selectinload,
)
from sqlalchemy.sql.dml import Insert

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

    # ------------------------------------------------------------------
    # Statement builders (shared between sync and async)
    # ------------------------------------------------------------------

    @classmethod
    def _append_log_stmt(
        cls,
        agent_id: uuid.UUID,
        message: str,
        status: Status,
        percentage: int | None,
    ) -> Insert:
        activity_id_subq = select(cls.id).where(cls.agent_id == agent_id).scalar_subquery()
        return insert(ActivityLog).values(
            activity_id=activity_id_subq,
            message=message,
            status=status,
            percentage=percentage,
        )

    @classmethod
    def _get_by_agent_id_stmt(
        cls,
        agent_id: uuid.UUID,
        metadata_filter: dict[str, Any] | None = None,
    ) -> Select:
        stmt = select(cls).options(selectinload(cls.logs)).filter_by(agent_id=agent_id)
        if metadata_filter:
            for key, value in metadata_filter.items():
                stmt = stmt.filter(cls.metadata_[key].as_string() == str(value))
        return stmt

    @classmethod
    def _get_list_stmt(
        cls,
        page: int,
        page_size: int,
        metadata_filter: dict[str, Any] | None = None,
    ) -> Select:
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
        query = query.order_by(
            is_active, active_priority, started_at.c.started_at.desc().nullslast()
        )

        offset = (page - 1) * page_size
        return query.offset(offset).limit(page_size)

    @classmethod
    def _get_pending_ids_stmt(cls) -> Select:
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

        return (
            select(cls.agent_id)
            .join(
                latest_log_subq,
                (cls.id == latest_log_subq.c.activity_id) & (latest_log_subq.c.rn == 1),
            )
            .filter(latest_log_subq.c.status.in_([Status.QUEUED, Status.RUNNING]))
        )

    @classmethod
    def _get_active_count_stmt(cls) -> Select:
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

        return (
            select(func.count(cls.id))
            .join(
                latest_log_subq,
                (cls.id == latest_log_subq.c.activity_id) & (latest_log_subq.c.rn == 1),
            )
            .filter(latest_log_subq.c.status.in_([Status.QUEUED, Status.RUNNING]))
        )

    # ------------------------------------------------------------------
    # Sync execution
    # ------------------------------------------------------------------

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

        Uses a subquery insert to avoid loading the Activity record.

        Args:
            session: SQLAlchemy session
            agent_id: The agent_id to append the log to
            message: Log message
            status: Current status of the agent
            percentage: Optional completion percentage (0-100)

        Raises:
            ValueError: If agent_id not found
        """
        stmt = cls._append_log_stmt(agent_id, message, status, percentage)
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
            metadata_filter: Optional metadata key-value filter.

        Returns:
            Activity or None
        """
        if isinstance(agent_id, str):
            agent_id = uuid.UUID(agent_id)
        stmt = cls._get_by_agent_id_stmt(agent_id, metadata_filter)
        result = session.execute(stmt)
        return result.scalars().first()

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
            session: SQLAlchemy session
            page: Page number (1-indexed)
            page_size: Number of items per page
            metadata_filter: Optional metadata key-value filter.

        Returns:
            List of RowMapping dicts with summary fields.
        """
        stmt = cls._get_list_stmt(page, page_size, metadata_filter)
        return list(session.execute(stmt).mappings().all())

    @classmethod
    def get_pending_ids(cls, session: Session) -> list[uuid.UUID]:
        """Get agent_ids for all activities with QUEUED or RUNNING status.

        Args:
            session: SQLAlchemy session

        Returns:
            List of agent_id UUIDs
        """
        result = session.execute(cls._get_pending_ids_stmt())
        return [row[0] for row in result.all()]

    @classmethod
    def get_active_count(cls, session: Session) -> int:
        """Get count of activities with QUEUED or RUNNING status.

        Args:
            session: SQLAlchemy session

        Returns:
            Count of active activities
        """
        result = session.execute(cls._get_active_count_stmt())
        return result.scalar() or 0

    # ------------------------------------------------------------------
    # Async execution
    # ------------------------------------------------------------------

    @classmethod
    async def aappend_log(
        cls,
        session: AsyncSession,
        agent_id: uuid.UUID,
        message: str,
        status: Status,
        percentage: int | None = None,
    ) -> None:
        """Async version of :meth:`append_log`."""
        stmt = cls._append_log_stmt(agent_id, message, status, percentage)
        try:
            await session.execute(stmt)
            await session.commit()
        except Exception as e:
            await session.rollback()
            raise ValueError(f"Failed to append log for agent_id {agent_id}") from e

    @classmethod
    async def aget_by_agent_id(
        cls,
        session: AsyncSession,
        agent_id: str | uuid.UUID,
        metadata_filter: dict[str, Any] | None = None,
    ) -> Activity | None:
        """Async version of :meth:`get_by_agent_id`."""
        if isinstance(agent_id, str):
            agent_id = uuid.UUID(agent_id)
        stmt = cls._get_by_agent_id_stmt(agent_id, metadata_filter)
        result = await session.execute(stmt)
        return result.scalars().first()

    @classmethod
    async def aget_list(
        cls,
        session: AsyncSession,
        page: int = 1,
        page_size: int = 50,
        metadata_filter: dict[str, Any] | None = None,
    ) -> list[RowMapping]:
        """Async version of :meth:`get_list`."""
        stmt = cls._get_list_stmt(page, page_size, metadata_filter)
        result = await session.execute(stmt)
        return list(result.mappings().all())

    @classmethod
    async def aget_pending_ids(cls, session: AsyncSession) -> list[uuid.UUID]:
        """Async version of :meth:`get_pending_ids`."""
        result = await session.execute(cls._get_pending_ids_stmt())
        return [row[0] for row in result.all()]

    @classmethod
    async def aget_active_count(cls, session: AsyncSession) -> int:
        """Async version of :meth:`get_active_count`."""
        result = await session.execute(cls._get_active_count_stmt())
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

    # Relationship back to activity
    activity: Mapped[Activity] = relationship("Activity", back_populates="logs")
