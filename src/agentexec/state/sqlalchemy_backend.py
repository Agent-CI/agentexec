"""SQLAlchemy state backend implementation.

Uses SQLAlchemy for all state management, supporting any SQLAlchemy-compatible database.
Tables can be managed via Alembic migrations.

Queue: SELECT FOR UPDATE SKIP LOCKED pattern
Key-value: Table with expires_at column
Counters: Atomic UPDATE with RETURNING
Pub/sub: Polling-based (database-agnostic) or LISTEN/NOTIFY for PostgreSQL
"""

from __future__ import annotations

import asyncio
import importlib
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, AsyncGenerator, Coroutine, Optional, TypedDict

from pydantic import BaseModel
from sqlalchemy import (
    BigInteger,
    DateTime,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    delete as sa_delete,
    func,
    insert,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Mapped, Session, mapped_column, declared_attr

from agentexec.config import CONF
from agentexec.core.db import Base, get_global_session

if TYPE_CHECKING:
    pass

__all__ = [
    "format_key",
    "serialize",
    "deserialize",
    "rpush",
    "lpush",
    "brpop",
    "aget",
    "get",
    "aset",
    "set",
    "adelete",
    "delete",
    "incr",
    "decr",
    "publish",
    "subscribe",
    "close",
    "QueueItem",
    "KeyValue",
    "Counter",
]


# =============================================================================
# Models - Add to Alembic migrations
# =============================================================================


class QueueItem(Base):
    """Task queue item for FIFO processing with priority support."""

    @declared_attr.directive
    def __tablename__(cls) -> str:
        return f"{CONF.table_prefix}queue"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    queue_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index(
            "ix_queue_pop", "queue_name", priority.desc(), "id",  # type: ignore[attr-defined]
        ),
    )


class KeyValue(Base):
    """Key-value store with optional TTL."""

    @declared_attr.directive
    def __tablename__(cls) -> str:
        return f"{CONF.table_prefix}kv"

    key: Mapped[str] = mapped_column(String(512), primary_key=True)
    value: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )


class Counter(Base):
    """Atomic counter storage."""

    @declared_attr.directive
    def __tablename__(cls) -> str:
        return f"{CONF.table_prefix}counters"

    key: Mapped[str] = mapped_column(String(512), primary_key=True)
    value: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)


class Notification(Base):
    """Pub/sub notifications (polling-based fallback)."""

    @declared_attr.directive
    def __tablename__(cls) -> str:
        return f"{CONF.table_prefix}notifications"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )


# =============================================================================
# Serialization (shared with Redis backend)
# =============================================================================


class SerializeWrapper(TypedDict):
    __class__: str
    __data__: str


def format_key(*args: str) -> str:
    """Format a key by joining parts with underscores.

    Uses underscores instead of colons for PostgreSQL LISTEN/NOTIFY compatibility.
    """
    return "_".join(args)


def serialize(obj: BaseModel) -> bytes:
    """Serialize a Pydantic BaseModel to JSON bytes with type information."""
    if not isinstance(obj, BaseModel):
        raise TypeError(f"Expected BaseModel, got {type(obj)}")

    cls = type(obj)
    wrapper: SerializeWrapper = {
        "__class__": f"{cls.__module__}.{cls.__qualname__}",
        "__data__": obj.model_dump_json(),
    }
    return json.dumps(wrapper).encode("utf-8")


def deserialize(data: bytes) -> BaseModel:
    """Deserialize JSON bytes back to a Pydantic BaseModel instance."""
    wrapper: SerializeWrapper = json.loads(data.decode("utf-8"))
    class_path = wrapper["__class__"]
    json_data = wrapper["__data__"]

    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)

    result: BaseModel = cls.model_validate_json(json_data)
    return result


# =============================================================================
# Connection Management
# =============================================================================


async def close() -> None:
    """Close connections (no-op for SQLAlchemy - managed by engine)."""
    pass


def _get_session() -> Session:
    """Get the current SQLAlchemy session."""
    return get_global_session()


def _cleanup_expired() -> None:
    """Remove expired key-value entries."""
    session = _get_session()
    session.execute(
        sa_delete(KeyValue).where(
            KeyValue.expires_at.isnot(None), KeyValue.expires_at < datetime.now(UTC)
        )
    )
    session.commit()


# =============================================================================
# Queue Operations
# =============================================================================


def rpush(key: str, value: str) -> int:
    """Push value with high priority (processed first)."""
    session = _get_session()
    session.execute(insert(QueueItem).values(queue_name=key, value=value, priority=1))
    session.commit()

    result = session.execute(
        select(func.count()).select_from(QueueItem).where(QueueItem.queue_name == key)
    )
    return result.scalar() or 0


def lpush(key: str, value: str) -> int:
    """Push value with low priority (processed last)."""
    session = _get_session()
    session.execute(insert(QueueItem).values(queue_name=key, value=value, priority=0))
    session.commit()

    result = session.execute(
        select(func.count()).select_from(QueueItem).where(QueueItem.queue_name == key)
    )
    return result.scalar() or 0


async def brpop(key: str, timeout: int = 0) -> Optional[tuple[str, str]]:
    """Pop highest priority item from queue with blocking.

    Uses SELECT FOR UPDATE SKIP LOCKED for safe concurrent access.
    Polls with exponential backoff when queue is empty.
    """
    start_time = asyncio.get_event_loop().time()
    poll_interval = 0.1
    max_poll_interval = 2.0

    while True:
        # Periodically clean up expired KV entries
        _cleanup_expired()

        session = _get_session()

        # Find and lock the next item
        subq = (
            select(QueueItem.id)
            .where(QueueItem.queue_name == key)
            .order_by(QueueItem.priority.desc(), QueueItem.id.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
            .scalar_subquery()
        )

        # Delete and return the item
        result = session.execute(
            sa_delete(QueueItem).where(QueueItem.id == subq).returning(
                QueueItem.queue_name, QueueItem.value
            )
        )
        row = result.fetchone()
        session.commit()

        if row:
            return (row[0], row[1])

        # Check timeout
        if timeout > 0:
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout:
                return None

        # Wait with exponential backoff
        await asyncio.sleep(poll_interval)
        poll_interval = min(poll_interval * 1.5, max_poll_interval)


# =============================================================================
# Key-Value Operations
# =============================================================================


def aget(key: str) -> Coroutine[None, None, Optional[bytes]]:
    """Get value for key asynchronously."""

    async def _aget() -> Optional[bytes]:
        return get(key)

    return _aget()


def get(key: str) -> Optional[bytes]:
    """Get value for key synchronously."""
    session = _get_session()
    result = session.execute(
        select(KeyValue.value).where(
            KeyValue.key == key,
            (KeyValue.expires_at.is_(None)) | (KeyValue.expires_at > datetime.now(UTC)),
        )
    )
    row = result.fetchone()
    return row[0] if row else None


def aset(
    key: str, value: bytes, ttl_seconds: Optional[int] = None
) -> Coroutine[None, None, bool]:
    """Set value for key asynchronously with optional TTL."""

    async def _aset() -> bool:
        return set(key, value, ttl_seconds)

    return _aset()


def set(key: str, value: bytes, ttl_seconds: Optional[int] = None) -> bool:
    """Set value for key synchronously with optional TTL."""
    session = _get_session()
    expires_at = None
    if ttl_seconds is not None:
        expires_at = datetime.now(UTC) + timedelta(seconds=ttl_seconds)

    # Use dialect-specific upsert for PostgreSQL, otherwise merge
    if _get_dialect(session) == "postgresql":
        stmt = pg_insert(KeyValue).values(key=key, value=value, expires_at=expires_at)
        stmt = stmt.on_conflict_do_update(
            index_elements=["key"], set_={"value": value, "expires_at": expires_at}
        )
        session.execute(stmt)
    else:
        # SQLite and others: delete then insert
        session.execute(sa_delete(KeyValue).where(KeyValue.key == key))
        session.execute(
            insert(KeyValue).values(key=key, value=value, expires_at=expires_at)
        )

    session.commit()
    return True


def adelete(key: str) -> Coroutine[None, None, int]:
    """Delete key asynchronously."""

    async def _adelete() -> int:
        return delete_key(key)

    return _adelete()


def delete_key(key: str) -> int:
    """Delete key synchronously."""
    session = _get_session()
    result = session.execute(sa_delete(KeyValue).where(KeyValue.key == key))
    session.commit()
    return result.rowcount  # type: ignore[return-value]


# Alias for protocol compatibility
delete = delete_key  # type: ignore[assignment]


# =============================================================================
# Counter Operations
# =============================================================================


def incr(key: str) -> int:
    """Increment a counter atomically."""
    session = _get_session()

    if _get_dialect(session) == "postgresql":
        stmt = pg_insert(Counter).values(key=key, value=1)
        stmt = stmt.on_conflict_do_update(
            index_elements=["key"], set_={"value": Counter.value + 1}
        )
        session.execute(stmt)
        session.commit()
        result = session.execute(select(Counter.value).where(Counter.key == key))
        row = result.fetchone()
        return row[0] if row else 1
    else:
        # SQLite: check exists then insert or update
        result = session.execute(select(Counter.value).where(Counter.key == key))
        row = result.fetchone()
        if row:
            new_val = row[0] + 1
            session.execute(
                update(Counter).where(Counter.key == key).values(value=new_val)
            )
        else:
            new_val = 1
            session.execute(insert(Counter).values(key=key, value=new_val))
        session.commit()
        return new_val


def decr(key: str) -> int:
    """Decrement a counter atomically."""
    session = _get_session()

    if _get_dialect(session) == "postgresql":
        stmt = pg_insert(Counter).values(key=key, value=-1)
        stmt = stmt.on_conflict_do_update(
            index_elements=["key"], set_={"value": Counter.value - 1}
        )
        session.execute(stmt)
        session.commit()
        result = session.execute(select(Counter.value).where(Counter.key == key))
        row = result.fetchone()
        return row[0] if row else -1
    else:
        result = session.execute(select(Counter.value).where(Counter.key == key))
        row = result.fetchone()
        if row:
            new_val = row[0] - 1
            session.execute(
                update(Counter).where(Counter.key == key).values(value=new_val)
            )
        else:
            new_val = -1
            session.execute(insert(Counter).values(key=key, value=new_val))
        session.commit()
        return new_val


# =============================================================================
# Pub/Sub Operations
# - PostgreSQL: Native LISTEN/NOTIFY (real-time)
# - Others: Polling-based fallback via Notification table
# =============================================================================


def _get_dialect(session: Session) -> str:
    """Get the database dialect name from a session."""
    return session.bind.dialect.name if session.bind else "default"


def publish(channel: str, message: str) -> None:
    """Publish message to a channel.

    PostgreSQL: Uses NOTIFY for real-time delivery.
    Others: Inserts into Notification table for polling.
    """
    session = _get_session()
    dialect = _get_dialect(session)

    if dialect == "postgresql":
        # Use native NOTIFY
        session.execute(
            text(f"NOTIFY {channel}, :message"),  # noqa: S608
            {"message": message},
        )
        session.commit()
    else:
        # Fallback to notification table
        session.execute(insert(Notification).values(channel=channel, message=message))
        session.commit()

        # Clean up old notifications (keep last hour)
        cutoff = datetime.now(UTC) - timedelta(hours=1)
        session.execute(sa_delete(Notification).where(Notification.created_at < cutoff))
        session.commit()


async def subscribe(channel: str) -> AsyncGenerator[str, None]:
    """Subscribe to a channel and yield messages.

    PostgreSQL: Uses LISTEN for real-time notifications.
    Others: Polls the Notification table.
    """
    session = _get_session()
    dialect = _get_dialect(session)

    if dialect == "postgresql":
        # Use native LISTEN/NOTIFY
        connection = session.connection()

        # Enable autocommit for LISTEN to work
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(text(f"LISTEN {channel}"))  # noqa: S608

        try:
            while True:
                # Get raw DBAPI connection for notification polling
                # psycopg exposes .poll() and .notifies for async notifications
                raw_conn = connection.connection.dbapi_connection
                import select as sel

                if sel.select([raw_conn], [], [], 0.5) != ([], [], []):
                    raw_conn.poll()  # type: ignore[union-attr]
                    while raw_conn.notifies:  # type: ignore[union-attr]
                        notify = raw_conn.notifies.pop(0)  # type: ignore[union-attr]
                        yield notify.payload
                else:
                    await asyncio.sleep(0.1)
        finally:
            connection.execute(text(f"UNLISTEN {channel}"))  # noqa: S608
    else:
        # Fallback to polling notification table
        last_id = 0

        # Get current max ID to only receive new messages
        result = session.execute(
            select(func.max(Notification.id)).where(Notification.channel == channel)
        )
        row = result.fetchone()
        if row and row[0]:
            last_id = row[0]

        poll_interval = 0.5

        while True:
            result = session.execute(
                select(Notification.id, Notification.message)
                .where(Notification.channel == channel, Notification.id > last_id)
                .order_by(Notification.id.asc())
            )
            rows = result.fetchall()

            for row in rows:
                last_id = row[0]
                yield row[1]

            await asyncio.sleep(poll_interval)
