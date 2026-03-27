"""Stream backend protocol.

Defines the interface for backends that provide stream/queue semantics:
producing messages, consuming messages, and topic management.

Kafka is the canonical implementation. Stream backends handle task
distribution, log streaming, and activity persistence as ordered,
partitioned event streams.
"""

from __future__ import annotations

from typing import Any, AsyncGenerator, Optional, Protocol, runtime_checkable


class StreamRecord:
    """A record consumed from a stream.

    Attributes:
        topic: Source topic name.
        key: Record key (may be None for non-keyed topics).
        value: Record payload as bytes.
        headers: Record headers as a dict.
        partition: Partition number.
        offset: Offset within the partition.
        timestamp: Record timestamp in milliseconds.
    """

    __slots__ = ("topic", "key", "value", "headers", "partition", "offset", "timestamp")

    def __init__(
        self,
        topic: str,
        key: str | None,
        value: bytes,
        headers: dict[str, bytes],
        partition: int,
        offset: int,
        timestamp: int,
    ) -> None:
        self.topic = topic
        self.key = key
        self.value = value
        self.headers = headers
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp


@runtime_checkable
class StreamBackend(Protocol):
    """Protocol for stream-based backends (e.g. Kafka).

    Stream backends treat everything as ordered, partitioned event streams.
    Partition assignment provides natural task isolation (replacing locks),
    and guaranteed ordering within a partition (replacing priority hacks).

    Key concepts:
    - **topic**: A named stream of records (replaces Redis lists, channels).
    - **partition_key**: Determines which partition a record lands in.
      Used to co-locate related work (e.g. all tasks for a user).
    - **consumer_group**: A set of consumers that cooperatively consume
      a topic, each partition assigned to exactly one consumer.
    """

    # -- Connection management ------------------------------------------------

    @staticmethod
    async def close() -> None:
        """Close all connections (producer, consumers) and release resources."""
        ...

    # -- Produce --------------------------------------------------------------

    @staticmethod
    async def produce(
        topic: str,
        value: bytes,
        *,
        key: str | None = None,
        headers: dict[str, bytes] | None = None,
    ) -> None:
        """Produce a message to a topic.

        Args:
            topic: Target topic name.
            value: Message payload as bytes.
            key: Optional partition key. Messages with the same key go to the
                same partition, guaranteeing order for that key.
            headers: Optional message headers (metadata that doesn't affect
                partitioning).
        """
        ...

    @staticmethod
    def produce_sync(
        topic: str,
        value: bytes,
        *,
        key: str | None = None,
        headers: dict[str, bytes] | None = None,
    ) -> None:
        """Produce a message to a topic (sync).

        Same as produce() but blocks until delivery is confirmed.
        Used from synchronous contexts (e.g. logging handlers).
        """
        ...

    # -- Consume --------------------------------------------------------------

    @staticmethod
    async def consume(
        topic: str,
        group_id: str,
        *,
        timeout_ms: int = 1000,
    ) -> AsyncGenerator[StreamRecord, None]:
        """Consume messages from a topic as an async generator.

        Messages are yielded one at a time. The consumer commits offsets
        after each message is yielded (at-least-once semantics).

        Args:
            topic: Topic to consume from.
            group_id: Consumer group ID. Partitions are distributed among
                consumers in the same group.
            timeout_ms: Poll timeout in milliseconds.

        Yields:
            StreamRecord instances.
        """
        ...

    # -- Topic management -----------------------------------------------------

    @staticmethod
    async def ensure_topic(
        topic: str,
        *,
        num_partitions: int | None = None,
        compact: bool = False,
        retention_ms: int | None = None,
    ) -> None:
        """Ensure a topic exists, creating it if necessary.

        Args:
            topic: Topic name.
            num_partitions: Number of partitions. Defaults to backend config.
            compact: If True, enable log compaction (latest value per key
                survives). Used for state topics (results, schedules).
            retention_ms: Optional retention period in milliseconds.
                None means use broker default.
        """
        ...

    @staticmethod
    async def delete_topic(topic: str) -> None:
        """Delete a topic and all its data."""
        ...

    # -- Key-value over streams (compacted topics) ----------------------------

    @staticmethod
    async def put(topic: str, key: str, value: bytes) -> None:
        """Write a keyed record to a compacted topic.

        This is a convenience over produce() that enforces the key requirement
        for compacted topics used as key-value stores.

        Args:
            topic: A compacted topic name.
            key: Record key (required for compaction semantics).
            value: Record value.
        """
        ...

    @staticmethod
    async def tombstone(topic: str, key: str) -> None:
        """Write a tombstone (null value) to a compacted topic.

        After compaction, the key will be removed from the topic.

        Args:
            topic: A compacted topic name.
            key: Record key to delete.
        """
        ...
