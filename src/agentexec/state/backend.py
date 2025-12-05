from types import ModuleType
from typing import AsyncGenerator, Coroutine, Optional, Protocol, runtime_checkable

from pydantic import BaseModel


@runtime_checkable
class StateBackend(Protocol):
    """Protocol defining the state backend interface.

    This protocol defines all the operations needed for:
    - Task queue management (priority queue operations)
    - Result storage (with TTL support)
    - Event coordination (shutdown flags, etc.)
    - Pub/sub messaging (worker logging)

    Any module that implements these functions can serve as a state backend.
    Methods are defined as @staticmethod to match module-level functions.

    Connection management is handled internally - connections are established
    lazily when first accessed. Only cleanup needs to be explicit.
    """

    # Connection management
    @staticmethod
    async def close() -> None:
        """Close all connections to the backend.

        This should close both async and sync connections and clean up
        any resources.
        """
        ...

    # Queue operations (Redis list commands)
    @staticmethod
    def rpush(key: str, value: str) -> int:
        """Push value to the right (front) of the list - for high priority tasks.

        Args:
            key: Redis list key
            value: Serialized task data

        Returns:
            Length of the list after the push
        """
        ...

    @staticmethod
    def lpush(key: str, value: str) -> int:
        """Push value to the left (back) of the list - for low priority tasks.

        Args:
            key: Redis list key
            value: Serialized task data

        Returns:
            Length of the list after the push
        """
        ...

    @staticmethod
    async def brpop(key: str, timeout: int = 0) -> Optional[tuple[str, str]]:
        """Pop value from the right of the list with blocking.

        Args:
            key: Redis list key
            timeout: Timeout in seconds (0 = block forever)

        Returns:
            Tuple of (key, value) or None if timeout
        """
        ...

    # Key-value operations
    @staticmethod
    def aget(key: str) -> Coroutine[None, None, Optional[bytes]]:
        """Get value for key asynchronously.

        Args:
            key: Key to retrieve

        Returns:
            Coroutine that resolves to value as bytes or None if not found
        """
        ...

    @staticmethod
    def get(key: str) -> Optional[bytes]:
        """Get value for key synchronously.

        Args:
            key: Key to retrieve

        Returns:
            Value as bytes or None if not found
        """
        ...

    @staticmethod
    def aset(
        key: str, value: bytes, ttl_seconds: Optional[int] = None
    ) -> Coroutine[None, None, bool]:
        """Set value for key asynchronously with optional TTL.

        Args:
            key: Key to set
            value: Value as bytes
            ttl_seconds: Optional time-to-live in seconds

        Returns:
            Coroutine that resolves to True if successful
        """
        ...

    @staticmethod
    def set(key: str, value: bytes, ttl_seconds: Optional[int] = None) -> bool:
        """Set value for key synchronously with optional TTL.

        Args:
            key: Key to set
            value: Value as bytes
            ttl_seconds: Optional time-to-live in seconds

        Returns:
            True if successful
        """
        ...

    @staticmethod
    def adelete(key: str) -> Coroutine[None, None, int]:
        """Delete key asynchronously.

        Args:
            key: Key to delete

        Returns:
            Coroutine that resolves to number of keys deleted (0 or 1)
        """
        ...

    @staticmethod
    def delete(key: str) -> int:
        """Delete key synchronously.

        Args:
            key: Key to delete

        Returns:
            Number of keys deleted (0 or 1)
        """
        ...

    # Counter operations
    @staticmethod
    def incr(key: str) -> int:
        """Increment a counter atomically.

        Args:
            key: Counter key

        Returns:
            Value after increment
        """
        ...

    @staticmethod
    def decr(key: str) -> int:
        """Decrement a counter atomically.

        Args:
            key: Counter key

        Returns:
            Value after decrement
        """
        ...

    # Pub/sub operations
    @staticmethod
    def publish(channel: str, message: str) -> None:
        """Publish message to a channel.

        Args:
            channel: Channel name
            message: Message to publish
        """
        ...

    @staticmethod
    def subscribe(channel: str) -> AsyncGenerator[str, None]:
        """Subscribe to a channel and yield messages.

        Args:
            channel: Channel name

        Yields:
            Messages from the channel
        """
        ...

    # Key formatting
    @staticmethod
    def format_key(*args: str) -> str:
        """Format a key by joining parts in a backend-specific way.

        Args:
            *args: Parts of the key to join

        Returns:
            Formatted key string
        """
        ...

    # Serialization
    @staticmethod
    def serialize(obj: BaseModel) -> bytes:
        """Serialize a Pydantic BaseModel to bytes.

        Stores the fully qualified class name alongside the data to enable
        automatic type reconstruction during deserialization.

        Args:
            obj: Pydantic BaseModel instance to serialize

        Returns:
            Serialized bytes

        Raises:
            TypeError: If obj is not a BaseModel instance
        """
        ...

    @staticmethod
    def deserialize(data: bytes) -> BaseModel:
        """Deserialize bytes back to a Pydantic BaseModel instance.

        Uses the stored class information to dynamically import and reconstruct
        the original type.

        Args:
            data: Serialized bytes

        Returns:
            Deserialized BaseModel instance

        Raises:
            ImportError: If the class module cannot be imported
            AttributeError: If the class does not exist in the module
            ValueError: If the data is invalid
        """
        ...


def load_backend(module: ModuleType) -> StateBackend:
    """Load and validate a backend module conforms to StateBackend protocol.

    Uses the Protocol's __protocol_attrs__ to determine required methods.

    Args:
        module: Backend module to validate

    Returns:
        The module typed as StateBackend

    Raises:
        TypeError: If the module is missing required functions
    """
    required: frozenset[str] = getattr(StateBackend, "__protocol_attrs__")
    missing = [name for name in required if not hasattr(module, name)]
    if missing:
        raise TypeError(
            f"Backend module '{module.__name__}' missing required functions: {missing}"
        )

    return module  # type: ignore[return-value]
