from __future__ import annotations

import importlib
import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional, TypedDict
from pydantic import BaseModel

if TYPE_CHECKING:
    from agentexec.core.queue import Priority
    from agentexec.schedule import ScheduledTask


class _SerializeWrapper(TypedDict):
    __type__: str
    data: dict[str, Any]


class BaseBackend(ABC):
    """Top-level backend interface with namespaced sub-backends."""

    state: BaseStateBackend
    queue: BaseQueueBackend
    schedule: BaseScheduleBackend

    @abstractmethod
    def format_key(self, *args: str) -> str: ...

    @abstractmethod
    async def close(self) -> None: ...

    def serialize(self, obj: BaseModel) -> bytes:
        """Serialize a Pydantic model to bytes with type information."""
        wrapper: _SerializeWrapper = {
            "__type__": f"{type(obj).__module__}.{type(obj).__qualname__}",
            "data": obj.model_dump(mode="json"),
        }
        return json.dumps(wrapper).encode("utf-8")

    def deserialize(self, data: bytes) -> BaseModel:
        """Deserialize bytes back to a typed Pydantic model."""
        wrapper: _SerializeWrapper = json.loads(data.decode("utf-8"))
        module_path, class_name = wrapper["__type__"].rsplit(".", 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        return cls.model_validate(wrapper["data"])


class BaseStateBackend(ABC):
    """KV store, counters, locks, pub/sub, sorted index."""

    @abstractmethod
    async def get(self, key: str) -> Optional[bytes]: ...

    @abstractmethod
    async def set(self, key: str, value: bytes, ttl_seconds: Optional[int] = None) -> bool: ...

    @abstractmethod
    async def delete(self, key: str) -> int: ...

    @abstractmethod
    async def counter_incr(self, key: str) -> int: ...

    @abstractmethod
    async def counter_decr(self, key: str) -> int: ...


class BaseQueueBackend(ABC):
    """Task queue with push/pop semantics and partition-level locking."""

    @abstractmethod
    async def push(
        self,
        value: str,
        *,
        priority: Priority | None = None,
        partition_key: str | None = None,
    ) -> None: ...

    @abstractmethod
    async def pop(self, *, timeout: int = 1) -> dict[str, Any] | None: ...

    @abstractmethod
    async def complete(self, partition_key: str | None) -> None:
        """Signal that the current task for this partition is done."""
        ...



class BaseScheduleBackend(ABC):
    """Schedule storage and retrieval."""

    @abstractmethod
    async def register(self, task: ScheduledTask) -> None:
        """Store a scheduled task definition."""
        ...

    @abstractmethod
    async def get_due(self) -> list[ScheduledTask]:
        """Return all scheduled tasks that are due to fire."""
        ...

    @abstractmethod
    async def remove(self, key: str) -> None:
        """Remove a schedule by its key."""
        ...
