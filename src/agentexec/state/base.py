"""Abstract base classes for state backends."""

from __future__ import annotations

import importlib
import json
from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Optional, TypedDict
from uuid import UUID

from pydantic import BaseModel


class _SerializeWrapper(TypedDict):
    __type__: str
    data: dict[str, Any]


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

    @abstractmethod
    async def log_publish(self, channel: str, message: str) -> None: ...

    @abstractmethod
    async def log_subscribe(self, channel: str) -> AsyncGenerator[str, None]: ...

    @abstractmethod
    async def acquire_lock(self, key: str, agent_id: UUID, ttl_seconds: int) -> bool: ...

    @abstractmethod
    async def release_lock(self, key: str) -> int: ...

    @abstractmethod
    async def index_add(self, key: str, mapping: dict[str, float]) -> int: ...

    @abstractmethod
    async def index_range(self, key: str, min_score: float, max_score: float) -> list[bytes]: ...

    @abstractmethod
    async def index_remove(self, key: str, *members: str) -> int: ...

    @abstractmethod
    async def clear(self) -> int: ...


class BaseQueueBackend(ABC):
    """Task queue with push/pop semantics."""

    @abstractmethod
    async def push(
        self,
        queue_name: str,
        value: str,
        *,
        high_priority: bool = False,
        partition_key: str | None = None,
    ) -> None: ...

    @abstractmethod
    async def pop(
        self,
        queue_name: str,
        *,
        timeout: int = 1,
    ) -> dict[str, Any] | None: ...


class BaseActivityBackend(ABC):
    """Task lifecycle tracking."""

    @abstractmethod
    async def create(
        self,
        agent_id: UUID,
        agent_type: str,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> None: ...

    @abstractmethod
    async def append_log(
        self,
        agent_id: UUID,
        message: str,
        status: str,
        percentage: int | None = None,
    ) -> None: ...

    @abstractmethod
    async def get(
        self,
        agent_id: UUID,
        metadata_filter: dict[str, Any] | None = None,
    ) -> Any: ...

    @abstractmethod
    async def list(
        self,
        page: int = 1,
        page_size: int = 50,
        metadata_filter: dict[str, Any] | None = None,
    ) -> tuple[list[Any], int]: ...

    @abstractmethod
    async def count_active(self) -> int: ...

    @abstractmethod
    async def get_pending_ids(self) -> list[UUID]: ...


class BaseBackend(ABC):
    """Top-level backend interface with namespaced sub-backends."""

    state: BaseStateBackend
    queue: BaseQueueBackend
    activity: BaseActivityBackend

    @abstractmethod
    def format_key(self, *args: str) -> str: ...

    @abstractmethod
    def configure(self, **kwargs: Any) -> None: ...

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
