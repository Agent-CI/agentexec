"""Kafka activity operations — compacted topic + in-memory cache.

Activity records are produced to a compacted topic keyed by agent_id.
Each update appends to the log history and re-produces the full record.
Pre-compaction, all intermediate states are visible; post-compaction,
only the final state per agent_id survives.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from agentexec.state.kafka_backend.connection import (
    _cache_lock,
    activity_topic,
    produce,
)

# In-memory cache for activity records
_activity_cache: dict[str, dict[str, Any]] = {}


def _now_iso() -> str:
    """Current UTC time as ISO string."""
    return datetime.now(UTC).isoformat()


async def _activity_produce(record: dict[str, Any]) -> None:
    """Persist an activity record to the compacted activity topic."""
    agent_id = record["agent_id"]
    data = json.dumps(record, default=str).encode("utf-8")
    await produce(activity_topic(), data, key=str(agent_id))


async def activity_create(
    agent_id: uuid.UUID,
    agent_type: str,
    message: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Create a new activity record with initial QUEUED log entry."""
    now = _now_iso()
    log_entry = {
        "id": str(uuid.uuid4()),
        "message": message,
        "status": "queued",
        "percentage": 0,
        "created_at": now,
    }
    record: dict[str, Any] = {
        "agent_id": str(agent_id),
        "agent_type": agent_type,
        "created_at": now,
        "updated_at": now,
        "metadata": metadata,
        "logs": [log_entry],
    }
    with _cache_lock:
        _activity_cache[str(agent_id)] = record
    await _activity_produce(record)


async def activity_append_log(
    agent_id: uuid.UUID,
    message: str,
    status: str,
    percentage: int | None = None,
) -> None:
    """Append a log entry to an existing activity record."""
    key = str(agent_id)
    now = _now_iso()
    log_entry = {
        "id": str(uuid.uuid4()),
        "message": message,
        "status": status,
        "percentage": percentage,
        "created_at": now,
    }
    with _cache_lock:
        record = _activity_cache.get(key)
        if record is None:
            raise ValueError(f"Activity not found for agent_id {agent_id}")
        record["logs"].append(log_entry)
        record["updated_at"] = now
    await _activity_produce(record)


async def activity_get(
    agent_id: uuid.UUID,
    metadata_filter: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Get a single activity record by agent_id."""
    key = str(agent_id)
    with _cache_lock:
        record = _activity_cache.get(key)
    if record is None:
        return None
    if metadata_filter and record.get("metadata"):
        for k, v in metadata_filter.items():
            if str(record["metadata"].get(k)) != str(v):
                return None
    elif metadata_filter:
        return None
    return record


async def activity_list(
    page: int = 1,
    page_size: int = 50,
    metadata_filter: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """List activity records with pagination.

    Returns (items, total) where items are summary dicts matching
    ActivityListItemSchema fields.
    """
    with _cache_lock:
        records = list(_activity_cache.values())

    # Apply metadata filter
    if metadata_filter:
        records = [
            r for r in records
            if r.get("metadata")
            and all(
                str(r["metadata"].get(k)) == str(v)
                for k, v in metadata_filter.items()
            )
        ]

    # Build summary items
    items: list[dict[str, Any]] = []
    for r in records:
        logs = r.get("logs", [])
        latest = logs[-1] if logs else None
        first = logs[0] if logs else None
        items.append({
            "agent_id": r["agent_id"],
            "agent_type": r.get("agent_type"),
            "status": latest["status"] if latest else "queued",
            "latest_log_message": latest["message"] if latest else None,
            "latest_log_timestamp": latest["created_at"] if latest else None,
            "percentage": latest.get("percentage", 0) if latest else 0,
            "started_at": first["created_at"] if first else None,
            "metadata": r.get("metadata"),
        })

    # Sort: active (running/queued) first, then by started_at descending
    items.sort(key=lambda x: x.get("started_at") or "", reverse=True)
    items.sort(key=lambda x: (
        0 if x["status"] in ("running", "queued") else 1,
        {"running": 1, "queued": 2}.get(x["status"], 3),
    ))

    total = len(items)
    offset = (page - 1) * page_size
    return items[offset:offset + page_size], total


async def activity_count_active() -> int:
    """Count activities with QUEUED or RUNNING status."""
    count = 0
    with _cache_lock:
        for record in _activity_cache.values():
            logs = record.get("logs", [])
            if logs and logs[-1]["status"] in ("queued", "running"):
                count += 1
    return count


async def activity_get_pending_ids() -> list[uuid.UUID]:
    """Get agent_ids for all activities with QUEUED or RUNNING status."""
    pending: list[uuid.UUID] = []
    with _cache_lock:
        for record in _activity_cache.values():
            logs = record.get("logs", [])
            if logs and logs[-1]["status"] in ("queued", "running"):
                pending.append(uuid.UUID(record["agent_id"]))
    return pending
