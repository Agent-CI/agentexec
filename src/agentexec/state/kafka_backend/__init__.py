"""Kafka backend — replaces both Redis and Postgres with Kafka.

- Queue: Kafka topics with consumer groups and partition-based ordering.
- State: Compacted topics with in-memory caches.
- Activity: Compacted activity topic as the permanent task lifecycle record.
- Locks: No-op — Kafka's partition assignment handles isolation.
"""

from agentexec.state.kafka_backend.connection import close, configure
from agentexec.state.kafka_backend.state import (
    store_get,
    store_set,
    store_delete,
    counter_incr,
    counter_decr,
    log_publish,
    log_subscribe,
    acquire_lock,
    release_lock,
    index_add,
    index_range,
    index_remove,
    serialize,
    deserialize,
    format_key,
    clear_keys,
)
from agentexec.state.kafka_backend.queue import (
    queue_push,
    queue_pop,
)
from agentexec.state.kafka_backend.activity import (
    activity_create,
    activity_append_log,
    activity_get,
    activity_list,
    activity_count_active,
    activity_get_pending_ids,
)

__all__ = [
    # Connection
    "close",
    "configure",
    # State
    "store_get",
    "store_set",
    "store_delete",
    "counter_incr",
    "counter_decr",
    "log_publish",
    "log_subscribe",
    "acquire_lock",
    "release_lock",
    "index_add",
    "index_range",
    "index_remove",
    "serialize",
    "deserialize",
    "format_key",
    "clear_keys",
    # Queue
    "queue_push",
    "queue_pop",
    # Activity
    "activity_create",
    "activity_append_log",
    "activity_get",
    "activity_list",
    "activity_count_active",
    "activity_get_pending_ids",
]
