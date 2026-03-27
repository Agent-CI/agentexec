# cspell:ignore rpush lpush brpop RPUSH LPUSH BRPOP
"""Redis backend — uses Redis for state/queue and Postgres for activity."""

from agentexec.state.redis_backend.connection import close
from agentexec.state.redis_backend.state import (
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
from agentexec.state.redis_backend.queue import (
    queue_push,
    queue_pop,
    queue_commit,
    queue_nack,
)
from agentexec.state.redis_backend.activity import (
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
    "queue_commit",
    "queue_nack",
    # Activity
    "activity_create",
    "activity_append_log",
    "activity_get",
    "activity_list",
    "activity_count_active",
    "activity_get_pending_ids",
]
