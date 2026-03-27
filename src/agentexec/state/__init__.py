"""State management layer.

Initializes the configured backend and exposes it as a public reference.
All state operations go through ``backend.state``, ``backend.queue``, and
``backend.activity`` directly. No ops passthrough layer.

Pick one backend via AGENTEXEC_STATE_BACKEND:
  - 'agentexec.state.redis_backend'  (default)
  - 'agentexec.state.kafka_backend'
"""

from __future__ import annotations

from agentexec.config import CONF
from agentexec.state.base import BaseBackend

# ---------------------------------------------------------------------------
# Key constants — used by domain modules to build namespaced keys
# ---------------------------------------------------------------------------

KEY_RESULT = (CONF.key_prefix, "result")
KEY_EVENT = (CONF.key_prefix, "event")
KEY_LOCK = (CONF.key_prefix, "lock")
KEY_SCHEDULE = (CONF.key_prefix, "schedule")
KEY_SCHEDULE_QUEUE = (CONF.key_prefix, "schedule_queue")
CHANNEL_LOGS = (CONF.key_prefix, "logs")

# ---------------------------------------------------------------------------
# Backend instance — created once at import time
# ---------------------------------------------------------------------------

_BACKEND_CLASSES = {
    "agentexec.state.redis_backend": "agentexec.state.redis_backend.backend:RedisBackend",
    "agentexec.state.kafka_backend": "agentexec.state.kafka_backend.backend:KafkaBackend",
}


def _create_backend() -> BaseBackend:
    """Instantiate the configured backend class."""
    backend_path = _BACKEND_CLASSES.get(CONF.state_backend)
    if backend_path is None:
        raise ValueError(
            f"Unknown state backend: {CONF.state_backend}. "
            f"Valid options: {list(_BACKEND_CLASSES.keys())}"
        )

    module_path, class_name = backend_path.rsplit(":", 1)
    import importlib
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls()


backend: BaseBackend = _create_backend()
