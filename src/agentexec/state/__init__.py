"""State management layer.

Initializes the configured backend and exposes it as a public reference.
All state operations go through ``backend.state``, ``backend.queue``,
and ``backend.schedule`` directly. Activity uses Postgres directly.

Pick one backend via AGENTEXEC_STATE_BACKEND:
  - 'agentexec.state.redis_backend'  (default)
  - 'agentexec.state.kafka_backend'
"""

from __future__ import annotations

import importlib

from agentexec.config import CONF
from agentexec.state.base import BaseBackend

KEY_RESULT = (CONF.key_prefix, "result")
KEY_EVENT = (CONF.key_prefix, "event")


def _create_backend(state_backend: str) -> BaseBackend:
    """Instantiate the given backend class.

    The state_backend string is a fully qualified module path containing
    a Backend class (e.g. 'agentexec.state.kafka').
    """
    try:
        module = importlib.import_module(state_backend)
        return module.Backend()
    except ImportError as e:
        raise ImportError(f"Could not import backend {state_backend}: {e}")
    except AttributeError:
        raise ValueError(f"Backend module {state_backend} has no Backend class")


backend: BaseBackend = _create_backend(CONF.state_backend)
