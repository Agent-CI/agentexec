# Installation

This guide covers installing agentexec and its dependencies on macOS and Ubuntu/Debian systems.

## Requirements

Before installing agentexec, ensure you have:

- **Python 3.11 or higher** - agentexec uses modern Python features
- **Redis 7.0 or higher** - For task queuing and coordination
- **A SQL database** - PostgreSQL (recommended), MySQL, or SQLite

## Installing agentexec

Create a new project and add agentexec:

```bash
uv add agentexec
```

## Environment Setup

Create a `.env` file in your project root with your configuration:

```bash
REDIS_URL=redis://localhost:6379/0
DATABASE_URL=postgresql://user:password@localhost/myapp

AGENTEXEC_NUM_WORKERS=4
AGENTEXEC_QUEUE_NAME=myapp_tasks
```

agentexec automatically loads environment variables from `.env` files using pydantic-settings.

