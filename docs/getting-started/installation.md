# Installation

This guide covers installing agentexec and its dependencies on macOS and Ubuntu/Debian systems.

## Requirements

Before installing agentexec, ensure you have:

- **Python 3.11 or higher** - agentexec uses modern Python features
- **Redis 7.0 or higher** - For task queuing and coordination
- **A SQL database** - PostgreSQL (recommended), MySQL, or SQLite

## Installing uv

agentexec uses [uv](https://docs.astral.sh/uv/) for fast, reliable Python package management.

**macOS:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Ubuntu/Debian:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Verify installation:
```bash
uv --version
```

## Installing agentexec

Create a new project and add agentexec:

```bash
# Create a new project
uv init my-agent-project
cd my-agent-project

# Add agentexec
uv add agentexec
```

Or add to an existing project:

```bash
uv add agentexec
```

## Installing Dependencies

### Redis

agentexec requires Redis for task queuing, result storage, and worker coordination.

**macOS (Homebrew):**
```bash
brew install redis
brew services start redis
```

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install redis-server
sudo systemctl start redis
sudo systemctl enable redis
```

Verify Redis is running:
```bash
redis-cli ping
# Should return: PONG
```

### Database

agentexec uses SQLAlchemy and supports any SQLAlchemy-compatible database.

**PostgreSQL (recommended for production):**

```bash
# macOS
brew install postgresql@16
brew services start postgresql@16

# Ubuntu/Debian
sudo apt-get install postgresql postgresql-contrib
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Install Python driver
uv add psycopg2-binary
```

**SQLite (for development):**

SQLite is included with Python - no additional installation needed. Note that SQLite has limitations with concurrent writes, making it unsuitable for production multi-worker deployments.

### OpenAI Agents SDK

If you're using the OpenAI runner (most common use case):

```bash
uv add openai-agents
```

Set your OpenAI API key:
```bash
export OPENAI_API_KEY=sk-your-key-here
```

## Verifying Installation

Create a simple test script to verify everything is installed correctly:

```python
# test_install.py
import agentexec as ax
from sqlalchemy import create_engine

# Test imports
print(f"agentexec version: {ax.__version__}")

# Test database connection
engine = create_engine("sqlite:///test.db")
ax.Base.metadata.create_all(engine)
print("Database tables created successfully")

# Test configuration
print(f"Default queue name: {ax.CONF.queue_name}")
print(f"Default workers: {ax.CONF.num_workers}")

print("\nInstallation verified successfully!")
```

Run the test:
```bash
uv run python test_install.py
```

## Optional Dependencies

### For type checking

```bash
uv add --dev mypy
```

### For testing

```bash
uv add --dev pytest pytest-asyncio fakeredis
```

### For development

```bash
uv add --dev ruff  # Linting and formatting
```

## Environment Setup

Create a `.env` file in your project root with your configuration:

```bash
# .env
REDIS_URL=redis://localhost:6379/0
DATABASE_URL=postgresql://user:password@localhost/myapp

# Optional
AGENTEXEC_NUM_WORKERS=4
AGENTEXEC_QUEUE_NAME=myapp_tasks
OPENAI_API_KEY=sk-your-key-here
```

agentexec automatically loads environment variables from `.env` files using pydantic-settings.

## Troubleshooting

### Redis Connection Errors

If you see `ConnectionError: Error connecting to Redis`:

1. Verify Redis is running: `redis-cli ping`
2. Check your `REDIS_URL` is correct
3. Ensure Redis is accepting connections on the expected host/port

**macOS:**
```bash
brew services list  # Check if redis is running
brew services restart redis
```

**Ubuntu/Debian:**
```bash
sudo systemctl status redis
sudo systemctl restart redis
```

### Database Connection Errors

If you see SQLAlchemy connection errors:

1. Verify your database is running
2. Check your `DATABASE_URL` format matches your database type:
   - PostgreSQL: `postgresql://user:pass@host:port/dbname`
   - SQLite: `sqlite:///path/to/db.sqlite`
3. Ensure you have the appropriate database driver installed

### Import Errors

If you see `ModuleNotFoundError`:

1. Verify agentexec is installed: `uv pip show agentexec`
2. Check you're using Python 3.11+: `python --version`
3. Make sure you're running with `uv run`

## Next Steps

- [Quick Start](quickstart.md) - Create your first task and worker
- [Configuration](configuration.md) - Customize agentexec for your needs
