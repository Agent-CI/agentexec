# Configuration

agentexec uses environment variables for configuration, following the [12-factor app methodology](https://12factor.net/config). All settings can be configured via environment variables or a `.env` file.

## Configuration Loading

Configuration is loaded automatically when you import agentexec:

```python
import agentexec as ax

# Access configuration
print(ax.CONF.redis_url)
print(ax.CONF.num_workers)
```

The configuration system uses [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) and automatically:

1. Loads from environment variables
2. Falls back to `.env` file in the current directory
3. Uses default values where available

## Required Settings

These settings must be provided - there are no defaults:

| Variable | Description | Example |
|----------|-------------|---------|
| `REDIS_URL` or `AGENTEXEC_REDIS_URL` | Redis connection URL | `redis://localhost:6379/0` |

The `DATABASE_URL` is passed directly to `Pool`, not through configuration.

## Optional Settings

### Worker Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENTEXEC_NUM_WORKERS` | `4` | Number of worker processes to spawn |
| `AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT` | `300` | Seconds to wait for workers to finish on shutdown |

**Example:**
```bash
AGENTEXEC_NUM_WORKERS=8
AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT=60
```

### Queue Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENTEXEC_QUEUE_NAME` | `agentexec_tasks` | Name of the Redis list used for the task queue |

**Example:**
```bash
AGENTEXEC_QUEUE_NAME=myapp_production_tasks
```

### Database Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENTEXEC_TABLE_PREFIX` | `agentexec_` | Prefix for database table names |

Tables created will be named `{prefix}activity` and `{prefix}activity_log`.

**Example:**
```bash
AGENTEXEC_TABLE_PREFIX=myapp_
# Creates tables: myapp_activity, myapp_activity_log
```

### Redis Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENTEXEC_REDIS_POOL_SIZE` | `10` | Maximum connections in the Redis pool |
| `AGENTEXEC_REDIS_POOL_TIMEOUT` | `5` | Timeout in seconds waiting for a pool connection |
| `AGENTEXEC_RESULT_TTL` | `3600` | Time-to-live in seconds for cached task results |

**Example:**
```bash
AGENTEXEC_REDIS_POOL_SIZE=20
AGENTEXEC_REDIS_POOL_TIMEOUT=10
AGENTEXEC_RESULT_TTL=7200  # 2 hours
```

### Activity Messages

Customize the default messages logged during task lifecycle:

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENTEXEC_ACTIVITY_MESSAGE_CREATE` | `"Waiting to start."` | Message when task is created |
| `AGENTEXEC_ACTIVITY_MESSAGE_STARTED` | `"Task started."` | Message when task begins execution |
| `AGENTEXEC_ACTIVITY_MESSAGE_COMPLETE` | `"Task completed successfully."` | Message on successful completion |
| `AGENTEXEC_ACTIVITY_MESSAGE_ERROR` | `"Task failed with error: {error}"` | Message on error (supports `{error}` placeholder) |

**Example:**
```bash
AGENTEXEC_ACTIVITY_MESSAGE_CREATE="Queued for processing"
AGENTEXEC_ACTIVITY_MESSAGE_STARTED="Processing started"
AGENTEXEC_ACTIVITY_MESSAGE_COMPLETE="Done!"
AGENTEXEC_ACTIVITY_MESSAGE_ERROR="Failed: {error}"
```

### Docker Worker Configuration

When using the Docker worker image:

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENTEXEC_WORKER_MODULE` | - | Python module path containing worker pool definition |

**Example:**
```bash
AGENTEXEC_WORKER_MODULE=myapp.worker
```

## Environment File

Create a `.env` file in your project root:

```bash
# .env

# Required
REDIS_URL=redis://localhost:6379/0

# Database (passed to Pool)
DATABASE_URL=postgresql://user:password@localhost:5432/myapp

# OpenAI (for agents)
OPENAI_API_KEY=sk-your-key-here

# Worker configuration
AGENTEXEC_NUM_WORKERS=4
AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT=300

# Queue configuration
AGENTEXEC_QUEUE_NAME=myapp_tasks

# Database tables
AGENTEXEC_TABLE_PREFIX=myapp_

# Redis tuning
AGENTEXEC_REDIS_POOL_SIZE=10
AGENTEXEC_RESULT_TTL=3600
```

## Configuration Patterns

### Development vs Production

Use different `.env` files for different environments:

```bash
# .env.development
REDIS_URL=redis://localhost:6379/0
DATABASE_URL=sqlite:///dev.db
AGENTEXEC_NUM_WORKERS=2

# .env.production
REDIS_URL=redis://redis-cluster:6379/0
DATABASE_URL=postgresql://user:pass@db-host/prod
AGENTEXEC_NUM_WORKERS=16
AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT=600
```

Load the appropriate file:
```bash
# Development
cp .env.development .env

# Production
cp .env.production .env
```

Or use environment-specific loading in your code:
```python
import os
from dotenv import load_dotenv

env = os.getenv("ENVIRONMENT", "development")
load_dotenv(f".env.{env}")
```

### Multiple Queues

Run different worker pools for different task types:

```bash
# High-priority queue
AGENTEXEC_QUEUE_NAME=myapp_high_priority
AGENTEXEC_NUM_WORKERS=8

# Low-priority queue (separate process)
AGENTEXEC_QUEUE_NAME=myapp_low_priority
AGENTEXEC_NUM_WORKERS=2
```

### Multiple Applications

Use different table prefixes to share a database:

```bash
# App 1
AGENTEXEC_TABLE_PREFIX=app1_

# App 2
AGENTEXEC_TABLE_PREFIX=app2_
```

## Accessing Configuration Programmatically

```python
import agentexec as ax

# Read configuration values
config = ax.CONF

print(f"Redis URL: {config.redis_url}")
print(f"Workers: {config.num_workers}")
print(f"Queue: {config.queue_name}")
print(f"Table prefix: {config.table_prefix}")
print(f"Result TTL: {config.result_ttl}s")
print(f"Shutdown timeout: {config.graceful_shutdown_timeout}s")
```

## Validation

Configuration is validated at import time using Pydantic. Invalid configuration will raise a `ValidationError`:

```python
# With invalid REDIS_URL
# ValidationError: 1 validation error for Config
#   redis_url
#     field required (type=value_error.missing)
```

## Redis URL Format

The Redis URL follows the standard format:

```
redis://[[username:]password@]host[:port][/database]
```

**Examples:**
```bash
# Local Redis, default port, database 0
REDIS_URL=redis://localhost:6379/0

# With password
REDIS_URL=redis://:mypassword@localhost:6379/0

# With username and password (Redis 6+)
REDIS_URL=redis://myuser:mypassword@localhost:6379/0

# Redis Sentinel
REDIS_URL=redis+sentinel://localhost:26379/mymaster/0

# Redis Cluster
REDIS_URL=redis://node1:6379,node2:6379,node3:6379/0

# TLS connection
REDIS_URL=rediss://localhost:6379/0
```

## Database URL Format

The `DATABASE_URL` passed to `Pool` follows SQLAlchemy conventions:

```bash
# PostgreSQL
DATABASE_URL=postgresql://user:password@host:5432/dbname

# PostgreSQL with SSL
DATABASE_URL=postgresql://user:password@host:5432/dbname?sslmode=require

# MySQL
DATABASE_URL=mysql://user:password@host:3306/dbname

# SQLite
DATABASE_URL=sqlite:///path/to/database.db

# SQLite in memory (not recommended for workers)
DATABASE_URL=sqlite:///:memory:
```

## Next Steps

- [Architecture](../concepts/architecture.md) - Understand how configuration affects system behavior
- [Worker Pool](../concepts/worker-pool.md) - Learn about worker configuration
- [Production Guide](../deployment/production.md) - Production configuration recommendations
