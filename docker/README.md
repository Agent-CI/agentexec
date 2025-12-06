# agentexec-worker Docker Image

Base Docker image for deploying agentexec workers.

## Quick Start

### 1. Create your worker module

```python
# src/worker.py
import os
import agentexec as ax

pool = ax.Pool(database_url=os.environ["DATABASE_URL"])

@pool.task("my_task")
async def my_task(agent_id, context):
    # Your task implementation
    pass
```

### 2. Create your Dockerfile

```dockerfile
FROM ghcr.io/agent-ci/agentexec-worker:latest

# Copy your application code
COPY ./src /app/src

# Point to your worker module
ENV AGENTEXEC_WORKER_MODULE=src.worker
```

### 3. Build and run

```bash
# Build your image
docker build -t my-worker .

# Run with required environment variables
docker run \
  -e DATABASE_URL=postgresql://user:pass@host:5432/db \
  -e REDIS_URL=redis://host:6379 \
  -e OPENAI_API_KEY=sk-... \
  my-worker
```

## Environment Variables

### Required

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | Database connection URL |
| `REDIS_URL` | Redis connection URL |
| `AGENTEXEC_WORKER_MODULE` | Python module path containing `pool` |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | - | Required if using OpenAI agents |
| `AGENTEXEC_NUM_WORKERS` | 4 | Number of worker processes |
| `AGENTEXEC_QUEUE_NAME` | agentexec_tasks | Redis queue name |
| `AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT` | 300 | Shutdown timeout (seconds) |
| `AGENTEXEC_RESULT_TTL` | 3600 | Task result TTL (seconds) |

## Worker Module Requirements

Your worker module must expose either:

1. A `pool` variable (recommended):
   ```python
   pool = ax.Pool(database_url=os.environ["DATABASE_URL"])
   ```

2. Or a `create_pool()` function:
   ```python
   def create_pool():
       return ax.Pool(database_url=os.environ["DATABASE_URL"])
   ```

## Docker Compose Example

```yaml
version: '3.8'

services:
  worker:
    build: .
    environment:
      - DATABASE_URL=postgresql://postgres:postgres@db:5432/app
      - REDIS_URL=redis://redis:6379
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - AGENTEXEC_NUM_WORKERS=2
    depends_on:
      - db
      - redis

  db:
    image: postgres:15
    environment:
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: app

  redis:
    image: redis:7-alpine
```

## Extending the Image

### Adding Python dependencies

```dockerfile
FROM ghcr.io/agent-ci/agentexec-worker:latest

# Install additional packages
RUN pip install --no-cache-dir \
    httpx \
    beautifulsoup4

COPY ./src /app/src
ENV AGENTEXEC_WORKER_MODULE=src.worker
```

### Using a different database

The base image includes `psycopg2-binary` for PostgreSQL. For other databases:

```dockerfile
FROM ghcr.io/agent-ci/agentexec-worker:latest

# For MySQL
RUN pip install --no-cache-dir mysqlclient

# For SQLite (included in Python stdlib, but you may want async driver)
RUN pip install --no-cache-dir aiosqlite
```

## Building Locally

```bash
cd docker
docker build -t agentexec-worker .
```

## Multi-Architecture Support

The published image supports both `linux/amd64` and `linux/arm64` architectures.
