# Docker Deployment

This guide covers deploying agentexec applications using Docker.

## Overview

A typical agentexec deployment consists of:

1. **API Service** - FastAPI or other web framework
2. **Worker Service** - One or more worker processes
3. **Redis** - Task queue and coordination
4. **Database** - PostgreSQL or other SQL database

```
┌─────────────────────────────────────────────────────────────┐
│                        Load Balancer                        │
└─────────────────────────────┬───────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        ┌─────────┐     ┌─────────┐     ┌─────────┐
        │  API 1  │     │  API 2  │     │  API 3  │
        └────┬────┘     └────┬────┘     └────┬────┘
             │               │               │
             └───────────────┼───────────────┘
                             │
                    ┌────────┴────────┐
                    ▼                 ▼
              ┌──────────┐      ┌──────────┐
              │  Redis   │      │ Postgres │
              └────┬─────┘      └──────────┘
                   │
       ┌───────────┼───────────┐
       ▼           ▼           ▼
  ┌─────────┐ ┌─────────┐ ┌─────────┐
  │Worker 1 │ │Worker 2 │ │Worker 3 │
  └─────────┘ └─────────┘ └─────────┘
```

## Application Dockerfile

Create a Dockerfile for your application:

```dockerfile
# Dockerfile
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-dev

# Copy application code
COPY src/ ./src/

# Set Python path
ENV PYTHONPATH=/app/src
ENV PATH="/app/.venv/bin:$PATH"

# Default command (override in docker-compose)
CMD ["python", "-m", "myapp.worker"]
```

## Docker Compose

### Development Setup

```yaml
# docker-compose.yml
version: '3.8'

services:
  api:
    build: .
    command: uvicorn myapp.main:app --host 0.0.0.0 --port 8000 --reload
    ports:
      - "8000:8000"
    volumes:
      - ./src:/app/src  # Hot reload
    environment:
      - DATABASE_URL=postgresql://postgres:postgres@db:5432/agentexec
      - REDIS_URL=redis://redis:6379/0
      - OPENAI_API_KEY=${OPENAI_API_KEY}
    depends_on:
      db:
        condition: service_healthy
      redis:
        condition: service_started

  worker:
    build: .
    command: python -m myapp.worker
    volumes:
      - ./src:/app/src
    environment:
      - DATABASE_URL=postgresql://postgres:postgres@db:5432/agentexec
      - REDIS_URL=redis://redis:6379/0
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - AGENTEXEC_NUM_WORKERS=2
    depends_on:
      db:
        condition: service_healthy
      redis:
        condition: service_started

  db:
    image: postgres:16-alpine
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
      - POSTGRES_DB=agentexec
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data

volumes:
  postgres_data:
  redis_data:
```

### Production Setup

```yaml
# docker-compose.prod.yml
version: '3.8'

services:
  api:
    image: myregistry/myapp:${VERSION:-latest}
    command: uvicorn myapp.main:app --host 0.0.0.0 --port 8000 --workers 4
    deploy:
      replicas: 3
      resources:
        limits:
          cpus: '1'
          memory: 1G
    environment:
      - DATABASE_URL=${DATABASE_URL}
      - REDIS_URL=${REDIS_URL}
      - OPENAI_API_KEY=${OPENAI_API_KEY}
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  worker:
    image: myregistry/myapp:${VERSION:-latest}
    command: python -m myapp.worker
    deploy:
      replicas: 2
      resources:
        limits:
          cpus: '2'
          memory: 2G
    environment:
      - DATABASE_URL=${DATABASE_URL}
      - REDIS_URL=${REDIS_URL}
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - AGENTEXEC_NUM_WORKERS=4
      - AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT=120
    stop_grace_period: 2m  # Allow time for graceful shutdown
```

## Using the Official Worker Image

agentexec provides an official Docker image for workers:

```yaml
# docker-compose.yml
services:
  worker:
    image: ghcr.io/agent-ci/agentexec-worker:latest
    environment:
      - DATABASE_URL=postgresql://postgres:postgres@db:5432/agentexec
      - REDIS_URL=redis://redis:6379/0
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - AGENTEXEC_WORKER_MODULE=myapp.worker
      - AGENTEXEC_NUM_WORKERS=4
    volumes:
      - ./src:/app/src  # Mount your application code
```

The `AGENTEXEC_WORKER_MODULE` environment variable specifies the Python module containing your worker pool definition.

## Multi-Stage Build

For smaller production images:

```dockerfile
# Dockerfile.prod
# Build stage
FROM python:3.11-slim as builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Runtime stage
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY src/ ./src/

ENV PYTHONPATH=/app/src
ENV PATH="/app/.venv/bin:$PATH"

# Run as non-root user
RUN useradd -m -u 1000 appuser
USER appuser

CMD ["python", "-m", "myapp.worker"]
```

## Environment Variables

### Required Variables

```bash
# Database connection
DATABASE_URL=postgresql://user:password@host:5432/dbname

# Redis connection
REDIS_URL=redis://host:6379/0

# OpenAI API key (if using OpenAI runner)
OPENAI_API_KEY=sk-...
```

### Optional Variables

```bash
# Worker configuration
AGENTEXEC_NUM_WORKERS=4
AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT=300
AGENTEXEC_QUEUE_NAME=myapp_tasks

# Redis tuning
AGENTEXEC_REDIS_POOL_SIZE=10
AGENTEXEC_RESULT_TTL=3600

# Database tables
AGENTEXEC_TABLE_PREFIX=myapp_
```

### Using .env Files

```yaml
# docker-compose.yml
services:
  api:
    env_file:
      - .env
      - .env.local  # Override for local development
```

## Health Checks

### API Health Check

```python
# myapp/main.py
from fastapi import FastAPI
from agentexec.core.redis_client import get_redis

app = FastAPI()

@app.get("/health")
async def health_check():
    # Check Redis
    try:
        redis = await get_redis()
        await redis.ping()
    except Exception as e:
        return {"status": "unhealthy", "redis": str(e)}

    # Check database
    try:
        with Session(engine) as session:
            session.execute("SELECT 1")
    except Exception as e:
        return {"status": "unhealthy", "database": str(e)}

    return {"status": "healthy"}
```

### Worker Health Check

Workers don't expose HTTP endpoints by default. Use Redis for health monitoring:

```python
# In your worker
import time
from agentexec.core.redis_client import get_redis_sync

def report_health(worker_id: str):
    redis = get_redis_sync()
    redis.setex(f"worker:{worker_id}:heartbeat", 60, str(time.time()))
```

## Logging

### Configure Logging

```python
# myapp/logging.py
import logging
import sys

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

# Call in worker and API entry points
setup_logging()
```

### Docker Logging Drivers

```yaml
# docker-compose.yml
services:
  worker:
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
```

## Scaling

### Horizontal Scaling

Scale workers based on queue depth:

```bash
# Scale to 5 worker containers
docker compose up -d --scale worker=5
```

### Auto-scaling with Kubernetes

```yaml
# kubernetes/worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: agentexec-worker
spec:
  replicas: 2
  selector:
    matchLabels:
      app: agentexec-worker
  template:
    metadata:
      labels:
        app: agentexec-worker
    spec:
      containers:
      - name: worker
        image: myregistry/myapp:latest
        command: ["python", "-m", "myapp.worker"]
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: agentexec-secrets
              key: database-url
        - name: REDIS_URL
          valueFrom:
            secretKeyRef:
              name: agentexec-secrets
              key: redis-url
        - name: AGENTEXEC_NUM_WORKERS
          value: "4"
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2"
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: agentexec-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: agentexec-worker
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: External
    external:
      metric:
        name: redis_queue_length
        selector:
          matchLabels:
            queue: agentexec_tasks
      target:
        type: AverageValue
        averageValue: "100"
```

## Graceful Shutdown

Ensure workers complete current tasks before stopping:

```yaml
# docker-compose.yml
services:
  worker:
    stop_grace_period: 5m  # Wait up to 5 minutes
    environment:
      - AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT=300
```

Workers handle SIGTERM by:
1. Stop accepting new tasks
2. Complete current task
3. Clean up connections
4. Exit

## Database Migrations

agentexec recommends using **Alembic** for database migrations in production. This ensures your database schema is version-controlled and migrations run before services start. See the [Basic Usage Guide](../guides/basic-usage.md#database-setup) for Alembic configuration with agentexec.

Run migrations before starting services:

```yaml
# docker-compose.yml
services:
  migrate:
    build: .
    command: alembic upgrade head
    environment:
      - DATABASE_URL=${DATABASE_URL}
    depends_on:
      db:
        condition: service_healthy

  api:
    depends_on:
      migrate:
        condition: service_completed_successfully
```

Or use an init container in Kubernetes:

```yaml
spec:
  initContainers:
  - name: migrate
    image: myregistry/myapp:latest
    command: ["alembic", "upgrade", "head"]
    env:
    - name: DATABASE_URL
      valueFrom:
        secretKeyRef:
          name: agentexec-secrets
          key: database-url
```

## Security

### Non-root User

```dockerfile
# Run as non-root
RUN useradd -m -u 1000 appuser
USER appuser
```

### Read-only Filesystem

```yaml
services:
  worker:
    read_only: true
    tmpfs:
      - /tmp
```

### Network Isolation

```yaml
services:
  api:
    networks:
      - frontend
      - backend

  worker:
    networks:
      - backend

  db:
    networks:
      - backend

networks:
  frontend:
  backend:
    internal: true
```

## Next Steps

- [Production Guide](production.md) - Production best practices
- [Configuration](../getting-started/configuration.md) - Environment variables
- [Architecture](../concepts/architecture.md) - System design
