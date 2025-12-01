# Production Guide

This guide covers best practices for deploying agentexec in production environments.

## Infrastructure Requirements

### Minimum Requirements

| Component | Specification |
|-----------|--------------|
| Python | 3.11+ |
| Redis | 7.0+ with persistence |
| Database | PostgreSQL 14+ (recommended) |
| Memory | 512MB per worker process |
| CPU | 0.5 cores per worker (I/O bound) |

### Recommended Production Setup

| Component | Specification |
|-----------|--------------|
| API Servers | 2+ instances behind load balancer |
| Worker Containers | 2+ containers, 4 workers each |
| Redis | Redis Cluster or Sentinel for HA |
| Database | PostgreSQL with read replicas |

## Database Configuration

### PostgreSQL Setup

```sql
-- Create database
CREATE DATABASE agentexec;

-- Create user with limited privileges
CREATE USER agentexec_app WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE agentexec TO agentexec_app;
GRANT USAGE ON SCHEMA public TO agentexec_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO agentexec_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO agentexec_app;

-- For migrations (separate user)
CREATE USER agentexec_migrate WITH PASSWORD 'migrate_password';
GRANT ALL PRIVILEGES ON DATABASE agentexec TO agentexec_migrate;
```

### Connection Pooling

Use PgBouncer or built-in SQLAlchemy pooling:

```python
from sqlalchemy import create_engine

engine = create_engine(
    DATABASE_URL,
    pool_size=10,           # Base connections
    max_overflow=20,        # Additional connections under load
    pool_timeout=30,        # Seconds to wait for connection
    pool_recycle=1800,      # Recycle connections every 30 min
    pool_pre_ping=True,     # Test connections before use
)
```

### Database Indexes

Ensure indexes exist for common queries:

```sql
-- These are created automatically, but verify:
CREATE INDEX IF NOT EXISTS ix_agentexec_activity_agent_id
    ON agentexec_activity(agent_id);

CREATE INDEX IF NOT EXISTS ix_agentexec_activity_log_activity_id
    ON agentexec_activity_log(activity_id);

-- Optional: Index for status queries
CREATE INDEX IF NOT EXISTS ix_agentexec_activity_log_status
    ON agentexec_activity_log(status);

-- Optional: Index for time-based queries
CREATE INDEX IF NOT EXISTS ix_agentexec_activity_updated_at
    ON agentexec_activity(updated_at DESC);
```

## Redis Configuration

### Production Redis Settings

```conf
# redis.conf
maxmemory 2gb
maxmemory-policy allkeys-lru

# Persistence
appendonly yes
appendfsync everysec

# Security
requirepass your_secure_password

# Performance
tcp-keepalive 300
timeout 0
```

### Redis Sentinel for High Availability

```yaml
# docker-compose.yml
services:
  redis-master:
    image: redis:7-alpine
    command: redis-server --requirepass ${REDIS_PASSWORD}

  redis-slave:
    image: redis:7-alpine
    command: redis-server --replicaof redis-master 6379 --masterauth ${REDIS_PASSWORD} --requirepass ${REDIS_PASSWORD}

  redis-sentinel:
    image: redis:7-alpine
    command: redis-sentinel /etc/redis/sentinel.conf
    volumes:
      - ./sentinel.conf:/etc/redis/sentinel.conf
```

Connect using Sentinel URL:

```bash
REDIS_URL=redis+sentinel://:password@sentinel1:26379,sentinel2:26379/mymaster/0
```

## Worker Configuration

### Optimal Worker Count

```bash
# For I/O-bound tasks (most LLM calls)
# Can exceed CPU count since workers spend time waiting
AGENTEXEC_NUM_WORKERS=8  # On 4-core machine

# For CPU-bound tasks
# Match CPU count
AGENTEXEC_NUM_WORKERS=4  # On 4-core machine
```

### Graceful Shutdown

Allow adequate time for tasks to complete:

```bash
# Allow 5 minutes for graceful shutdown
AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT=300
```

### Resource Limits

Set memory limits per container:

```yaml
# docker-compose.yml
services:
  worker:
    deploy:
      resources:
        limits:
          memory: 2G
        reservations:
          memory: 512M
```

## Application Configuration

### Environment Variables

```bash
# Production .env
# Database
DATABASE_URL=postgresql://user:pass@db-host:5432/agentexec?sslmode=require

# Redis
REDIS_URL=redis://:password@redis-host:6379/0

# Worker settings
AGENTEXEC_NUM_WORKERS=4
AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT=300
AGENTEXEC_QUEUE_NAME=production_tasks

# Redis tuning
AGENTEXEC_REDIS_POOL_SIZE=20
AGENTEXEC_REDIS_POOL_TIMEOUT=10
AGENTEXEC_RESULT_TTL=3600

# Table prefix (for multi-tenant)
AGENTEXEC_TABLE_PREFIX=prod_

# OpenAI
OPENAI_API_KEY=sk-...
```

### Secrets Management

Never commit secrets to version control. Use:

- **Environment variables** from CI/CD
- **AWS Secrets Manager** / **GCP Secret Manager**
- **HashiCorp Vault**
- **Kubernetes Secrets**

```python
# Example: AWS Secrets Manager
import boto3
import json

def get_secrets():
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId='agentexec/production')
    return json.loads(response['SecretString'])
```

## Monitoring

### Application Metrics

Track key metrics:

```python
# metrics.py
from prometheus_client import Counter, Histogram, Gauge

# Task metrics
tasks_queued = Counter('agentexec_tasks_queued_total', 'Tasks queued', ['task_name'])
tasks_completed = Counter('agentexec_tasks_completed_total', 'Tasks completed', ['task_name', 'status'])
task_duration = Histogram('agentexec_task_duration_seconds', 'Task duration', ['task_name'])

# Queue metrics
queue_depth = Gauge('agentexec_queue_depth', 'Queue depth')

# Worker metrics
active_workers = Gauge('agentexec_active_workers', 'Active workers')
```

### Health Endpoints

```python
# health.py
from fastapi import FastAPI, Response
from sqlalchemy.orm import Session
from agentexec.core.redis_client import get_redis

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/health/ready")
async def readiness():
    errors = []

    # Check Redis
    try:
        redis = await get_redis()
        await redis.ping()
    except Exception as e:
        errors.append(f"redis: {e}")

    # Check Database
    try:
        with Session(engine) as session:
            session.execute("SELECT 1")
    except Exception as e:
        errors.append(f"database: {e}")

    if errors:
        return Response(
            content=json.dumps({"status": "not ready", "errors": errors}),
            status_code=503,
            media_type="application/json"
        )

    return {"status": "ready"}

@app.get("/health/live")
async def liveness():
    return {"status": "alive"}
```

### Logging

Use structured logging:

```python
import logging
import json
from datetime import datetime

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

# Configure logging
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)
```

### Alerting

Set up alerts for:

| Metric | Threshold | Action |
|--------|-----------|--------|
| Queue depth | > 1000 | Scale workers |
| Error rate | > 5% | Investigate |
| Task duration | > 5 min avg | Check for issues |
| Worker count | < 2 | Auto-restart |
| Redis memory | > 80% | Increase memory |

## Error Handling

### Startup Cleanup

Clean up stale activities on startup:

```python
# In your startup code
from sqlalchemy.orm import Session
import agentexec as ax

def startup():
    with Session(engine) as session:
        canceled = ax.activity.cancel_pending(session)
        if canceled > 0:
            logger.info(f"Cleaned up {canceled} stale activities")
```

### Dead Letter Queue

Handle tasks that consistently fail:

```python
@pool.task("process_with_dlq")
async def process_with_dlq(agent_id: UUID, context: MyContext):
    try:
        return await do_work(context)
    except Exception as e:
        # Move to dead letter queue after 3 failures
        if context.retry_count >= 3:
            await move_to_dlq(agent_id, context, str(e))
            ax.activity.error(agent_id, f"Moved to DLQ: {e}")
            return

        # Retry
        context.retry_count += 1
        await ax.enqueue("process_with_dlq", context)
        raise
```

### Circuit Breaker

Protect against cascade failures:

```python
from circuitbreaker import circuit

@circuit(failure_threshold=5, recovery_timeout=60)
async def call_external_api(data):
    async with httpx.AsyncClient() as client:
        response = await client.post(API_URL, json=data)
        response.raise_for_status()
        return response.json()

@pool.task("api_task")
async def api_task(agent_id: UUID, context: APIContext):
    try:
        result = await call_external_api(context.data)
        return result
    except CircuitBreakerError:
        ax.activity.error(agent_id, "Circuit breaker open - service unavailable")
        raise
```

## Security

### Network Security

```yaml
# Kubernetes NetworkPolicy
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: agentexec-worker
spec:
  podSelector:
    matchLabels:
      app: agentexec-worker
  policyTypes:
  - Ingress
  - Egress
  ingress: []  # No incoming traffic needed
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: redis
    - podSelector:
        matchLabels:
          app: postgres
  - to:  # Allow OpenAI API
    - ipBlock:
        cidr: 0.0.0.0/0
    ports:
    - protocol: TCP
      port: 443
```

### API Key Rotation

Rotate OpenAI API keys periodically:

```python
import os
from datetime import datetime

def get_openai_key():
    # Check for rotated key
    rotated_key = os.getenv("OPENAI_API_KEY_NEW")
    rotation_date = os.getenv("OPENAI_KEY_ROTATION_DATE")

    if rotated_key and rotation_date:
        if datetime.utcnow() >= datetime.fromisoformat(rotation_date):
            return rotated_key

    return os.getenv("OPENAI_API_KEY")
```

### Rate Limiting

Protect your API:

```python
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)

@app.post("/api/tasks")
@limiter.limit("100/minute")
async def create_task(request: Request, ...):
    ...
```

## Backup and Recovery

### Database Backup

```bash
# Daily backup
pg_dump -h localhost -U agentexec_app agentexec | gzip > backup_$(date +%Y%m%d).sql.gz

# Restore
gunzip -c backup_20240115.sql.gz | psql -h localhost -U agentexec_app agentexec
```

### Redis Persistence

Ensure AOF is enabled:

```conf
# redis.conf
appendonly yes
appendfsync everysec
```

### Disaster Recovery

1. **Database**: Use PostgreSQL streaming replication
2. **Redis**: Use Redis Sentinel or Cluster
3. **Application**: Multi-region deployment

## Performance Tuning

### Database Query Optimization

```python
# Use eager loading for related data
from sqlalchemy.orm import joinedload

activity = session.query(Activity).options(
    joinedload(Activity.logs)
).filter(Activity.agent_id == agent_id).first()
```

### Redis Pipeline

```python
# Batch Redis operations
async def get_multiple_results(agent_ids: list[str]):
    redis = await get_redis()
    pipe = redis.pipeline()
    for agent_id in agent_ids:
        pipe.get(f"result:{agent_id}")
    return await pipe.execute()
```

### Connection Management

```python
# Reuse connections with context manager
from contextlib import asynccontextmanager

@asynccontextmanager
async def get_session():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
```

## Maintenance

### Database Cleanup

Archive old activities:

```python
from datetime import datetime, timedelta

def cleanup_old_activities(days: int = 30):
    cutoff = datetime.utcnow() - timedelta(days=days)

    with Session(engine) as session:
        # Archive to separate table or delete
        old_activities = session.query(Activity).filter(
            Activity.updated_at < cutoff
        ).all()

        for activity in old_activities:
            # Archive logic here
            session.delete(activity)

        session.commit()
        logger.info(f"Cleaned up {len(old_activities)} old activities")
```

### Redis Cleanup

Set TTL on results:

```bash
AGENTEXEC_RESULT_TTL=86400  # 24 hours
```

Monitor memory usage:

```bash
redis-cli INFO memory
```

## Checklist

Before going to production:

- [ ] PostgreSQL with SSL enabled
- [ ] Redis with password and persistence
- [ ] Connection pooling configured
- [ ] Graceful shutdown timeout set
- [ ] Health endpoints implemented
- [ ] Structured logging configured
- [ ] Metrics exported
- [ ] Alerts configured
- [ ] Secrets in secure storage
- [ ] Database backups automated
- [ ] Cleanup jobs scheduled
- [ ] Rate limiting enabled
- [ ] Network policies configured
- [ ] Load testing completed

## Next Steps

- [Docker Deployment](docker.md) - Containerization
- [Architecture](../concepts/architecture.md) - System design
- [Configuration](../getting-started/configuration.md) - All options
