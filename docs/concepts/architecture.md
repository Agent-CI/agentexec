# Architecture

This document explains the high-level architecture of agentexec and how its components work together to provide reliable, scalable AI agent execution.

## Overview

agentexec is designed around a **distributed task queue** pattern, where:

1. **Producers** (your API, CLI, or other services) enqueue tasks
2. **Redis** stores the task queue and coordinates workers
3. **Workers** (multiple processes) dequeue and execute tasks
4. **Database** stores activity logs and task metadata

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   FastAPI   │     │    Redis    │     │  Database   │
│   (API)     │────>│   (Queue)   │<────│  (Activity) │
└─────────────┘     └──────┬──────┘     └──────▲──────┘
                          │                    │
                    ┌─────▼─────┐             │
                    │  Worker   │─────────────┘
                    │   Pool    │
                    └───────────┘
                          │
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
      ┌─────────┐   ┌─────────┐   ┌─────────┐
      │Worker 0 │   │Worker 1 │   │Worker 2 │
      └─────────┘   └─────────┘   └─────────┘
```

## Core Components

### Task Queue (Redis)

The task queue is the central coordination point:

- **Task Storage**: Tasks are serialized to JSON and stored in a Redis list
- **Priority Support**: HIGH priority tasks go to the front of the queue, LOW to the back
- **Blocking Dequeue**: Workers use `BRPOP` to efficiently wait for tasks
- **Result Storage**: Task results are cached in Redis with configurable TTL
- **Pub/Sub**: Used for log streaming from workers to the main process

**Why Redis?**

Redis provides atomic operations, persistence options, and excellent performance for queue operations. Its pub/sub capabilities enable real-time log streaming without polling.

### Worker Pool

The worker pool manages multiple Python processes:

```python
pool = ax.WorkerPool(engine=engine, database_url=DATABASE_URL)

@pool.task("my_task")
async def my_task(agent_id: UUID, context: MyContext):
    ...

pool.run()  # Starts workers and collects logs
```

**Key characteristics:**

- **Multi-process**: Each worker is a separate Python process using `multiprocessing`
- **Process isolation**: Worker crashes don't affect other workers
- **Graceful shutdown**: Workers complete current tasks before stopping
- **Log aggregation**: Logs from all workers are collected via Redis pub/sub

### Activity Tracking (Database)

Activity tracking provides observability:

```
┌──────────────────────────────────────────────────────┐
│                    Activity                          │
├──────────────────────────────────────────────────────┤
│ id: UUID (primary key)                               │
│ agent_id: UUID (unique, indexed)                     │
│ agent_type: str (task name)                          │
│ created_at: datetime                                 │
│ updated_at: datetime                                 │
└──────────────────────────────────────────────────────┘
                          │
                          │ 1:N
                          ▼
┌──────────────────────────────────────────────────────┐
│                   ActivityLog                        │
├──────────────────────────────────────────────────────┤
│ id: int (primary key)                                │
│ activity_id: UUID (foreign key)                      │
│ message: text                                        │
│ status: enum (QUEUED, RUNNING, COMPLETE, ERROR, ...) │
│ percentage: int (nullable)                │
│ created_at: datetime                                 │
└──────────────────────────────────────────────────────┘
```

**Features:**

- **Full lifecycle tracking**: Every state change is recorded
- **Progress logging**: Agents can report completion percentage
- **Query-friendly**: Efficient queries for listing and detail views
- **Database agnostic**: Works with PostgreSQL, MySQL, SQLite

### Agent Runners

Runners integrate with agent frameworks (like OpenAI Agents SDK):

```python
runner = ax.OpenAIRunner(
    agent_id=agent_id,
    max_turns_recovery=True,
)

result = await runner.run(agent, input="...", max_turns=15)
```

**Responsibilities:**

- **Lifecycle management**: Update activity status (RUNNING → COMPLETE/ERROR)
- **Progress reporting**: Provide tools for agents to report status
- **Error handling**: Catch and log exceptions
- **Recovery**: Handle max turns exceeded gracefully

## Data Flow

### Task Enqueue Flow

```
1. API receives request
2. Create Activity record (status=QUEUED)
3. Serialize task to JSON
4. Push to Redis queue (LPUSH or RPUSH based on priority)
5. Return agent_id to caller
```

```python
# In your API handler
task = await ax.enqueue("research", ResearchContext(company="Acme"))
return {"agent_id": task.agent_id}
```

### Task Execute Flow

```
1. Worker calls BRPOP on queue (blocking wait)
2. Deserialize task JSON
3. Update Activity status to RUNNING
4. Execute task handler
5. Store result in Redis (if needed for pipelines)
6. Update Activity status to COMPLETE or ERROR
7. Loop back to step 1
```

### Activity Query Flow

```
1. API receives status request with agent_id
2. Query Activity + ActivityLog from database
3. Return status, progress, and logs
```

```python
# In your API handler
activity = ax.activity.detail(session, agent_id)
return {
    "status": activity.status,
    "progress": activity.latest_percentage,
    "logs": [{"message": log.message} for log in activity.logs]
}
```

## Process Model

### Main Process

The main process (your application):

1. Receives HTTP requests
2. Enqueues tasks to Redis
3. Queries database for activity status
4. Optionally starts and manages worker pool

### Worker Processes

Each worker process:

1. Initializes its own database session
2. Connects to Redis
3. Polls the task queue
4. Executes tasks
5. Reports logs via Redis pub/sub

```
Main Process                    Worker Process
     │                               │
     │  fork()                       │
     ├──────────────────────────────>│
     │                               │
     │                          Initialize DB session
     │                               │
     │                          Connect to Redis
     │                               │
     │                          ┌────┴────┐
     │                          │  Loop   │
     │                          │ BRPOP   │◄─────┐
     │                          │ Execute │      │
     │                          │ Log     │──────┘
     │                          └─────────┘
     │
 Collect logs via pub/sub
```

### Graceful Shutdown

On SIGTERM or SIGINT:

1. Main process signals shutdown via Redis event
2. Workers finish current task (up to timeout)
3. Workers close database and Redis connections
4. Main process waits for workers to exit
5. Pending activities are marked as CANCELED

```python
# Automatic on SIGTERM/SIGINT, or manual:
pool.shutdown(timeout=60)  # Wait up to 60 seconds
```

## Scalability

### Horizontal Scaling

Scale by running more worker processes:

```bash
# Single machine - more workers
AGENTEXEC_NUM_WORKERS=16

# Multiple machines - each runs its own pool
# Machine 1
python worker.py  # Spawns 8 workers

# Machine 2
python worker.py  # Spawns 8 more workers
```

All workers share the same Redis queue, automatically distributing load.

### Vertical Scaling

For CPU-bound agents, increase workers per machine. For I/O-bound agents (most LLM calls), workers can handle many concurrent tasks.

### Queue Partitioning

Use multiple queues for different workloads:

```python
# High-priority pool
pool_high = ax.WorkerPool(queue_name="high_priority", ...)

# Low-priority pool
pool_low = ax.WorkerPool(queue_name="low_priority", ...)

# Enqueue to specific queue
await ax.enqueue("urgent_task", ctx, queue_name="high_priority")
```

## Fault Tolerance

### Worker Crashes

If a worker crashes:

- Other workers continue processing
- Current task may be lost (not re-queued)
- Activity will show RUNNING (stale)

**Mitigation**: Use `activity.cancel_pending()` on startup to clean up stale activities.

### Redis Failures

If Redis is unavailable:

- Enqueue operations fail immediately
- Workers block waiting for reconnection
- Results become inaccessible

**Mitigation**: Use Redis Sentinel or Cluster for high availability.

### Database Failures

If the database is unavailable:

- Activity tracking fails
- Tasks may still execute but won't be logged
- API queries fail

**Mitigation**: Use database replication and failover.

## Security Considerations

### Network Security

- Use TLS for Redis connections (`rediss://`)
- Use SSL for database connections
- Run workers in private networks

### Authentication

- Redis: Use password authentication
- Database: Use strong credentials, least-privilege access
- OpenAI: Secure API key storage (environment variables, secrets manager)

### Data Isolation

- Use table prefixes for multi-tenant deployments
- Use separate Redis databases for isolation
- Consider separate queues for sensitive workloads

## Performance Considerations

### Queue Performance

- Redis `BRPOP` is O(1) - very fast
- Task serialization/deserialization is typically sub-millisecond
- Use `priority=HIGH` sparingly to avoid queue starvation

### Database Performance

- Activity queries are indexed by `agent_id`
- Log appending uses efficient `append_log()` method
- Consider archiving old activities for large deployments

### Memory Usage

- Each worker is a separate process with its own memory
- Task contexts should be reasonably sized (avoid large payloads)
- Results are cached in Redis - configure `result_ttl` appropriately

## Next Steps

- [Task Lifecycle](task-lifecycle.md) - Deep dive into task states
- [Worker Pool](worker-pool.md) - Worker process details
- [Activity Tracking](activity-tracking.md) - Observability features
