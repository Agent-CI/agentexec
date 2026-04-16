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
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine(DATABASE_URL)
pool = ax.Pool(engine=engine)

@pool.task("my_task")
async def my_task(agent_id: UUID, context: MyContext):
    ...

# Start via the CLI:   agentexec run mymodule:pool
# or programmatically: asyncio.run(pool.start())
```

**Key characteristics:**

- **Multi-process**: Each worker is a separate Python process started with
  the `spawn` method — fresh interpreter, no inherited state.
- **Daemon workers**: Workers die with the parent so they can't orphan and
  poll the queue after the pool exits.
- **Process isolation**: Worker crashes don't affect other workers.
- **Graceful shutdown**: Workers complete current tasks before stopping.
- **Log aggregation**: Every worker installs a stderr handler on startup so
  logs from framework code and user code share a consistent format.

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
activity = await ax.activity.detail(agent_id=agent_id)
latest = activity.logs[-1] if activity and activity.logs else None
return {
    "status": latest.status if latest else None,
    "progress": latest.percentage if latest else None,
    "logs": [{"message": log.message} for log in (activity.logs if activity else [])],
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
     │  spawn() fresh interpreter    │
     ├──────────────────────────────>│
     │                               │
     │                          Re-import user module
     │                               │
     │                          Install stderr log handler
     │                               │
     │                          ┌────┴────┐
     │                          │  Loop   │
     │                          │  pop    │◄─────┐
     │                          │ Execute │      │
     │                          │complete │──────┘
     │                          └─────────┘
     │
  Event handler (IPC queue)
  Scheduler loop
```

### Graceful Shutdown

On SIGTERM or SIGINT:

1. Main process sets the shared `multiprocessing.Event`.
2. Workers finish their current task and exit the loop.
3. Workers call `backend.close()` and exit.
4. Main process joins each worker (up to `CONF.graceful_shutdown_timeout`).
5. Any stragglers are `terminate()`'d.

```python
# Automatic on SIGTERM/SIGINT, or manual:
await pool.shutdown(timeout=60)
```

## Scalability

### Horizontal Scaling

Scale by running more worker processes:

```bash
# Single machine: more workers
AGENTEXEC_NUM_WORKERS=16

# Multiple machines: each runs its own pool
# Machine 1
agentexec run myapp.worker:pool -w 8

# Machine 2
agentexec run myapp.worker:pool -w 8
```

All pools share the same Redis queue, automatically distributing load.

### Vertical Scaling

For CPU-bound agents, increase workers per machine. For I/O-bound agents
(most LLM calls), workers can handle many concurrent tasks because each
handler is async.

### Partitioned Locking

Use `lock_key` on a task to serialize work within a partition (e.g. per
user, per document):

```python
@pool.task("sync_user", lock_key="user:{user_id}")
async def sync_user(agent_id: UUID, context: UserContext):
    ...
```

Tasks with matching evaluated keys execute one at a time; tasks with
different keys run concurrently.

## Fault Tolerance

### Worker Failures

If a task handler raises:

- The worker logs the full traceback.
- A `TaskFailed` message is sent to the main process via IPC.
- The main process re-enqueues the task at `HIGH` priority, up to
  `CONF.max_task_retries` (default `3`) times.

If the entire worker process crashes:

- Other workers continue processing.
- The in-flight task may be lost (not automatically re-queued).
- The activity record may be left in `RUNNING`.

**Mitigation:** Run `await ax.activity.cancel_pending()` on startup to
reconcile stale activities.

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
