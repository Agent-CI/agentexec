# Queue Fairness Benchmark

Validates that the scan-based partitioned queue distributes work fairly across both workers and partition keys.

## Background

agentexec uses Redis `SCAN` to iterate partition queues during dequeue. SCAN returns keys in hash-table order, which is effectively random — this gives us pseudo-random partition selection without explicit shuffling or round-robin bookkeeping.

This benchmark measures two dimensions of fairness:

- **Worker fairness**: Are tasks spread evenly across workers?
- **Partition fairness**: Are all partitions served at a similar pace, or do some starve while others get immediate attention?

## Usage

```bash
uv run python examples/queue-fairness/run.py
uv run python examples/queue-fairness/run.py --partitions 1000 --tasks-per-partition 10 --workers 8
```

Requires a running Redis instance (`REDIS_URL` environment variable).

## What it does

1. Enqueues `partitions * tasks_per_partition` tasks, each routed to a named partition queue
2. Spawns N async workers that pop, simulate work, then release the partition lock via `complete()`
3. Records timing data for every task: which worker, which partition, wait time, pickup time
4. Reports fairness metrics at the end

## Results

At 1000 partitions, 10 tasks each (10,000 total), 8 workers:

### Worker fairness

Each worker processed between 1243 and 1257 tasks (ideal: 1250). Standard deviation of 5.2 across 8 workers — essentially uniform distribution.

```
Worker 0: 1257 tasks (12.6%)
Worker 1: 1249 tasks (12.5%)
Worker 2: 1248 tasks (12.5%)
Worker 3: 1257 tasks (12.6%)
Worker 4: 1246 tasks (12.5%)
Worker 5: 1243 tasks (12.4%)
Worker 6: 1247 tasks (12.5%)
Worker 7: 1253 tasks (12.5%)
```

### Partition fairness

The "first-task pickup time" measures when each partition's first task gets served, relative to the start. A fair system serves all partitions at roughly the same pace — no partition should wait significantly longer than others for its first task.

```
First-task pickup time (seconds after start):
  Mean:   15.606s
  Median: 15.685s
  Stdev:  9.030s
  Min:    0.019s
  Max:    31.103s
```

The median first pickup (15.7s) lands at almost exactly half the total runtime (31.6s), which is what you'd expect from a uniform distribution. No partitions were flagged as starved (first pickup > 2x the median).

### Throughput

Throughput held steady at ~317 tasks/sec across all partition counts tested (50, 200, 1000). SCAN-based dequeue does not degrade as the number of partitions grows.

## Why it works

Redis `SCAN` iterates the hash table in slot order, which is determined by the hash of each key. Since partition keys hash to different slots, the iteration order is effectively random and changes as keys are added or removed. This gives us:

- **No hot spots**: No partition is systematically visited first or last
- **No coordination**: Workers don't need to agree on which partition to try next
- **Free rebalancing**: As partitions drain and their keys disappear, SCAN naturally skips them
- **Lock-aware skipping**: Locked partitions are skipped immediately, so workers don't block on busy partitions — they move on to the next available one
