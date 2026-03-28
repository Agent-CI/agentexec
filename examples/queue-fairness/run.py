"""Queue fairness test.

Validates that tasks distributed across many partition queues get
roughly equal treatment under the scan-based dequeue strategy.

Usage:
    uv run python examples/queue-fairness/run.py
    uv run python examples/queue-fairness/run.py --partitions 100 --tasks-per-partition 5 --workers 8
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from uuid import UUID, uuid4

from pydantic import BaseModel

import agentexec as ax
from agentexec.config import CONF
from agentexec.state import backend


class BenchContext(BaseModel):
    partition_id: int
    task_index: int
    queued_at: float


async def enqueue_tasks(partitions: int, tasks_per_partition: int) -> int:
    """Push tasks across N partitions with M tasks each."""
    total = 0
    for p in range(partitions):
        partition_key = f"partition:{p}"
        for t in range(tasks_per_partition):
            task = ax.Task(
                task_name="bench_task",
                context={
                    "partition_id": p,
                    "task_index": t,
                    "queued_at": time.time(),
                },
                agent_id=uuid4(),
            )
            await backend.queue.push(
                CONF.queue_prefix,
                task.model_dump_json(),
                partition_key=partition_key,
            )
            total += 1
    return total


async def worker(
    worker_id: int,
    results: list[dict],
    stop_event: asyncio.Event,
    work_duration: float,
):
    """Simulated worker that pops tasks and records timing."""
    while not stop_event.is_set():
        data = await backend.queue.pop(CONF.queue_prefix, timeout=1)
        if data is None:
            # Check if we should stop
            await asyncio.sleep(0.1)
            continue

        picked_up_at = time.time()
        context = data.get("context", {})
        queued_at = context.get("queued_at", picked_up_at)
        wait_time = picked_up_at - queued_at

        results.append({
            "worker_id": worker_id,
            "partition_id": context.get("partition_id"),
            "task_index": context.get("task_index"),
            "wait_time": wait_time,
            "picked_up_at": picked_up_at,
        })

        # Simulate work
        await asyncio.sleep(work_duration)

        # Release the partition lock
        partition_key = f"partition:{context.get('partition_id')}"
        await backend.queue.release_lock(CONF.queue_prefix, partition_key)


async def run(
    partitions: int,
    tasks_per_partition: int,
    num_workers: int,
    work_duration: float,
):
    print(f"Enqueueing {partitions} partitions x {tasks_per_partition} tasks = {partitions * tasks_per_partition} total")
    total = await enqueue_tasks(partitions, tasks_per_partition)
    print(f"Enqueued {total} tasks")

    results: list[dict] = []
    stop_event = asyncio.Event()

    print(f"Starting {num_workers} workers (simulated work: {work_duration}s)")
    start = time.time()

    workers = [
        asyncio.create_task(worker(i, results, stop_event, work_duration))
        for i in range(num_workers)
    ]

    # Wait until all tasks are processed
    while len(results) < total:
        await asyncio.sleep(0.5)
        elapsed = time.time() - start
        print(f"  {len(results)}/{total} tasks processed ({elapsed:.1f}s)", end="\r")

    elapsed = time.time() - start
    stop_event.set()

    # Let workers drain
    await asyncio.gather(*workers, return_exceptions=True)

    print(f"\n\nCompleted {len(results)} tasks in {elapsed:.1f}s")
    print(f"Throughput: {len(results) / elapsed:.1f} tasks/sec")

    # Analyze fairness per partition
    partition_times: dict[int, list[float]] = {}
    for r in results:
        pid = r["partition_id"]
        if pid not in partition_times:
            partition_times[pid] = []
        partition_times[pid].append(r["wait_time"])

    avg_per_partition = {
        pid: statistics.mean(times) for pid, times in partition_times.items()
    }

    all_waits = [r["wait_time"] for r in results]
    all_avgs = list(avg_per_partition.values())

    print(f"\nWait time (seconds from enqueue to pickup):")
    print(f"  Overall mean:   {statistics.mean(all_waits):.3f}s")
    print(f"  Overall median: {statistics.median(all_waits):.3f}s")
    print(f"  Overall stdev:  {statistics.stdev(all_waits):.3f}s")
    print(f"  Min:            {min(all_waits):.3f}s")
    print(f"  Max:            {max(all_waits):.3f}s")

    print(f"\nFairness across {len(partition_times)} partitions:")
    print(f"  Mean of partition averages: {statistics.mean(all_avgs):.3f}s")
    print(f"  Stdev of partition averages: {statistics.stdev(all_avgs):.3f}s")
    print(f"  Min partition avg:  {min(all_avgs):.3f}s")
    print(f"  Max partition avg:  {max(all_avgs):.3f}s")
    print(f"  Spread (max-min):   {max(all_avgs) - min(all_avgs):.3f}s")

    # Worker distribution
    worker_counts: dict[int, int] = {}
    for r in results:
        wid = r["worker_id"]
        worker_counts[wid] = worker_counts.get(wid, 0) + 1

    print(f"\nWorker distribution:")
    for wid in sorted(worker_counts):
        print(f"  Worker {wid}: {worker_counts[wid]} tasks")

    await backend.close()


def main():
    parser = argparse.ArgumentParser(description="Queue fairness benchmark")
    parser.add_argument("--partitions", type=int, default=500, help="Number of partition queues")
    parser.add_argument("--tasks-per-partition", type=int, default=12, help="Tasks per partition")
    parser.add_argument("--workers", type=int, default=4, help="Number of concurrent workers")
    parser.add_argument("--work-duration", type=float, default=0.5, help="Simulated work time (seconds)")
    args = parser.parse_args()

    asyncio.run(run(
        partitions=args.partitions,
        tasks_per_partition=args.tasks_per_partition,
        num_workers=args.workers,
        work_duration=args.work_duration,
    ))


if __name__ == "__main__":
    main()
