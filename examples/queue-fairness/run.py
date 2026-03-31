"""Queue fairness benchmark.

Validates that tasks distributed across many partition queues get
roughly equal treatment under the scan-based dequeue strategy.

Measures two dimensions of fairness:
  - Worker fairness: are tasks spread evenly across workers?
  - Partition fairness: are partitions served in a balanced order,
    or do some starve while others get picked up immediately?

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
from uuid import uuid4

from pydantic import BaseModel

import agentexec as ax
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
        data = await backend.queue.pop(timeout=1)
        if data is None:
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
        await backend.queue.complete(partition_key)


async def run(
    partitions: int,
    tasks_per_partition: int,
    num_workers: int,
    work_duration: float,
):
    total = partitions * tasks_per_partition
    print(f"Enqueueing {partitions} partitions x {tasks_per_partition} tasks = {total} total")
    enqueue_start = time.time()
    await enqueue_tasks(partitions, tasks_per_partition)
    print(f"Enqueued {total} tasks in {time.time() - enqueue_start:.1f}s")

    results: list[dict] = []
    stop_event = asyncio.Event()

    print(f"Starting {num_workers} workers (simulated work: {work_duration}s)")
    start = time.time()

    workers = [
        asyncio.create_task(worker(i, results, stop_event, work_duration))
        for i in range(num_workers)
    ]

    while len(results) < total:
        await asyncio.sleep(0.5)
        elapsed = time.time() - start
        print(f"  {len(results)}/{total} tasks processed ({elapsed:.1f}s)", end="\r")

    elapsed = time.time() - start
    stop_event.set()
    await asyncio.gather(*workers, return_exceptions=True)

    print(f"\n\nCompleted {len(results)} tasks in {elapsed:.1f}s")
    print(f"Throughput: {len(results) / elapsed:.1f} tasks/sec")

    # --- Wait time analysis ---
    all_waits = [r["wait_time"] for r in results]
    print(f"\nWait time (enqueue → pickup):")
    print(f"  Mean:   {statistics.mean(all_waits):.3f}s")
    print(f"  Median: {statistics.median(all_waits):.3f}s")
    print(f"  Stdev:  {statistics.stdev(all_waits):.3f}s")
    print(f"  Min:    {min(all_waits):.3f}s")
    print(f"  Max:    {max(all_waits):.3f}s")

    # --- Worker fairness ---
    worker_counts: dict[int, int] = {}
    for r in results:
        wid = r["worker_id"]
        worker_counts[wid] = worker_counts.get(wid, 0) + 1

    worker_vals = list(worker_counts.values())
    ideal_per_worker = total / num_workers

    print(f"\nWorker fairness ({num_workers} workers, ideal {ideal_per_worker:.0f} each):")
    for wid in sorted(worker_counts):
        count = worker_counts[wid]
        pct = count / total * 100
        print(f"  Worker {wid}: {count} tasks ({pct:.1f}%)")
    if len(worker_vals) > 1:
        print(f"  Stdev: {statistics.stdev(worker_vals):.1f}")

    # --- Partition fairness ---
    # For each partition, when was its first task picked up (relative to start)?
    # A fair system serves all partitions at roughly the same pace.
    partition_first_pickup: dict[int, float] = {}
    partition_waits: dict[int, list[float]] = {}
    for r in results:
        pid = r["partition_id"]
        pickup_offset = r["picked_up_at"] - start
        if pid not in partition_first_pickup or pickup_offset < partition_first_pickup[pid]:
            partition_first_pickup[pid] = pickup_offset
        if pid not in partition_waits:
            partition_waits[pid] = []
        partition_waits[pid].append(r["wait_time"])

    first_pickups = list(partition_first_pickup.values())
    avg_waits = [statistics.mean(w) for w in partition_waits.values()]

    print(f"\nPartition fairness ({len(partition_first_pickup)} partitions):")

    print(f"  First-task pickup time (seconds after start):")
    print(f"    Mean:   {statistics.mean(first_pickups):.3f}s")
    print(f"    Median: {statistics.median(first_pickups):.3f}s")
    print(f"    Stdev:  {statistics.stdev(first_pickups):.3f}s")
    print(f"    Min:    {min(first_pickups):.3f}s")
    print(f"    Max:    {max(first_pickups):.3f}s")
    print(f"    Spread: {max(first_pickups) - min(first_pickups):.3f}s")

    print(f"  Average wait per partition:")
    print(f"    Mean:   {statistics.mean(avg_waits):.3f}s")
    print(f"    Stdev:  {statistics.stdev(avg_waits):.3f}s")
    print(f"    Spread: {max(avg_waits) - min(avg_waits):.3f}s")

    # Identify starved partitions (first pickup > 2x median)
    median_pickup = statistics.median(first_pickups)
    starved = [pid for pid, t in partition_first_pickup.items() if t > median_pickup * 2]
    if starved:
        print(f"  Starved partitions (first pickup > 2x median): {len(starved)}/{partitions}")
    else:
        print(f"  No starved partitions detected")

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
