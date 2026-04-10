# Python Multiprocessing with Spawn: Research for agentexec Worker Pool

**Date:** 2026-04-01
**Scope:** Python 3.12, 3.13, 3.14 behavior with `mp.get_context("spawn")`, focusing on
IPC (Queue, Event), asyncio integration, clean shutdown, and resource tracking.

---

## Table of Contents

1. [Start Methods: spawn vs fork vs forkserver](#1-start-methods-spawn-vs-fork-vs-forkserver)
2. [Python 3.14 Changes](#2-python-314-changes)
3. [mp.Queue Internals and the Feeder Thread Problem](#3-mpqueue-internals-and-the-feeder-thread-problem)
4. [mp.Event with spawn: Cross-Process Safety](#4-mpevent-with-spawn-cross-process-safety)
5. [asyncio + multiprocessing: Integration Patterns](#5-asyncio--multiprocessing-integration-patterns)
6. [Resource Tracker and Semaphore Leak Warnings](#6-resource-tracker-and-semaphore-leak-warnings)
7. [How Other Projects Handle Shutdown](#7-how-other-projects-handle-shutdown)
8. [os.\_exit() as a Shutdown Pattern](#8-os_exit-as-a-shutdown-pattern)
9. [Recommended Architecture for agentexec](#9-recommended-architecture-for-agentexec)
10. [Sources](#10-sources)

---

## 1. Start Methods: spawn vs fork vs forkserver

### fork (default on Linux through Python 3.13)

- Calls `os.fork()`. Child gets a copy-on-write clone of the parent's memory space.
- **Fast** startup because no re-import or serialization happens.
- **Dangerous** with threads: only the calling thread survives `fork()`. Any other threads
  (logging, asyncio event loops, connection pools) vanish, but their locks remain held.
- This is the root cause of the "Future attached to a different loop" error with async Redis
  clients: the parent's event loop object is inherited but the loop thread is gone.

```python
from threading import Lock
from os import fork

lock = Lock()
lock.acquire()

if fork() == 0:
    # Child process: lock is held, no thread to release it
    lock.acquire()  # DEADLOCK
```

### spawn (default on Windows/macOS, explicit everywhere else)

- Starts a **fresh Python interpreter**. The parent pickles the target function and arguments,
  sends them over a pipe, and the child unpickles and executes.
- **Safe**: no inherited threads, locks, event loops, or connection state.
- **Slower**: full interpreter startup + pickle/unpickle overhead.
- **Constraint**: everything passed to the child must be picklable. This means:
  - `mp.Queue` and `mp.Event` are picklable (they serialize their underlying OS handles).
  - `asyncio.Event`, `asyncio.Queue`, coroutines, lambdas, and closures over unpicklable state
    are NOT picklable.
  - Redis connection objects are NOT picklable (they hold sockets and event loop references).

### forkserver (new default on Linux in Python 3.14)

- On first use, starts a single-threaded "fork server" process early (before any threads exist).
- New workers are created by sending a request to the server, which calls `fork()` from a
  clean state (no threads to corrupt).
- **Safer than fork** (the server has no threads), **faster than spawn** (fork is cheaper
  than exec + re-import).
- Still requires picklable arguments (the request to the server must be serialized).
- Has the same `resource_tracker` overhead as spawn.

### Comparison Matrix

| Property | fork | spawn | forkserver |
|---|---|---|---|
| Startup speed | Fastest | Slowest | Medium |
| Thread-safe | No | Yes | Yes |
| Requires pickling | No | Yes | Yes |
| Inherits parent state | Yes (CoW) | No | No |
| Resource tracker | No | Yes | Yes |
| Default (3.12-3.13 Linux) | Yes | No | No |
| Default (3.14 Linux) | No | No | Yes |
| Default (Windows/macOS) | N/A | Yes | N/A |

### Implications for agentexec

Using `mp.get_context("spawn")` is the right choice. It avoids fork's thread-safety issues
and gives consistent behavior across all platforms and Python versions. The forkserver default
in 3.14 is irrelevant when you explicitly request "spawn", but it validates the direction:
the Python ecosystem is moving away from fork.

---

## 2. Python 3.14 Changes

### Default Start Method: fork -> forkserver (Linux)

From the [What's New in Python 3.14](https://docs.python.org/3/whatsnew/3.14.html):

> On Unix platforms other than macOS, 'forkserver' is now the default start method
> (replacing 'fork'). This does not affect Windows or macOS, where 'spawn' remains
> the default.

The deprecation path:
- **3.12**: `DeprecationWarning` when using fork with active threads.
- **3.14**: Default changed to forkserver on Linux. fork still available via explicit opt-in.

### Forkserver Security Enhancement

The forkserver now authenticates its control socket to prevent other processes from
requesting it to spawn arbitrary code. This is a security hardening change.

### New Process Methods

- **`Process.interrupt()`** (new in 3.14): Sends `SIGINT` to the child, enabling `finally`
  clauses and stack traces instead of silent death from `terminate()`.

### New ProcessPoolExecutor Methods

- **`terminate_workers()`**: Calls `Process.terminate()` on all living workers, then
  `Executor.shutdown()`.
- **`kill_workers()`**: Calls `Process.kill()` on all living workers, then
  `Executor.shutdown()`.

### redis-py Compatibility

redis-py had to [avoid the forkserver method](https://github.com/redis/redis-py/pull/3442)
in Python 3.14 because their multiprocessing tests used local functions as targets, which
cannot be pickled. The fix was to explicitly use `mp.get_context("fork")` in tests.

The broader issue: the Redis client class internally uses types that are not serializable
(sockets, event loops). This means Redis clients **cannot be passed** to child processes
with spawn or forkserver -- they must be created fresh in each worker. agentexec already
does this correctly by having workers call `backend.connect()` on startup.

---

## 3. mp.Queue Internals and the Feeder Thread Problem

### How mp.Queue Works

`mp.Queue` wraps a `multiprocessing.Pipe` with a buffer and a background thread:

```
put() -> buffer (deque) -> [feeder thread] -> pipe -> get()
```

1. `put()` appends the item to a process-local `deque` (never blocks).
2. A **feeder thread** (daemon thread, started on first `put()`) drains the deque and
   writes pickled objects into the pipe. It holds a `_wlock` (write lock) during writes.
3. `get()` reads from the other end of the pipe in a different process.

The feeder thread is created as a daemon thread:

```python
# From CPython Lib/multiprocessing/queues.py
self._thread = threading.Thread(
    target=Queue._feed,
    args=(self._buffer, self._notempty, self._send_bytes,
          self._wlock, self._reader.close, self._writer.close,
          self._ignore_epipe, self._on_queue_feeder_error,
          self._sem),
    name='QueueFeederThread',
    daemon=True,
)
```

### Why Processes Hang on Exit

The Queue registers two `Finalize` handlers (atexit-like callbacks):

1. **`_finalize_close`**: Appends a sentinel to the buffer, telling the feeder thread to stop.
2. **`_finalize_join`**: Calls `self._thread.join()` to wait for the feeder thread to finish
   flushing.

**The hang happens when:**

- The process exits (e.g., `asyncio.run()` completes after seeing the shutdown event).
- Python's atexit/finalizer machinery runs `_finalize_close` then `_finalize_join`.
- The feeder thread tries to flush remaining items, but the pipe's reader end (in the parent)
  is either closed or full.
- `_finalize_join` blocks forever waiting for the feeder thread.

This is the exact scenario in agentexec: workers put log messages and activity events into
the Queue. When shutdown is signaled, `asyncio.run()` completes, but the feeder thread may
still have buffered items. The process hangs in `_finalize_join`.

### The _wlock Deadlock (Pool Termination)

A separate but related issue: if a process is `terminate()`'d while the feeder thread holds
`_wlock`, the lock is never released. Any other process or thread trying to write to the
same Queue will deadlock. This is why `process.terminate()` should be a last resort.

### Solutions

**Option A: `cancel_join_thread()`**

```python
queue.cancel_join_thread()
```

Tells the Queue NOT to wait for the feeder thread during process exit. Data in the buffer
may be lost. Call this from the worker side when you know the Queue is fully drained.

**Option B: Drain before exit**

In the worker, stop putting items on the Queue before exiting:

```python
async def _run(self):
    try:
        while not self._context.shutdown_event.is_set():
            # ... process tasks ...
            pass
    finally:
        await backend.close()
        # Stop logging through the queue
        self.logger.handlers.clear()
        # Give feeder thread time to flush
        import time
        time.sleep(0.1)
```

**Option C: Sentinel-based shutdown from the parent**

Have the parent put a sentinel on the Queue, workers read it and exit. This ensures the
Queue is not in use when the process exits:

```python
# Parent
for _ in self._processes:
    self._worker_queue.put(None)  # sentinel

# Worker
while True:
    msg = queue.get(timeout=1)
    if msg is None:
        break
```

**Option D: `os._exit(0)` in workers (nuclear option)**

Bypasses all finalizers and atexit handlers, including `_finalize_join`. The feeder thread
just dies. See [Section 8](#8-os_exit-as-a-shutdown-pattern).

### Recommended Approach for agentexec

Use a combination:

1. Workers check `shutdown_event.is_set()` and stop processing.
2. Workers stop putting items on the Queue (detach IPC logger/handler).
3. Workers call `self._context.tx.cancel_join_thread()` before exiting.
4. Workers call `sys.exit(0)` (runs atexit but skips the join).
5. Parent calls `process.join(timeout=N)` then `process.terminate()` as fallback.

```python
def run(self) -> None:
    try:
        asyncio.run(self._run())
    except Exception as e:
        self.logger.exception(f"Worker {self._worker_id} fatal error: {e}")
    finally:
        # Prevent feeder thread from blocking exit
        self._context.tx.cancel_join_thread()
```

---

## 4. mp.Event with spawn: Cross-Process Safety

### How mp.Event Works Internally

`mp.Event` is backed by a `multiprocessing.Condition` which wraps a `multiprocessing.Lock`,
which wraps an OS-level **named semaphore** (on POSIX) or a kernel event (on Windows).

When you create an Event via `mp.get_context("spawn").Event()`:

1. A named semaphore is allocated in shared memory by the OS.
2. The Event object stores a reference to this semaphore (a name/handle).
3. When pickled for spawn, the semaphore **name** is pickled, not the semaphore itself.
4. When unpickled in the child, it re-opens the **same** named semaphore.

This means **mp.Event is truly cross-process safe with spawn**. Both parent and child
operate on the same underlying OS semaphore. `set()`, `clear()`, `is_set()`, and `wait()`
all work correctly across process boundaries.

### Potential Issues

1. **Polling vs. blocking**: `event.wait(timeout=N)` is a blocking call. In an async worker,
   you should NOT call `event.wait()` -- instead, poll `event.is_set()` in the async loop:

   ```python
   while not shutdown_event.is_set():
       # ... do async work ...
       await asyncio.sleep(1)
   ```

2. **Signal delivery timing**: When the parent calls `event.set()`, it's immediately visible
   to all processes. But if the worker is blocked on a Redis `BRPOP` (or similar), it won't
   check the event until the Redis call times out. This is why agentexec uses
   `backend.queue.pop(timeout=1)` -- the 1-second timeout ensures the shutdown event is
   checked regularly.

3. **Resource tracker**: The named semaphore backing the Event is tracked by the resource
   tracker process. If workers are killed without cleanup, the resource tracker logs a
   "leaked semaphore" warning. See [Section 6](#6-resource-tracker-and-semaphore-leak-warnings).

### Verdict

mp.Event works correctly with spawn. The "event doesn't propagate" issue in agentexec is
almost certainly caused by the process hanging in Queue cleanup (feeder thread), not by the
Event itself. The worker sees `shutdown_event.is_set() == True`, exits `asyncio.run()`, and
then hangs in the Queue's `_finalize_join`. The parent's `process.join(timeout=5)` then
times out, making it *look* like the event wasn't received.

---

## 5. asyncio + multiprocessing: Integration Patterns

### The Core Challenge

The parent process runs an asyncio event loop that needs to read from a `mp.Queue`.
But `mp.Queue.get()` is a blocking call that cannot be awaited.

### Pattern 1: run_in_executor (recommended)

Wrap the blocking `Queue.get()` in a thread executor:

```python
async def drain_queue(queue: mp.Queue, shutdown: mp.Event):
    loop = asyncio.get_event_loop()
    while not shutdown.is_set():
        try:
            message = await asyncio.wait_for(
                loop.run_in_executor(None, queue.get, True, 0.1),  # timeout=0.1
                timeout=1.0,
            )
            await handle(message)
        except (Empty, asyncio.TimeoutError):
            continue
```

This is cleaner than the polling approach but adds a thread. For agentexec's use case
(relatively low-frequency log/activity messages), either approach works.

### Pattern 2: Polling with get_nowait (current agentexec approach)

```python
async def drain_queue(queue: mp.Queue, shutdown: mp.Event):
    while not shutdown.is_set():
        try:
            message = queue.get_nowait()
            await handle(message)
        except Empty:
            await asyncio.sleep(0.05)
```

Simple but adds 50ms latency. Good enough for logs and activity events.

### Pattern 3: Pipe + asyncio reader (advanced)

Use a raw `mp.Pipe` with `loop.add_reader()` for zero-latency, zero-polling reads:

```python
parent_conn, child_conn = mp.Pipe(duplex=False)

# Parent (asyncio side)
loop = asyncio.get_event_loop()
loop.add_reader(parent_conn.fileno(), callback)

# Child (worker side)
child_conn.send(pickle.dumps(message))
```

This is event-driven but requires manual pickle/unpickle and doesn't have Queue's
buffering. Only worth it if polling latency is a problem.

### Shutdown Sequence for asyncio + mp.Queue

The critical order of operations:

```python
class Pool:
    def run(self) -> None:
        async def _loop() -> None:
            try:
                await self.start()
            except asyncio.CancelledError:
                pass
            finally:
                # 1. Signal workers to stop
                self._context.shutdown_event.set()

                # 2. Wait for workers to exit (they should drain their side of the Queue)
                for process in self._processes:
                    process.join(timeout=5)
                    if process.is_alive():
                        process.terminate()
                        process.join(timeout=2)

                # 3. Drain any remaining messages from the Queue
                while True:
                    try:
                        msg = self._worker_queue.get_nowait()
                        await self._handle(msg)
                    except Empty:
                        break

                # 4. Close backend connections BEFORE the event loop dies
                await backend.close()

        try:
            asyncio.run(_loop())
        except KeyboardInterrupt:
            pass
        finally:
            # 5. Close the Queue AFTER the event loop (synchronous)
            self._worker_queue.close()
            self._worker_queue.join_thread()
```

### The "Transport Bound to Dead Loop" Problem

When `asyncio.run()` completes (e.g., on KeyboardInterrupt), it closes the event loop.
If you then try to call `await backend.close()` outside `asyncio.run()`, the Redis client's
transport references the now-dead loop.

**Solution:** Always close async resources (Redis, database connections) INSIDE the
`asyncio.run()` block, in a `finally` clause or shutdown handler:

```python
async def _loop():
    try:
        await self.start()
    except asyncio.CancelledError:
        pass
    finally:
        # This runs while the event loop is still alive
        await backend.close()
```

The current agentexec code does `backend._client = None` which avoids the error by
discarding the reference, but this may leak the underlying connection. It's better to
close properly inside the loop.

---

## 6. Resource Tracker and Semaphore Leak Warnings

### What the Resource Tracker Is

When using `spawn` or `forkserver`, Python starts a background **resource tracker** process.
Its job is to track named OS resources (semaphores, shared memory blocks) created by the
program's processes, and clean them up at shutdown.

The tracker maintains a registry: when a process creates a named semaphore (e.g., for an
`mp.Event` or `mp.Lock`), it registers with the tracker. When a process exits cleanly, it
unregisters. When the tracker itself exits, it checks for any resources still registered and
unlinks them, printing a warning.

### Why the Warning Appears

```
UserWarning: resource_tracker: There appear to be N leaked semaphore objects to clean up at shutdown
```

This happens when:

1. **Workers are terminated (`SIGTERM`/`SIGKILL`) instead of exiting cleanly.** The worker
   never gets to unregister its semaphores with the tracker.
2. **Workers create synchronization primitives** (Lock, Event, Condition, Queue) that
   allocate named semaphores, and these aren't properly cleaned up.
3. **The resource tracker outlives the workers** and sees "orphaned" semaphores.

In agentexec's case, each worker's side of the `mp.Queue` creates internal locks that use
named semaphores. If a worker is terminated (because `process.join(timeout)` timed out and
we called `process.terminate()`), those semaphores are "leaked" from the tracker's
perspective.

### The Warning Is (Usually) Harmless

The resource tracker **does clean up** the leaked semaphores -- that's why it's warning you.
The semaphores are unlinked. The warning is informational, not an error. The OS resources
are freed.

However, if the resource tracker is killed too, the semaphores persist until reboot
(on Linux, they live in `/dev/shm/`).

### Suppressing the Warning

If graceful shutdown is not achievable in all cases:

```python
import warnings
warnings.filterwarnings("ignore", message="resource_tracker")
```

Or more targeted:

```python
import multiprocessing.resource_tracker
# Monkey-patch to suppress (use with caution)
import logging
multiprocessing.resource_tracker._resource_tracker._warn = lambda *a, **kw: None
```

### Fixing the Warning Properly

1. **Ensure workers exit cleanly**: Workers should return from their target function, not
   be terminated.
2. **Use `cancel_join_thread()`**: Prevents the Queue's feeder thread from blocking exit,
   so workers can exit cleanly without being terminated.
3. **Close/cleanup from within the worker**:

```python
@classmethod
def run_in_process(cls, worker_id: int, context: WorkerContext) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        instance = cls(worker_id, context)
        instance.run()
    finally:
        # Ensure the queue won't block our exit
        context.tx.cancel_join_thread()
```

---

## 7. How Other Projects Handle Shutdown

### Gunicorn

- **Architecture**: Arbiter (parent) manages worker processes via signals.
- **Shutdown**: Arbiter sends `SIGTERM` to workers. Workers set `self.alive = False` and
  finish their current request. A `graceful_timeout` controls how long to wait.
- **Escalation**: If workers don't exit within the timeout, `SIGKILL` is sent.
- **No Queue**: Gunicorn doesn't use `mp.Queue` for IPC. Workers communicate via shared
  sockets and a simple pipe for the "worker notify" heartbeat.
- **Start method**: Gunicorn uses pre-fork (plain `os.fork()`), not spawn.

### Uvicorn

- **Architecture**: `Multiprocess` supervisor manages worker processes.
- **Start method**: Uses `spawn` (for Windows compatibility).
- **Shutdown sequence**:
  1. Signal handler sets `should_exit` event.
  2. Main loop exits.
  3. `terminate_all()` sends `SIGTERM` to each child.
  4. `join_all()` waits for process completion.
- **Health monitoring**: Periodically checks if workers are alive. Hung workers (no response
  within `timeout_worker_healthcheck`) are killed and replaced.
- **No `os._exit()` or `cancel_join_thread()`** -- relies on standard terminate/join.

### Celery

- **Architecture**: Uses `billiard` (a fork of Python's multiprocessing) for its prefork pool.
- **Start method**: fork (via billiard), with explicit fork-safety handling.
- **Shutdown**: Workers receive a "soft" shutdown signal (finish current task), then a "hard"
  shutdown (terminate). Celery's `consumer.close()` -> `pool.close()` -> `pool.join()`.
- **Queue handling**: Uses billiard's enhanced Queue with better cleanup and error handling.
- **Warm shutdown**: Workers consume a sentinel task from the queue to signal exit.

### Key Takeaways

| Project | Start Method | Shutdown Signal | Queue Cleanup | Forced Kill |
|---|---|---|---|---|
| Gunicorn | fork | SIGTERM -> SIGKILL | N/A (no Queue) | Yes, after timeout |
| Uvicorn | spawn | SIGTERM -> kill() | N/A (no Queue) | Yes, after timeout |
| Celery | fork (billiard) | Soft -> Hard | Sentinel values | Yes, after timeout |
| **agentexec** | **spawn** | **mp.Event** | **cancel_join_thread** | **terminate after timeout** |

All projects follow the same pattern: **cooperative shutdown first, forced kill as fallback.**
The difference is in the IPC mechanism (signals vs events) and how they handle the Queue.

---

## 8. os._exit() as a Shutdown Pattern

### What os._exit() Does

`os._exit(n)` terminates the process **immediately** without:
- Running `atexit` handlers
- Flushing stdio buffers
- Running `__del__` methods
- Running `finally` blocks (in any thread other than the calling one)
- Joining the Queue's feeder thread (`_finalize_join`)

### When It's Acceptable

`os._exit()` is an **accepted pattern** in worker processes specifically when:

1. The worker has finished all meaningful work.
2. All important data has been sent via the IPC channel.
3. The process is stuck in cleanup that will never complete (feeder thread deadlock).
4. You need guaranteed exit without risk of hanging.

CPython itself uses `os._exit()` in the `multiprocessing` module's `forkserver`:

```python
# From Lib/multiprocessing/forkserver.py
os._exit(code)  # called after worker function returns
```

### Risks

1. **Data loss**: Any un-flushed Queue items are lost.
2. **Resource leaks**: Named semaphores, shared memory, temp files are not cleaned up by
   the process (the resource tracker handles semaphores, but other resources may leak).
3. **Database corruption**: If the worker has uncommitted transactions.
4. **File descriptor leaks**: Open files/sockets are closed by the OS when the process
   exits, but application-level cleanup (e.g., Redis `QUIT`) doesn't happen.

### Recommended Usage in agentexec

`os._exit()` should be the **last resort**, not the default. The better approach:

```python
@classmethod
def run_in_process(cls, worker_id: int, context: WorkerContext) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        instance = cls(worker_id, context)
        instance.run()
    finally:
        context.tx.cancel_join_thread()
        # If we're still here after cleanup, force exit
        # This should rarely be needed
```

If `cancel_join_thread()` is called before exit, the feeder thread won't block, and
a normal `sys.exit(0)` (or just returning from the function) should work. Reserve
`os._exit()` for emergency fallback only.

---

## 9. Recommended Architecture for agentexec

Based on this research, here is the recommended shutdown sequence:

### Worker Side

```python
class Worker:
    @classmethod
    def run_in_process(cls, worker_id: int, context: WorkerContext) -> None:
        import signal
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        # Prevent feeder thread from blocking process exit
        context.tx.cancel_join_thread()

        instance = cls(worker_id, context)
        instance.run()

    def run(self) -> None:
        try:
            asyncio.run(self._run())
        except Exception as e:
            self.logger.exception(f"Worker {self._worker_id} fatal error: {e}")

    async def _run(self) -> None:
        try:
            while not self._context.shutdown_event.is_set():
                if data := await backend.queue.pop(timeout=1):
                    task = Task.model_validate(data)
                    definition = self._context.tasks[task.task_name]
                    await definition.execute(task)
                else:
                    await asyncio.sleep(1)
        finally:
            # Close backend INSIDE asyncio.run() while loop is alive
            await backend.close()
            # Detach the queue-based logger so no more puts happen
            self.logger.handlers.clear()
```

### Pool Side

```python
class Pool:
    def run(self) -> None:
        async def _loop() -> None:
            try:
                await self.start()
            except asyncio.CancelledError:
                pass
            finally:
                await self.shutdown()

        try:
            asyncio.run(_loop())
        except KeyboardInterrupt:
            pass
        finally:
            # Synchronous cleanup after event loop is gone
            self._worker_queue.close()
            self._worker_queue.join_thread()

    async def shutdown(self, timeout: int = 5) -> None:
        # 1. Signal workers
        self._context.shutdown_event.set()

        # 2. Wait for workers to exit cleanly
        for process in self._processes:
            process.join(timeout=timeout)
            if process.is_alive():
                logger.warning(f"Worker {process.pid} did not stop, terminating")
                process.terminate()
                process.join(timeout=2)

        # 3. Drain remaining messages
        while True:
            try:
                msg = self._worker_queue.get_nowait()
                await self._handle(msg)
            except Empty:
                break

        # 4. Close backend while event loop is alive
        await backend.close()

        self._processes.clear()
```

### Key Points

1. **Call `cancel_join_thread()` early in the worker** -- before any work starts. This
   prevents the feeder thread from blocking exit regardless of what happens later.

2. **Close async resources inside `asyncio.run()`** -- both in the worker (`backend.close()`)
   and in the pool (`backend.close()`). Never try to close an async client after the loop
   is dead.

3. **Drain the Queue after workers exit** -- pick up any remaining log/activity messages
   before closing the Queue.

4. **Use `process.join(timeout)` + `process.terminate()` as fallback** -- this is the
   universal pattern used by Gunicorn, Uvicorn, and Celery.

5. **Don't use `os._exit()`** unless `cancel_join_thread()` proves insufficient.

6. **Suppress resource tracker warnings** only if graceful shutdown handles the common case.
   The warnings indicate terminated (not cleanly exited) workers.

### Version Compatibility

This architecture works identically on Python 3.12, 3.13, and 3.14 because:
- We explicitly use `mp.get_context("spawn")`, bypassing any default changes.
- All IPC primitives (Queue, Event) are picklable with spawn.
- No reliance on fork-inherited state.
- Redis clients are created fresh in each worker process.

---

## 10. Sources

- [Python 3.14 What's New - Multiprocessing Changes](https://docs.python.org/3/whatsnew/3.14.html)
- [Python multiprocessing documentation](https://docs.python.org/3/library/multiprocessing.html)
- [CPython multiprocessing Queue source (queues.py)](https://github.com/python/cpython/blob/main/Lib/multiprocessing/queues.py)
- [cpython issue #84559: multiprocessing default start method change](https://github.com/python/cpython/issues/84559)
- [redis-py PR #3442: Avoid forkserver in Python 3.14](https://github.com/redis/redis-py/pull/3442)
- [redis-py issue #3141: Async redis in multiprocessing hangs](https://github.com/redis/redis-py/issues/3141)
- [PyTorch issue #169252: fork() no longer default in 3.14](https://github.com/pytorch/pytorch/issues/169252)
- [Python discuss: Switching default to spawn on POSIX](https://discuss.python.org/t/switching-default-multiprocessing-context-to-spawn-on-posix-as-well/21868)
- [Why your multiprocessing Pool is stuck (pythonspeed.com)](https://pythonspeed.com/articles/python-multiprocessing/)
- [Graceful exit with Python multiprocessing (the-fonz)](https://the-fonz.gitlab.io/posts/python-multiprocessing/)
- [Signal handling with async multiprocesses (cziegler)](https://medium.com/@cziegler_99189/gracefully-shutting-down-async-multiprocesses-in-python-2223be384510)
- [Python multiprocessing shutdown (jimmyg)](https://jimmyg.org/blog/2024/multiprocessing-shutdown/index.html)
- [Post mortem of a multiprocessing bug (Tyler Neylon)](https://tylerneylon.com/a/py_bug_analysis/py_bug_analysis.html)
- [Python bugs: Queue hangs when process dies (#43805)](https://bugs.python.org/issue43805)
- [Python bugs: Deadlock with mp.Queue (#29797)](https://bugs.python.org/issue29797)
- [Fork vs Spawn (British Geological Survey)](https://britishgeologicalsurvey.github.io/science/python-forking-vs-spawn/)
- [Gunicorn signal handling docs](https://docs.gunicorn.org/en/stable/signals.html)
- [Gunicorn arbiter source](https://github.com/benoitc/gunicorn/blob/master/gunicorn/arbiter.py)
- [Uvicorn multiprocess supervisor](https://github.com/encode/uvicorn/blob/master/uvicorn/supervisors/multiprocess.py)
- [Uvicorn deployment docs](https://uvicorn.dev/deployment/)
- [Celery workers guide](https://docs.celeryq.dev/en/stable/userguide/workers.html)
- [aioprocessing library](https://github.com/dano/aioprocessing)
- [The power and danger of os.fork (Tom Rutherford)](https://medium.com/@tmrutherford/the-default-method-of-spawning-processes-on-linux-is-changing-in-python-3-14-heres-why-b9711df0d1b1)
- [Python multiprocessing resource_tracker source](https://fossies.org/linux/Python/Lib/multiprocessing/resource_tracker.py)
- [cpython issue #104090: Leaked semaphore in test_concurrent_futures](https://github.com/python/cpython/issues/104090)
