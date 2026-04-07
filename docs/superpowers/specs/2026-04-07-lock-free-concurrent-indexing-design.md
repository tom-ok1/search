# Lock-Free Concurrent Indexing

## Problem

Concurrent indexing does not scale: 2 goroutines is slower than 1, and 8 goroutines achieves only 2x speedup instead of near-linear scaling (issue #33).

Root cause: every `addDocument` call acquires 3 global mutexes sequentially on the hot path:

| Lock | Acquisitions per doc | Purpose |
|------|---------------------|---------|
| `perThreadPool.mu` | 2 | Get/return DWPT |
| `DeleteQueue.mu` | 1 | Read tail in `updateSlice` |
| `FlushControl.mu` | 1-2 | Byte accounting + flush check |
| **Total** | **4-5 mutex locks per document** | |

With N goroutines, all serialize on these locks for every document.

## Approach

Follow Lucene's patterns: make the common path (no flush needed, ~99% of calls) completely lock-free using atomic operations. Mutexes are only acquired for rare events (flush trigger, commit, explicit deletes).

## Design

### 1. FlushControl: Atomic byte accounting

**Current**: `doAfterDocument` acquires `fc.mu` on every call to update `activeBytes` and check the flush threshold.

**Change**: `activeBytes` becomes `atomic.Int64`.

`doAfterDocument` fast path (no flush needed):
1. `activeBytes.Add(bytesAdded)` — atomic
2. `activeBytes.Load()` to compare against `ramBufferSize` — atomic
3. If below threshold: return false, **no lock acquired**

`doAfterDocument` slow path (flush triggered):
1. Acquire `fc.mu`
2. Re-check threshold under lock (double-check pattern — another goroutine may have already flushed between the atomic load and lock acquisition)
3. If still over threshold: mark DWPT flush-pending, subtract bytes from `activeBytes`, add to `flushBytes`, append to `flushQueue`
4. Check stall condition, release lock

`flushBytes` stays under mutex — only touched during flush trigger/completion (infrequent). Stall check also moves into the slow path.

### 2. DeleteQueue: Atomic tail pointer

**Current**: `updateSlice` acquires `dq.mu` to read `dq.tail` on every document.

**Change**: `tail` becomes `atomic.Pointer[deleteNode]`.

`updateSlice` (per-document hot path):
1. `tail.Load()` — atomic, no lock
2. Compare with slice's current tail
3. Return whether new deletes exist

`addDelete` (rare — only on explicit deletes):
1. Acquire `dq.mu`
2. Link new node: `old_tail.next = node`
3. `tail.Store(node)` — atomic
4. Release lock

`newSlice` also becomes a simple atomic load.

`freezeGlobalBuffer` and `tryApplyGlobalSlice` still acquire `dq.mu` — these are infrequent and correctness-critical.

### 3. perThreadPool: sync.Pool replacement

**Current**: `getAndLock`/`returnAndUnlock` acquire `p.mu` to manipulate a free list and active map on every document.

**Change**: Replace the hand-rolled free list with `sync.Pool` for the common path.

`sync.Pool` has per-P sharding internally — goroutines on different OS threads don't contend at all. The `active` tracking map is only needed during full flush (commit).

Common path (`fullFlush == false`, ~99.9% of calls):
- `getAndLock`: `sync.Pool.Get()` — lock-free
- `returnAndUnlock`: `sync.Pool.Put()` — lock-free

Full flush path (`fullFlush == true`, only during commit):
- Acquire mutex, track active DWPTs in the map as before
- Acceptable because commit is infrequent and already serialized

Implementation:
- Add `atomic.Bool` field `fullFlush` for fast-path check
- `getAndLock` checks `fullFlush` atomically. If false, use `sync.Pool`. If true, fall back to mutex + active map tracking.
- `returnAndUnlock` same pattern: atomic check, then either `sync.Pool.Put()` or mutex path.

`sync.Pool` may discard objects between GC cycles. This is fine — a cleared DWPT just means we allocate a fresh one (same as the current `newDWPT` fallback). DWPTs are lightweight to create; the expensive part is the accumulated document data, which gets flushed before return.

## Result: Lock acquisitions per document

| Lock | Before | After (common path) |
|------|--------|---------------------|
| `perThreadPool.mu` | 2 | 0 (sync.Pool) |
| `DeleteQueue.mu` | 1 | 0 (atomic load) |
| `FlushControl.mu` | 1-2 | 0 (atomic add/load) |
| **Total** | **4-5** | **0** |

Mutex only acquired when:
- Flush threshold crossed (~1 per thousands of docs)
- Full flush / commit (once per commit)
- Delete-by-term issued (application-driven, rare)

## Expected outcome

With per-document lock contention eliminated, goroutines should scale near-linearly until I/O or memory bandwidth becomes the bottleneck. Target: 8 goroutines achieving 6-8x throughput (currently 2x).

## Testing

- **Correctness**: Existing unit and integration tests (no API changes)
- **Scaling**: `BenchmarkConcurrentIndex` in `index/scale_bench_test.go` — verify improved throughput ratios across 1/2/4/8 goroutines
- **Race detection**: Run tests with `-race` to verify atomic correctness

## Files to modify

- `index/flush_control.go` — atomic `activeBytes`, lock-free fast path
- `index/delete_queue.go` — atomic tail pointer, lock-free `updateSlice`
- `index/dwpt_pool.go` — `sync.Pool` replacement, atomic `fullFlush` flag
