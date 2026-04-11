# Worker Model DWPT Architecture

Replace Lucene-style DWPT pool with Go-idiomatic worker goroutines to eliminate lock contention and improve multi-goroutine scaling.

## Problem

The current architecture copies Lucene's DWPT pool pattern, which assumes expensive Java threads. In Go, this creates unnecessary contention:

- **3 mutex acquisitions per document**: `pool.getAndLock()` → `flushControl.doAfterDocument()` → `pool.returnAndUnlock()`
- At 8 goroutines × 100K docs = **2.4M lock acquisitions**
- Scaling regresses at G=4+ (0.89x vs G=1), with G=2 being the ceiling (1.48x)
- FlushControl lock is the hottest — acquired every single document even when no flush is needed
- More goroutines → faster RAM fill → more frequent small flushes → more segments → heavier merge at commit

## Design

### Phase 1: Worker Model + Lock-Free FlushControl

#### Worker Model

Replace `perThreadPool` with fixed worker goroutines, each owning a DWPT permanently.

```
AddDocument(doc)
    │
    ▼
round-robin select worker (atomic counter)
    │
    ▼
worker.inbox <- job          ← per-worker buffered channel
    │
    ▼
┌─ Worker Goroutine ─────────────────────────┐
│  dwpt.addDocument(doc)     ← no lock       │
│  activeBytes.Add(bytes)    ← atomic         │
│  if activeBytes >= threshold:               │
│      hand off dwpt to flushCh  ← rare      │
│      create new dwpt           ← resume    │
│  job.result <- err             ← respond   │
└─────────────────────────────────────────────┘
    │ (when flush triggered)
    ▼
┌─ Flush Goroutine ──────────────────────────┐
│  dwpt.prepareFlush()                        │
│  dwpt.flush(dir)           ← disk I/O      │
│  flushControl.doAfterFlush()               │
│  ticketQueue.markDone()                     │
└─────────────────────────────────────────────┘
```

- Worker count: `runtime.GOMAXPROCS(0)` by default
- Per-worker channels: each worker has its own `inbox` channel, eliminating shared-channel contention
- Hand-off flush: when a worker's DWPT triggers flush, the worker hands the DWPT to a flush goroutine, creates a fresh DWPT, and resumes accepting documents immediately
- Lock count per document: **3 → 0** on the common path (99.99% of documents)
- Channel operations per document: 2 (send job + receive result), on per-worker channels with minimal contention

#### Core Structs

```go
type indexWorker struct {
    id             int
    inbox          chan *indexJob
    dwpt           *DocumentsWriterPerThread
    fc             *FlushControl
    flushCh        chan<- *flushRequest
    quit           chan struct{}
    nameFunc       func() string
    fieldAnalyzers *analysis.FieldAnalyzers
    deleteQueue    *DeleteQueue
}

type indexJob struct {
    doc *document.Document
    err chan error // buffered, cap=1
}

type flushRequest struct {
    dwpt *DocumentsWriterPerThread
    done chan struct{} // closed when flush completes
}
```

#### Lock-Free FlushControl

Replace `FlushControl.doAfterDocument()` (mutex every document) with an atomic fast path:

```go
type FlushControl struct {
    activeBytes   atomic.Int64 // updated atomically by workers
    ramBufferSize int64
    stallLimit    int64

    mu         sync.Mutex // only for slow path (flush trigger + stall)
    flushBytes int64
    stalled    bool
    stallCond  *sync.Cond
}

// Fast path — called every document, no lock
func (fc *FlushControl) trackBytes(bytesAdded int64) bool {
    return fc.activeBytes.Add(bytesAdded) >= fc.ramBufferSize
}
```

Only when `trackBytes` returns true does the worker enter the slow path (acquire mutex, check again, enqueue flush). This is the double-checked locking pattern — the atomic check eliminates 99.99% of lock acquisitions.

`maxBufferedDocs` is checked locally by each worker (compare `dwpt.segment.docCount`) with no shared state.

#### Flush Coordination for Commit

`flushAllThreads()` sends a special flush job (with `doc == nil`) to each worker's inbox channel. Each worker:

1. Checks if its DWPT has documents
2. If yes: hands off the DWPT to the flush goroutine, creates a new DWPT
3. Signals completion via `sync.WaitGroup`

The caller waits on the WaitGroup until all handed-off DWPTs are flushed to disk.

#### Backpressure

When `activeBytes + flushBytes >= stallLimit` (2x ramBufferSize), new `AddDocument` calls block before sending to a worker. The stall check moves from inside FlushControl to the `DocumentsWriter.addDocument()` entry point, using the same `sync.Cond` pattern.

#### Delete Handling

No change to the DeleteQueue design. Each worker's DWPT has its own `DeleteSlice`, same as today. The global DeleteQueue remains shared and uses its existing dual-lock pattern (append lock + global buffer lock with TryLock).

### Phase 2: Parallel Merge in Commit

Run multiple merge candidates in parallel goroutines during `Commit()`:

- `Commit()` holds `w.mu` throughout (same as today), so only one commit at a time
- `FindMerges()` returns candidates, each executed in its own goroutine
- All merges complete before `segments_N` is written — guarantees clean commit point
- Segment list mutations are collected and applied after all merges complete (not mutated in-flight)
- Workers continue accepting documents during merge (they don't need `w.mu`)

## Files Changed

### Phase 1
- `index/dwpt_pool.go` → **delete** (replaced by worker model)
- `index/documents_writer.go` → **rewrite** (worker lifecycle, channel dispatch, flush coordination)
- `index/flush_control.go` → **rewrite** (atomic fast path, remove pool dependency)
- `index/dwpt.go` → minor changes (remove `checkoutGen`, `flushPending` fields that were pool-specific)
- `index/documents_writer_test.go` → **update** tests for new architecture

### Phase 2
- `index/writer.go` → modify `Commit()` and `executeMerge()` for parallel merge

## Delivery

| Phase | Scope | Expected Impact |
|-------|-------|-----------------|
| 1 | Worker model + atomic FlushControl | Eliminate 3 locks/doc → 0. Primary scaling fix. |
| 2 | Parallel merge in Commit | Reduce commit time at G=4+ when multiple merge candidates exist. |

Phase 1 addresses the root cause (lock contention). Phase 2 is additive. Each phase should be benchmarked independently before proceeding.
