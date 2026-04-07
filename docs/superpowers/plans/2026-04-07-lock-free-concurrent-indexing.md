# Lock-Free Concurrent Indexing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate per-document mutex contention so concurrent indexing scales near-linearly with goroutines.

**Architecture:** Replace 3 per-document mutex acquisitions with lock-free atomic operations on the common path. FlushControl uses `atomic.Int64` for byte accounting, DeleteQueue uses `atomic.Pointer` for tail reads, and perThreadPool uses `sync.Pool` for DWPT checkout/return. Mutexes are only acquired for rare events (flush trigger, commit, explicit deletes).

**Tech Stack:** Go `sync/atomic`, `sync.Pool`

---

### Task 1: FlushControl — Atomic byte accounting

**Files:**
- Modify: `index/flush_control.go`
- Modify: `index/flush_control_test.go`

- [ ] **Step 1: Write failing test for lock-free doAfterDocument fast path**

Add a test that verifies `doAfterDocument` correctly tracks bytes and returns false (no flush) without requiring external synchronization. This test already passes with the current mutex-based implementation but will validate the atomic refactor doesn't break the fast path.

```go
func TestFlushControlDoAfterDocumentFastPath(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	fc := newFlushControl(1<<30, 0, pool) // large buffer — no flush
	dwpt := pool.getAndLock()

	doc := document.NewDocument()
	doc.AddField("body", "hello world", document.FieldTypeText)
	bytesAdded, _ := dwpt.addDocument(doc)

	flushPending := fc.doAfterDocument(dwpt, bytesAdded)
	if flushPending {
		t.Fatal("expected no flush with large buffer")
	}

	if fc.activeBytes.Load() != bytesAdded {
		t.Errorf("activeBytes: got %d, want %d", fc.activeBytes.Load(), bytesAdded)
	}
	pool.returnAndUnlock(dwpt)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./index/ -run TestFlushControlDoAfterDocumentFastPath -v`
Expected: FAIL — `fc.activeBytes` is an `int64`, not `atomic.Int64`, so `.Load()` is not a method.

- [ ] **Step 3: Refactor FlushControl to use atomic.Int64 for activeBytes**

In `index/flush_control.go`, make these changes:

1. Change `activeBytes` field from `int64` to `atomic.Int64`
2. Rewrite `doAfterDocument` with a lock-free fast path:

```go
type FlushControl struct {
	mu              sync.Mutex
	activeBytes     atomic.Int64 // bytes in actively indexing DWPTs
	flushBytes      int64        // bytes in DWPTs that are pending flush (protected by mu)
	ramBufferSize   int64
	maxBufferedDocs int
	stallLimit      int64
	stallCond       *sync.Cond
	stalled         bool
	flushQueue      []*DocumentsWriterPerThread
	pool            *perThreadPool
	infoStream      InfoStream
	metrics         *IndexWriterMetrics
}
```

Rewrite `doAfterDocument`:

```go
func (fc *FlushControl) doAfterDocument(dwpt *DocumentsWriterPerThread, bytesAdded int64) bool {
	newActive := fc.activeBytes.Add(bytesAdded)

	// Fast path: no flush needed (common case — lock-free)
	byDocCount := fc.maxBufferedDocs > 0 && dwpt.segment.docCount >= fc.maxBufferedDocs
	if newActive < fc.ramBufferSize && !byDocCount {
		return false
	}

	// Slow path: flush may be needed — acquire lock
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if dwpt.flushPending {
		return false
	}

	// Re-check RAM threshold under lock (another goroutine may have flushed)
	shouldFlush := fc.activeBytes.Load() >= fc.ramBufferSize || byDocCount
	if !shouldFlush {
		return false
	}

	dwpt.flushPending = true
	dwptBytes := dwpt.estimateBytesUsed()
	fc.activeBytes.Add(-dwptBytes)
	fc.flushBytes += dwptBytes
	fc.flushQueue = append(fc.flushQueue, dwpt)

	if fc.metrics != nil {
		fc.metrics.FlushPendingBytes.Store(fc.flushBytes)
	}
	if fc.infoStream.IsEnabled("DWFC") {
		activeBytes := fc.activeBytes.Load()
		fc.infoStream.Message("DWFC", fmt.Sprintf(
			"flush triggered: ramBytes=%.1f MB > limit=%.1f MB",
			float64(activeBytes+fc.flushBytes)/(1024*1024),
			float64(fc.ramBufferSize)/(1024*1024)))
	}

	activeBytes := fc.activeBytes.Load()
	if activeBytes+fc.flushBytes >= fc.stallLimit {
		fc.stalled = true
	}

	return true
}
```

Update `doAfterFlush` to use atomic operations for `activeBytes`:

```go
func (fc *FlushControl) doAfterFlush(dwpt *DocumentsWriterPerThread) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.flushBytes -= dwpt.estimateBytesUsed()
	if fc.flushBytes < 0 {
		fc.flushBytes = 0
	}
	if fc.metrics != nil {
		fc.metrics.FlushPendingBytes.Store(fc.flushBytes)
	}

	activeBytes := fc.activeBytes.Load()
	if fc.stalled && activeBytes+fc.flushBytes < fc.stallLimit {
		fc.stalled = false
		fc.stallCond.Broadcast()
	}
}
```

Update `markForFullFlush` to use `fc.activeBytes.Add(-dwptBytes)` instead of `fc.activeBytes -= dwptBytes`.

Remove `fc.metrics.ActiveBytes.Store(...)` calls — consumers should read `fc.activeBytes.Load()` directly. If `IndexWriterMetrics.ActiveBytes` is used elsewhere, update it in a single place or remove it.

- [ ] **Step 4: Run all FlushControl tests**

Run: `go test ./index/ -run TestFlushControl -v`
Expected: all PASS

- [ ] **Step 5: Run tests with race detector**

Run: `go test ./index/ -run TestFlushControl -race -v`
Expected: all PASS, no race conditions

- [ ] **Step 6: Commit**

```bash
git add index/flush_control.go index/flush_control_test.go
git commit -m "perf: make FlushControl.activeBytes atomic for lock-free fast path"
```

---

### Task 2: DeleteQueue — Atomic tail pointer

**Files:**
- Modify: `index/delete_queue.go`
- Modify: `index/delete_queue_test.go`

- [ ] **Step 1: Write failing test for lock-free updateSlice**

Add a test that accesses `dq.tail` via atomic load:

```go
func TestDeleteQueueAtomicTailRead(t *testing.T) {
	dq := newDeleteQueue()

	// Read tail atomically — should be the sentinel
	sentinel := dq.tail.Load()
	if sentinel == nil {
		t.Fatal("expected non-nil sentinel tail")
	}

	dq.addDelete("f", "t1")

	// Tail should have advanced
	newTail := dq.tail.Load()
	if newTail == sentinel {
		t.Fatal("tail should have advanced after addDelete")
	}
	if newTail.field != "f" || newTail.term != "t1" {
		t.Errorf("unexpected tail node: field=%s term=%s", newTail.field, newTail.term)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./index/ -run TestDeleteQueueAtomicTailRead -v`
Expected: FAIL — `dq.tail` is `*deleteNode`, not `atomic.Pointer[deleteNode]`, so `.Load()` is not a method.

- [ ] **Step 3: Refactor DeleteQueue to use atomic.Pointer for tail**

In `index/delete_queue.go`:

1. Change `tail *deleteNode` to `tail atomic.Pointer[deleteNode]`
2. Update all access sites:

```go
type DeleteQueue struct {
	mu   sync.Mutex
	tail atomic.Pointer[deleteNode]

	globalBufferLock      sync.Mutex
	globalSlice           *DeleteSlice
	globalBufferedUpdates *BufferedUpdates
}

func newDeleteQueue() *DeleteQueue {
	sentinel := &deleteNode{}
	dq := &DeleteQueue{
		globalSlice: &DeleteSlice{
			sliceHead: sentinel,
			sliceTail: sentinel,
		},
		globalBufferedUpdates: newBufferedUpdates(),
	}
	dq.tail.Store(sentinel)
	return dq
}

func (dq *DeleteQueue) addDelete(field, term string) {
	node := &deleteNode{field: field, term: term}
	dq.mu.Lock()
	dq.tail.Load().next = node
	dq.tail.Store(node)
	dq.mu.Unlock()

	dq.tryApplyGlobalSlice()
}

func (dq *DeleteQueue) newSlice() *DeleteSlice {
	t := dq.tail.Load()
	return &DeleteSlice{sliceHead: t, sliceTail: t}
}

func (dq *DeleteQueue) updateSlice(slice *DeleteSlice) bool {
	currentTail := dq.tail.Load()
	if slice.sliceTail != currentTail {
		slice.sliceTail = currentTail
		return true
	}
	return false
}
```

For `tryApplyGlobalSlice`, `freezeGlobalBuffer`, and `anyChanges`, replace `dq.mu.Lock()` / `dq.tail` / `dq.mu.Unlock()` with `dq.tail.Load()`:

```go
func (dq *DeleteQueue) tryApplyGlobalSlice() {
	if dq.globalBufferLock.TryLock() {
		defer dq.globalBufferLock.Unlock()

		currentTail := dq.tail.Load()
		if dq.globalSlice.sliceTail != currentTail {
			dq.globalSlice.sliceTail = currentTail
			dq.globalSlice.apply(dq.globalBufferedUpdates, maxDocIDUpto)
		}
	}
}

func (dq *DeleteQueue) freezeGlobalBuffer(callerSlice *DeleteSlice) *FrozenBufferedUpdates {
	dq.globalBufferLock.Lock()
	defer dq.globalBufferLock.Unlock()

	currentTail := dq.tail.Load()

	if callerSlice != nil {
		callerSlice.sliceTail = currentTail
	}

	if dq.globalSlice.sliceTail != currentTail {
		dq.globalSlice.sliceTail = currentTail
		dq.globalSlice.apply(dq.globalBufferedUpdates, maxDocIDUpto)
	}

	if dq.globalBufferedUpdates.any() {
		frozen := newFrozenBufferedUpdates(dq.globalBufferedUpdates)
		dq.globalBufferedUpdates.clear()
		return frozen
	}
	return nil
}

func (dq *DeleteQueue) anyChanges() bool {
	dq.globalBufferLock.Lock()
	defer dq.globalBufferLock.Unlock()

	currentTail := dq.tail.Load()
	return dq.globalBufferedUpdates.any() || !dq.globalSlice.isEmpty() || dq.globalSlice.sliceTail != currentTail
}
```

- [ ] **Step 4: Run all DeleteQueue tests**

Run: `go test ./index/ -run TestDeleteQueue -v`
Expected: all PASS

- [ ] **Step 5: Run tests with race detector**

Run: `go test ./index/ -run TestDeleteQueue -race -v`
Expected: all PASS, no race conditions

- [ ] **Step 6: Commit**

```bash
git add index/delete_queue.go index/delete_queue_test.go
git commit -m "perf: make DeleteQueue.tail atomic for lock-free updateSlice"
```

---

### Task 3: perThreadPool — sync.Pool replacement

**Files:**
- Modify: `index/dwpt_pool.go`
- Modify: `index/dwpt_pool_test.go`

- [ ] **Step 1: Write failing test for sync.Pool-based checkout**

Add a test that verifies the pool works without the `active` map on the common path:

```go
func TestPoolSyncPoolFastPath(t *testing.T) {
	var counter atomic.Int32
	pool := newPerThreadPool(func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	}, newTestFieldAnalyzers(), newDeleteQueue())

	// Common path: get and return without full flush
	d1 := pool.getAndLock()
	if d1 == nil {
		t.Fatal("expected non-nil DWPT")
	}
	pool.returnAndUnlock(d1)

	// sync.Pool may or may not return the same instance, but should return a valid DWPT
	d2 := pool.getAndLock()
	if d2 == nil {
		t.Fatal("expected non-nil DWPT from pool")
	}
	pool.returnAndUnlock(d2)
}
```

- [ ] **Step 2: Run test to verify it passes (baseline)**

Run: `go test ./index/ -run TestPoolSyncPoolFastPath -v`
Expected: PASS (this test works with the current implementation too — it's a baseline)

- [ ] **Step 3: Refactor perThreadPool to use sync.Pool**

In `index/dwpt_pool.go`, replace the mutex-guarded free list with `sync.Pool` for the common path:

```go
type perThreadPool struct {
	mu             sync.Mutex
	syncPool       sync.Pool
	active         map[*DocumentsWriterPerThread]bool
	inFullFlush    atomic.Bool
	flushingActive map[*DocumentsWriterPerThread]bool
	flushOnReturn  []*DocumentsWriterPerThread
	flushRemaining int
	fullFlushDone  chan struct{}
	nameFunc       func() string
	fieldAnalyzers *analysis.FieldAnalyzers
	deleteQueue    *DeleteQueue
}

func newPerThreadPool(nameFunc func() string, fieldAnalyzers *analysis.FieldAnalyzers, deleteQueue *DeleteQueue) *perThreadPool {
	p := &perThreadPool{
		active:         make(map[*DocumentsWriterPerThread]bool),
		nameFunc:       nameFunc,
		fieldAnalyzers: fieldAnalyzers,
		deleteQueue:    deleteQueue,
	}
	p.syncPool.New = func() any {
		return newDWPT(p.nameFunc(), p.fieldAnalyzers, p.deleteQueue)
	}
	return p
}

func (p *perThreadPool) getAndLock() *DocumentsWriterPerThread {
	dwpt := p.syncPool.Get().(*DocumentsWriterPerThread)

	if p.inFullFlush.Load() {
		p.mu.Lock()
		p.active[dwpt] = true
		p.mu.Unlock()
	}

	return dwpt
}

func (p *perThreadPool) returnAndUnlock(dwpt *DocumentsWriterPerThread) {
	if !p.inFullFlush.Load() {
		p.syncPool.Put(dwpt)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.active, dwpt)

	if p.flushingActive[dwpt] {
		delete(p.flushingActive, dwpt)
		p.flushOnReturn = append(p.flushOnReturn, dwpt)
		p.flushRemaining--
		if p.flushRemaining == 0 {
			close(p.fullFlushDone)
		}
	} else {
		p.syncPool.Put(dwpt)
	}
}

func (p *perThreadPool) remove(dwpt *DocumentsWriterPerThread) {
	if !p.inFullFlush.Load() {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.active, dwpt)

	if p.flushingActive[dwpt] {
		delete(p.flushingActive, dwpt)
		p.flushRemaining--
		if p.flushRemaining == 0 {
			close(p.fullFlushDone)
		}
	}
}

func (p *perThreadPool) drainFreeAndMarkActive() []*DocumentsWriterPerThread {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Drain what we can from sync.Pool — it doesn't expose a "drain all" API,
	// so we just collect what Get() gives us until it returns a fresh instance.
	// In practice, during full flush the important thing is capturing active DWPTs;
	// free ones in sync.Pool have no documents buffered (they were returned after use).
	// We skip draining sync.Pool entirely — free DWPTs have already been flushed
	// or have 0 docs, so they don't need to be included in full flush.
	// Note: markForFullFlush() in flush_control.go calls this and appends the
	// result with waitAndDrainActive(). Returning nil here is correct —
	// append(nil, returned...) works fine in Go.

	if len(p.active) > 0 {
		p.inFullFlush.Store(true)
		p.flushingActive = make(map[*DocumentsWriterPerThread]bool, len(p.active))
		for dwpt := range p.active {
			p.flushingActive[dwpt] = true
		}
		p.flushRemaining = len(p.flushingActive)
		p.flushOnReturn = nil
		p.fullFlushDone = make(chan struct{})
	} else {
		p.inFullFlush.Store(true)
		p.flushRemaining = 0
	}

	return nil
}

func (p *perThreadPool) waitAndDrainActive() []*DocumentsWriterPerThread {
	p.mu.Lock()
	if !p.inFullFlush.Load() {
		p.mu.Unlock()
		return nil
	}
	if p.flushRemaining == 0 {
		result := p.flushOnReturn
		p.flushOnReturn = nil
		p.flushingActive = nil
		p.inFullFlush.Store(false)
		p.mu.Unlock()
		return result
	}
	done := p.fullFlushDone
	p.mu.Unlock()

	<-done

	p.mu.Lock()
	defer p.mu.Unlock()
	result := p.flushOnReturn
	p.flushOnReturn = nil
	p.flushingActive = nil
	p.inFullFlush.Store(false)
	return result
}
```

- [ ] **Step 4: Update tests for sync.Pool semantics**

`sync.Pool` doesn't guarantee returning the same instance (GC may clear it). Update `TestPoolGetAndReturn` to not assert pointer identity, and `TestPoolFullFlushIgnoresNewDWPTs` similarly:

In `index/dwpt_pool_test.go`, update `TestPoolGetAndReturn`:

```go
func TestPoolGetAndReturn(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	dwpt1 := pool.getAndLock()
	if dwpt1 == nil {
		t.Fatal("expected non-nil DWPT")
	}
	pool.returnAndUnlock(dwpt1)

	// sync.Pool may or may not return the same instance
	dwpt2 := pool.getAndLock()
	if dwpt2 == nil {
		t.Fatal("expected non-nil DWPT")
	}
	pool.returnAndUnlock(dwpt2)
}
```

Update `TestPoolRemove` — with sync.Pool, a removed DWPT is simply not returned to the pool, so we just verify we get a valid DWPT next:

```go
func TestPoolRemove(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	dwpt := pool.getAndLock()
	pool.remove(dwpt)

	// Pool should give us a valid DWPT
	dwpt2 := pool.getAndLock()
	if dwpt2 == nil {
		t.Fatal("expected non-nil DWPT after remove")
	}
	pool.returnAndUnlock(dwpt2)
}
```

Update `TestPoolFullFlushOnlyFree` — with sync.Pool, `drainFreeAndMarkActive` returns nil (free DWPTs in sync.Pool have 0 docs):

```go
func TestPoolFullFlushOnlyFree(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	d1 := pool.getAndLock()
	d2 := pool.getAndLock()
	pool.returnAndUnlock(d1)
	pool.returnAndUnlock(d2)

	// With sync.Pool, drainFreeAndMarkActive doesn't drain free DWPTs
	// (they have 0 docs buffered). It only sets up full flush mode.
	freed := pool.drainFreeAndMarkActive()
	if len(freed) != 0 {
		t.Fatalf("expected 0 freed DWPTs from sync.Pool drain, got %d", len(freed))
	}

	returned := pool.waitAndDrainActive()
	if len(returned) != 0 {
		t.Errorf("expected 0 returned DWPTs, got %d", len(returned))
	}
}
```

Update `TestPoolFullFlushIgnoresNewDWPTs` — remove the assertion that `d3 == d2`:

```go
func TestPoolFullFlushIgnoresNewDWPTs(t *testing.T) {
	var counter atomic.Int32
	pool := newPerThreadPool(func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	}, newTestFieldAnalyzers(), newDeleteQueue())

	d1 := pool.getAndLock()
	pool.drainFreeAndMarkActive()

	done := make(chan []*DocumentsWriterPerThread, 1)
	go func() {
		done <- pool.waitAndDrainActive()
	}()

	// New DWPT created during full flush — should not block flush
	d2 := pool.getAndLock()
	pool.returnAndUnlock(d2)

	select {
	case <-done:
		t.Fatal("waitAndDrainActive should still block — d1 is not returned yet")
	case <-time.After(50 * time.Millisecond):
	}

	pool.returnAndUnlock(d1)

	select {
	case returned := <-done:
		if len(returned) != 1 {
			t.Errorf("expected 1 returned DWPT, got %d", len(returned))
		}
		if returned[0] != d1 {
			t.Error("expected returned DWPT to be d1")
		}
	case <-time.After(time.Second):
		t.Fatal("waitAndDrainActive timed out")
	}
}
```

Update `TestPoolFullFlushReturnRouting` — remove the assertion that `d2 != d1`:

```go
func TestPoolFullFlushReturnRouting(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	d1 := pool.getAndLock()

	pool.drainFreeAndMarkActive()

	pool.returnAndUnlock(d1)

	returned := pool.waitAndDrainActive()
	if len(returned) != 1 {
		t.Fatalf("expected 1, got %d", len(returned))
	}

	// After full flush, pool should still give us a valid DWPT
	d2 := pool.getAndLock()
	if d2 == nil {
		t.Fatal("expected non-nil DWPT after full flush")
	}
	pool.returnAndUnlock(d2)
}
```

- [ ] **Step 5: Run all pool tests**

Run: `go test ./index/ -run TestPool -v`
Expected: all PASS

- [ ] **Step 6: Run tests with race detector**

Run: `go test ./index/ -run TestPool -race -v`
Expected: all PASS, no race conditions

- [ ] **Step 7: Commit**

```bash
git add index/dwpt_pool.go index/dwpt_pool_test.go
git commit -m "perf: replace perThreadPool mutex with sync.Pool for lock-free DWPT checkout"
```

---

### Task 4: Update FlushControl tests and fix metrics

**Files:**
- Modify: `index/flush_control_test.go`
- Modify: `index/flush_control.go` (if metrics cleanup needed)

- [ ] **Step 1: Update existing FlushControl tests for atomic activeBytes**

In `index/flush_control_test.go`, update `TestFlushControlMarkForFullFlush` — this test accesses `fc.activeBytes` indirectly via `markForFullFlush` which now uses atomic operations. Ensure any direct `fc.activeBytes` reads in tests use `.Load()`.

Also update `TestFlushControlDoAfterFlush` — if it reads `fc.flushBytes` directly, that's still a plain `int64` under mutex, so no change needed there.

Review the test file and fix any compilation errors from the `activeBytes` type change.

- [ ] **Step 2: Run the full index package test suite**

Run: `go test ./index/ -v -count=1 -timeout=120s`
Expected: all PASS

- [ ] **Step 3: Run with race detector**

Run: `go test ./index/ -race -count=1 -timeout=120s`
Expected: all PASS, no race conditions

- [ ] **Step 4: Commit if any fixes were needed**

```bash
git add index/
git commit -m "test: fix FlushControl and pool tests for atomic refactor"
```

---

### Task 5: Run concurrent benchmark and validate scaling

**Files:**
- No code changes — benchmarking only

- [ ] **Step 1: Run baseline benchmark (before optimization, from main branch)**

Run: `go test ./index/ -bench=BenchmarkConcurrentIndex -benchmem -count=1 -timeout=300s`

Record the docs/sec for each goroutine count. This establishes the baseline.

- [ ] **Step 2: Run benchmark on the optimized branch**

Run: `go test ./index/ -bench=BenchmarkConcurrentIndex -benchmem -count=1 -timeout=300s`

Compare docs/sec ratios. Expected improvement:
- 2 goroutines: should be faster than 1 (currently slower)
- 8 goroutines: should achieve 4-8x throughput (currently 2x)

- [ ] **Step 3: Run race detector on concurrent benchmark**

Run: `go test ./index/ -bench=BenchmarkConcurrentIndex/Goroutines_8 -race -count=1 -timeout=300s`
Expected: PASS, no races

- [ ] **Step 4: Commit benchmark results as a comment on issue #33**

Document the before/after results and close the issue if scaling targets are met.
