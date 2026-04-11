# Worker Model DWPT Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Lucene-style DWPT pool with Go-idiomatic worker goroutines and atomic FlushControl to eliminate lock contention (3 locks/doc to 0).

**Architecture:** Fixed worker goroutines (GOMAXPROCS count) each own a DWPT. Documents are dispatched via per-worker channels with round-robin selection. FlushControl uses atomic fast path for byte tracking, only acquiring a mutex when flush is actually triggered. Workers hand off DWPTs for flush to a dedicated flush goroutine and immediately resume with a fresh DWPT.

**Tech Stack:** Go stdlib (`sync/atomic`, `sync`, `runtime`, channels)

**Spec:** `docs/superpowers/specs/2026-04-11-worker-model-dwpt-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `index/flush_control.go` | Rewrite | Atomic fast path + mutex slow path for flush decisions and backpressure |
| `index/flush_control_test.go` | Rewrite | Tests for new FlushControl (atomic tracking, slow path, stall/unstall) |
| `index/index_worker.go` | Create | Worker goroutine, inbox channel, flush hand-off |
| `index/index_worker_test.go` | Create | Tests for worker lifecycle, document processing, flush trigger |
| `index/documents_writer.go` | Rewrite | Worker lifecycle, channel dispatch, flush goroutine, flush coordination |
| `index/documents_writer_test.go` | Rewrite | Tests for new DocumentsWriter (single/concurrent add, flush queue, deletes) |
| `index/dwpt.go` | Modify | Remove pool-specific fields (`checkoutGen`, `flushPending`) |
| `index/dwpt_pool.go` | Delete | Replaced entirely by worker model |
| `index/dwpt_pool_test.go` | Delete | Pool tests no longer applicable |
| `index/writer.go` | Modify | Add `Close` call to `docWriter`, parallel merge in `Commit` |

---

### Task 1: Rewrite FlushControl with Atomic Fast Path

The new FlushControl uses `atomic.Int64` for byte tracking. The fast path (called every document) is a single atomic add. The slow path (mutex) is only entered when the threshold is crossed.

**Files:**
- Modify: `index/flush_control.go`
- Modify: `index/flush_control_test.go`

- [ ] **Step 1: Write failing tests for the new FlushControl**

Replace the contents of `index/flush_control_test.go` with:

```go
package index

import (
	"testing"
	"time"
)

func TestFlushControlTrackBytesBelowThreshold(t *testing.T) {
	fc := newFlushControl(1000, 0)

	// Adding bytes below threshold should not trigger flush
	if fc.trackBytes(500) {
		t.Error("expected trackBytes to return false when below threshold")
	}
	if fc.activeBytes.Load() != 500 {
		t.Errorf("expected activeBytes=500, got %d", fc.activeBytes.Load())
	}
}

func TestFlushControlTrackBytesExceedsThreshold(t *testing.T) {
	fc := newFlushControl(1000, 0)

	// Adding bytes that exceed threshold should trigger flush
	if !fc.trackBytes(1500) {
		t.Error("expected trackBytes to return true when exceeding threshold")
	}
}

func TestFlushControlEnterSlowPath(t *testing.T) {
	fc := newFlushControl(100, 0)

	// Exceed threshold
	fc.trackBytes(200)

	// Slow path should transfer bytes from active to flush
	dwptBytes := int64(200)
	fc.enterSlowPath(dwptBytes)

	if fc.activeBytes.Load() != 0 {
		t.Errorf("expected activeBytes=0 after enterSlowPath, got %d", fc.activeBytes.Load())
	}

	fc.mu.Lock()
	fb := fc.flushBytes
	fc.mu.Unlock()
	if fb != 200 {
		t.Errorf("expected flushBytes=200, got %d", fb)
	}
}

func TestFlushControlDoAfterFlush(t *testing.T) {
	fc := newFlushControl(100, 0)

	fc.trackBytes(200)
	fc.enterSlowPath(200)

	fc.doAfterFlush(200)

	fc.mu.Lock()
	fb := fc.flushBytes
	fc.mu.Unlock()
	if fb != 0 {
		t.Errorf("expected flushBytes=0 after doAfterFlush, got %d", fb)
	}
}

func TestFlushControlStallAndUnstall(t *testing.T) {
	// ramBufferSize=50, stallLimit=100
	fc := newFlushControl(50, 0)

	// Exceed stall limit
	fc.trackBytes(150)
	fc.enterSlowPath(150)

	fc.mu.Lock()
	stalled := fc.stalled
	fc.mu.Unlock()
	if !stalled {
		t.Fatal("expected stalled after exceeding stallLimit")
	}

	// waitIfStalled should block
	done := make(chan struct{})
	go func() {
		fc.waitIfStalled()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("should be stalled but wasn't")
	case <-time.After(50 * time.Millisecond):
	}

	// Flush to unstall
	fc.doAfterFlush(150)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("should have unstalled after flush")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search && go test ./index/ -run "TestFlushControl" -v`

Expected: compilation errors because `newFlushControl` signature changed, `trackBytes` and `enterSlowPath` don't exist yet.

- [ ] **Step 3: Implement the new FlushControl**

Replace the contents of `index/flush_control.go` with:

```go
package index

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// FlushControl tracks RAM usage across all workers and decides when to flush.
// The fast path (trackBytes) uses a single atomic add — no lock.
// The slow path (enterSlowPath) acquires a mutex only when flush is triggered.
type FlushControl struct {
	activeBytes   atomic.Int64 // updated atomically by workers — fast path
	ramBufferSize int64
	maxBufferedDocs int
	stallLimit    int64 // 2x ramBufferSize

	mu         sync.Mutex // slow path only: flush trigger + stall
	flushBytes int64
	stalled    bool
	stallCond  *sync.Cond
	infoStream InfoStream
	metrics    *IndexWriterMetrics
}

func newFlushControl(ramBufferSize int64, maxBufferedDocs int) *FlushControl {
	fc := &FlushControl{
		ramBufferSize:   ramBufferSize,
		maxBufferedDocs: maxBufferedDocs,
		stallLimit:      2 * ramBufferSize,
		infoStream:      &NoOpInfoStream{},
	}
	fc.stallCond = sync.NewCond(&fc.mu)
	return fc
}

// trackBytes atomically adds bytes and returns true if the RAM threshold
// is exceeded. This is the fast path — called every document, no lock.
func (fc *FlushControl) trackBytes(bytesAdded int64) bool {
	return fc.activeBytes.Add(bytesAdded) >= fc.ramBufferSize
}

// enterSlowPath transfers dwptBytes from active to flush accounting and
// checks whether backpressure should be applied. Called only when
// trackBytes returns true (rare).
func (fc *FlushControl) enterSlowPath(dwptBytes int64) {
	fc.activeBytes.Add(-dwptBytes)

	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.flushBytes += dwptBytes
	if fc.metrics != nil {
		fc.metrics.FlushPendingBytes.Store(fc.flushBytes)
		fc.metrics.ActiveBytes.Store(fc.activeBytes.Load())
	}
	if fc.infoStream.IsEnabled("DWFC") {
		fc.infoStream.Message("DWFC", fmt.Sprintf(
			"flush triggered: activeBytes=%.1f MB, flushBytes=%.1f MB, limit=%.1f MB",
			float64(fc.activeBytes.Load())/(1024*1024),
			float64(fc.flushBytes)/(1024*1024),
			float64(fc.ramBufferSize)/(1024*1024)))
	}

	if fc.activeBytes.Load()+fc.flushBytes >= fc.stallLimit {
		fc.stalled = true
	}
}

// doAfterFlush decrements flushBytes and potentially unstalls blocked goroutines.
func (fc *FlushControl) doAfterFlush(dwptBytes int64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.flushBytes -= dwptBytes
	if fc.flushBytes < 0 {
		fc.flushBytes = 0
	}
	if fc.metrics != nil {
		fc.metrics.FlushPendingBytes.Store(fc.flushBytes)
	}

	if fc.stalled && fc.activeBytes.Load()+fc.flushBytes < fc.stallLimit {
		fc.stalled = false
		fc.stallCond.Broadcast()
	}
}

// waitIfStalled blocks the calling goroutine if backpressure is active.
func (fc *FlushControl) waitIfStalled() {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if !fc.stalled {
		return
	}

	if fc.metrics != nil {
		fc.metrics.StallCount.Add(1)
	}
	if fc.infoStream.IsEnabled("DW") {
		fc.infoStream.Message("DW", fmt.Sprintf(
			"now stalling: activeBytes=%.1f MB flushBytes=%.1f MB",
			float64(fc.activeBytes.Load())/(1024*1024),
			float64(fc.flushBytes)/(1024*1024)))
	}

	start := time.Now()
	for fc.stalled {
		fc.stallCond.Wait()
	}
	elapsed := time.Since(start)

	if fc.metrics != nil {
		fc.metrics.StallTimeNanos.Add(elapsed.Nanoseconds())
	}
	if fc.infoStream.IsEnabled("DW") {
		fc.infoStream.Message("DW", fmt.Sprintf("done stalling for %.1f ms", float64(elapsed.Nanoseconds())/1e6))
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search && go test ./index/ -run "TestFlushControl" -v`

Expected: all 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add index/flush_control.go index/flush_control_test.go
git commit -m "refactor: rewrite FlushControl with atomic fast path

trackBytes() uses atomic.Int64 — no lock on the hot path.
enterSlowPath() only called when RAM threshold exceeded (~0.01% of docs)."
```

---

### Task 2: Create indexWorker

The worker goroutine owns a DWPT, reads jobs from its inbox channel, and hands off DWPTs for flush.

**Files:**
- Create: `index/index_worker.go`
- Create: `index/index_worker_test.go`

- [ ] **Step 1: Write failing tests for indexWorker**

Create `index/index_worker_test.go`:

```go
package index

import (
	"fmt"
	"sync/atomic"
	"testing"

	"gosearch/document"
	"gosearch/store"
)

func TestWorkerProcessesDocument(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	fc := newFlushControl(1<<30, 0) // large buffer, no flush
	var counter atomic.Int32
	nameFunc := func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	}
	dq := newDeleteQueue()
	fa := newTestFieldAnalyzers()
	tq := newFlushTicketQueue()

	w := newIndexWorker(0, fc, dir, nameFunc, fa, dq, tq, nil, nil)
	w.start()
	defer w.stop()

	doc := document.NewDocument()
	doc.AddField("body", "hello world", document.FieldTypeText)

	err := w.addDocument(doc)
	if err != nil {
		t.Fatal(err)
	}

	if fc.activeBytes.Load() <= 0 {
		t.Error("expected activeBytes > 0 after adding a document")
	}
}

func TestWorkerTriggersFlush(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	fc := newFlushControl(200, 0) // small buffer to trigger flush
	var counter atomic.Int32
	nameFunc := func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	}
	dq := newDeleteQueue()
	fa := newTestFieldAnalyzers()
	tq := newFlushTicketQueue()

	var flushedCount atomic.Int32
	onFlushed := func(info *SegmentCommitInfo) {
		flushedCount.Add(1)
	}

	w := newIndexWorker(0, fc, dir, nameFunc, fa, dq, tq, onFlushed, nil)
	w.start()
	defer w.stop()

	for i := range 20 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("document %d with enough text to trigger flush eventually", i), document.FieldTypeText)
		if err := w.addDocument(doc); err != nil {
			t.Fatal(err)
		}
	}

	if flushedCount.Load() == 0 {
		t.Error("expected at least one flush to be triggered")
	}
}

func TestWorkerFlushAll(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	fc := newFlushControl(1<<30, 0) // large buffer, no auto flush
	var counter atomic.Int32
	nameFunc := func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	}
	dq := newDeleteQueue()
	fa := newTestFieldAnalyzers()
	tq := newFlushTicketQueue()

	var flushedCount atomic.Int32
	onFlushed := func(info *SegmentCommitInfo) {
		flushedCount.Add(1)
	}

	w := newIndexWorker(0, fc, dir, nameFunc, fa, dq, tq, onFlushed, nil)
	w.start()
	defer w.stop()

	for i := range 5 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("doc %d", i), document.FieldTypeText)
		w.addDocument(doc)
	}

	w.flushAll()

	if flushedCount.Load() != 1 {
		t.Errorf("expected 1 flush after flushAll, got %d", flushedCount.Load())
	}
}

func TestWorkerMaxBufferedDocs(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	fc := newFlushControl(1<<30, 3) // flush after 3 docs
	var counter atomic.Int32
	nameFunc := func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	}
	dq := newDeleteQueue()
	fa := newTestFieldAnalyzers()
	tq := newFlushTicketQueue()

	var flushedCount atomic.Int32
	onFlushed := func(info *SegmentCommitInfo) {
		flushedCount.Add(1)
	}

	w := newIndexWorker(0, fc, dir, nameFunc, fa, dq, tq, onFlushed, nil)
	w.start()
	defer w.stop()

	for i := range 6 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("doc %d", i), document.FieldTypeText)
		w.addDocument(doc)
	}

	// 6 docs with maxBufferedDocs=3 should trigger 2 flushes
	if flushedCount.Load() < 2 {
		t.Errorf("expected at least 2 flushes with maxBufferedDocs=3, got %d", flushedCount.Load())
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search && go test ./index/ -run "TestWorker" -v`

Expected: compilation errors because `newIndexWorker` doesn't exist.

- [ ] **Step 3: Implement indexWorker**

Create `index/index_worker.go`:

```go
package index

import (
	"fmt"
	"time"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

// indexJob represents a document to be indexed by a worker.
type indexJob struct {
	doc *document.Document
	err chan error // buffered, cap=1
}

// flushJob is a sentinel job that tells the worker to flush its DWPT.
// The worker signals completion by closing done.
type flushJob struct {
	done chan struct{}
}

// indexWorker owns a single DWPT and processes documents from its inbox.
// When a flush is triggered, the worker hands off its DWPT, creates a
// fresh one, and resumes accepting documents immediately.
type indexWorker struct {
	id    int
	inbox chan any // *indexJob or *flushJob

	dwpt           *DocumentsWriterPerThread
	fc             *FlushControl
	dir            store.Directory
	nameFunc       func() string
	fieldAnalyzers *analysis.FieldAnalyzers
	deleteQueue    *DeleteQueue
	ticketQueue    *FlushTicketQueue
	infoStream     InfoStream
	metrics        *IndexWriterMetrics

	onSegmentFlushed func(info *SegmentCommitInfo)
	onGlobalUpdates  func(updates *FrozenBufferedUpdates)

	quit chan struct{}
	stopped chan struct{}
}

func newIndexWorker(
	id int,
	fc *FlushControl,
	dir store.Directory,
	nameFunc func() string,
	fieldAnalyzers *analysis.FieldAnalyzers,
	deleteQueue *DeleteQueue,
	ticketQueue *FlushTicketQueue,
	onSegmentFlushed func(*SegmentCommitInfo),
	onGlobalUpdates func(*FrozenBufferedUpdates),
) *indexWorker {
	return &indexWorker{
		id:               id,
		inbox:            make(chan any, 64),
		fc:               fc,
		dir:              dir,
		nameFunc:         nameFunc,
		fieldAnalyzers:   fieldAnalyzers,
		deleteQueue:      deleteQueue,
		ticketQueue:      ticketQueue,
		onSegmentFlushed: onSegmentFlushed,
		onGlobalUpdates:  onGlobalUpdates,
		infoStream:       &NoOpInfoStream{},
		quit:             make(chan struct{}),
		stopped:          make(chan struct{}),
	}
}

// start launches the worker goroutine.
func (w *indexWorker) start() {
	w.dwpt = newDWPT(w.nameFunc(), w.fieldAnalyzers, w.deleteQueue)
	go w.run()
}

// stop signals the worker to shut down and waits for it to finish.
func (w *indexWorker) stop() {
	close(w.quit)
	<-w.stopped
}

// addDocument sends a document to the worker and waits for the result.
func (w *indexWorker) addDocument(doc *document.Document) error {
	job := &indexJob{
		doc: doc,
		err: make(chan error, 1),
	}
	w.inbox <- job
	return <-job.err
}

// flushAll tells the worker to flush its current DWPT and waits for completion.
func (w *indexWorker) flushAll() {
	job := &flushJob{done: make(chan struct{})}
	w.inbox <- job
	<-job.done
}

func (w *indexWorker) run() {
	defer close(w.stopped)
	for {
		select {
		case msg := <-w.inbox:
			switch j := msg.(type) {
			case *indexJob:
				j.err <- w.processDocument(j.doc)
			case *flushJob:
				w.doFlushAll()
				close(j.done)
			}
		case <-w.quit:
			// Drain remaining jobs
			for {
				select {
				case msg := <-w.inbox:
					switch j := msg.(type) {
					case *indexJob:
						j.err <- w.processDocument(j.doc)
					case *flushJob:
						w.doFlushAll()
						close(j.done)
					}
				default:
					return
				}
			}
		}
	}
}

func (w *indexWorker) processDocument(doc *document.Document) error {
	bytesAdded, err := w.dwpt.addDocument(doc)
	if err != nil {
		return err
	}
	if w.metrics != nil {
		w.metrics.DocsAdded.Add(1)
		w.metrics.ActiveBytes.Store(w.fc.activeBytes.Load())
	}

	shouldFlush := w.fc.trackBytes(bytesAdded)
	if !shouldFlush && w.fc.maxBufferedDocs > 0 {
		shouldFlush = w.dwpt.segment.docCount >= w.fc.maxBufferedDocs
	}

	if shouldFlush {
		w.doFlush()
	}

	return nil
}

// doFlush hands off the current DWPT for flushing and creates a fresh one.
func (w *indexWorker) doFlush() {
	dwpt := w.dwpt
	dwptBytes := dwpt.estimateBytesUsed()

	// Transfer bytes from active to flush accounting
	w.fc.enterSlowPath(dwptBytes)

	// Create fresh DWPT immediately so the worker can resume
	w.dwpt = newDWPT(w.nameFunc(), w.fieldAnalyzers, w.deleteQueue)

	// Flush the old DWPT
	w.executeDWPTFlush(dwpt, dwptBytes)
}

// doFlushAll flushes the current DWPT if it has documents.
func (w *indexWorker) doFlushAll() {
	if w.dwpt.segment.docCount == 0 {
		return
	}

	dwpt := w.dwpt
	dwptBytes := dwpt.estimateBytesUsed()

	// Transfer active bytes to flush
	w.fc.activeBytes.Add(-dwptBytes)
	w.fc.mu.Lock()
	w.fc.flushBytes += dwptBytes
	w.fc.mu.Unlock()

	// Create fresh DWPT
	w.dwpt = newDWPT(w.nameFunc(), w.fieldAnalyzers, w.deleteQueue)

	// Flush the old DWPT
	w.executeDWPTFlush(dwpt, dwptBytes)
}

func (w *indexWorker) executeDWPTFlush(dwpt *DocumentsWriterPerThread, dwptBytes int64) {
	ticket := w.ticketQueue.addTicket()
	globalUpdates := dwpt.prepareFlush()

	start := time.Now()
	info, err := dwpt.flush(w.dir)
	elapsed := time.Since(start)

	w.fc.doAfterFlush(dwptBytes)
	w.ticketQueue.markDone(ticket, info, globalUpdates, err)

	if w.metrics != nil {
		w.metrics.FlushCount.Add(1)
		w.metrics.FlushTimeNanos.Add(elapsed.Nanoseconds())
		if info != nil {
			w.metrics.FlushBytes.Add(dwptBytes)
		}
	}
	if w.infoStream.IsEnabled("DWPT") && info != nil {
		w.infoStream.Message("DWPT", fmt.Sprintf(
			"flush %s: %d docs, %.1f MB, took %dms",
			info.Name, info.MaxDoc,
			float64(dwptBytes)/(1024*1024),
			elapsed.Milliseconds()))
	}

	// Publish completed tickets
	published := w.ticketQueue.publishCompleted()
	for _, t := range published {
		if t.err != nil {
			continue
		}
		if t.globalUpdates != nil && t.globalUpdates.any() && w.onGlobalUpdates != nil {
			w.onGlobalUpdates(t.globalUpdates)
		}
		if t.result != nil && w.onSegmentFlushed != nil {
			w.onSegmentFlushed(t.result)
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search && go test ./index/ -run "TestWorker" -v`

Expected: all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add index/index_worker.go index/index_worker_test.go
git commit -m "feat: add indexWorker with per-worker DWPT ownership

Each worker owns a DWPT, processes docs from its inbox channel,
and hands off for flush without blocking other workers."
```

---

### Task 3: Rewrite DocumentsWriter with Worker Dispatch

Replace pool-based DocumentsWriter with worker dispatch model.

**Files:**
- Modify: `index/documents_writer.go`

- [ ] **Step 1: Rewrite DocumentsWriter**

Replace the contents of `index/documents_writer.go` with:

```go
package index

import (
	"runtime"
	"sync"
	"sync/atomic"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

// DocumentsWriter coordinates concurrent document indexing using worker
// goroutines. Each worker owns a DWPT and processes documents from its
// inbox channel. This eliminates the pool checkout/return lock contention
// of the previous design.
type DocumentsWriter struct {
	workers     []*indexWorker
	next        atomic.Uint64 // round-robin counter
	fc          *FlushControl
	ticketQueue *FlushTicketQueue
	deleteQueue *DeleteQueue
	dir         store.Directory
	infoStream  InfoStream
	metrics     *IndexWriterMetrics

	onSegmentFlushed func(info *SegmentCommitInfo)
	onGlobalUpdates  func(updates *FrozenBufferedUpdates)

	startOnce sync.Once
	stopOnce  sync.Once
}

func newDocumentsWriter(dir store.Directory, fieldAnalyzers *analysis.FieldAnalyzers, ramBufferSize int64, maxBufferedDocs int, nameFunc func() string) *DocumentsWriter {
	deleteQueue := newDeleteQueue()
	fc := newFlushControl(ramBufferSize, maxBufferedDocs)
	ticketQueue := newFlushTicketQueue()

	dw := &DocumentsWriter{
		fc:          fc,
		ticketQueue: ticketQueue,
		deleteQueue: deleteQueue,
		dir:         dir,
		infoStream:  &NoOpInfoStream{},
	}

	numWorkers := runtime.GOMAXPROCS(0)
	dw.workers = make([]*indexWorker, numWorkers)
	for i := range numWorkers {
		dw.workers[i] = newIndexWorker(
			i, fc, dir, nameFunc, fieldAnalyzers, deleteQueue, ticketQueue,
			func(info *SegmentCommitInfo) {
				if dw.onSegmentFlushed != nil {
					dw.onSegmentFlushed(info)
				}
			},
			func(updates *FrozenBufferedUpdates) {
				if dw.onGlobalUpdates != nil {
					dw.onGlobalUpdates(updates)
				}
			},
		)
	}

	return dw
}

// ensureStarted lazily starts workers on first use.
func (dw *DocumentsWriter) ensureStarted() {
	dw.startOnce.Do(func() {
		for _, w := range dw.workers {
			w.start()
		}
	})
}

// setInfoStream sets the InfoStream for diagnostic logging.
func (dw *DocumentsWriter) setInfoStream(infoStream InfoStream) {
	dw.infoStream = infoStream
	dw.fc.infoStream = infoStream
	for _, w := range dw.workers {
		w.infoStream = infoStream
	}
}

// addDocument indexes a document concurrently. The caller does not need
// to hold any lock.
func (dw *DocumentsWriter) addDocument(doc *document.Document) error {
	dw.ensureStarted()
	dw.fc.waitIfStalled()

	// Round-robin dispatch to workers
	idx := dw.next.Add(1) % uint64(len(dw.workers))
	return dw.workers[idx].addDocument(doc)
}

// flushAllThreads flushes all worker DWPTs. Called during commit/NRT.
func (dw *DocumentsWriter) flushAllThreads() error {
	dw.ensureStarted()

	// Send flush signal to all workers in parallel
	var wg sync.WaitGroup
	for _, w := range dw.workers {
		wg.Add(1)
		go func(w *indexWorker) {
			defer wg.Done()
			w.flushAll()
		}(w)
	}
	wg.Wait()

	return nil
}

// deleteDocuments buffers a delete-by-term operation.
func (dw *DocumentsWriter) deleteDocuments(field, term string) {
	dw.deleteQueue.addDelete(field, term)
}

// freezeGlobalBuffer freezes and returns the global delete buffer.
func (dw *DocumentsWriter) freezeGlobalBuffer() *FrozenBufferedUpdates {
	return dw.deleteQueue.freezeGlobalBuffer(nil)
}

// close stops all workers. Must be called when done.
func (dw *DocumentsWriter) close() {
	dw.stopOnce.Do(func() {
		for _, w := range dw.workers {
			w.stop()
		}
	})
}
```

- [ ] **Step 2: Verify compilation**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search && go build ./index/`

Expected: compilation succeeds (tests may still reference old APIs, we'll fix those next).

- [ ] **Step 3: Commit**

```bash
git add index/documents_writer.go
git commit -m "refactor: rewrite DocumentsWriter with worker dispatch

Replace pool checkout/return with round-robin worker dispatch.
Each worker owns its DWPT — zero lock contention per document."
```

---

### Task 4: Clean Up DWPT and Remove Pool

Remove pool-specific fields from DWPT and delete the pool files.

**Files:**
- Modify: `index/dwpt.go`
- Delete: `index/dwpt_pool.go`
- Delete: `index/dwpt_pool_test.go`

- [ ] **Step 1: Remove pool-specific fields from DWPT**

In `index/dwpt.go`, remove the `checkoutGen` and `flushPending` fields from the struct and `reset` method:

Remove from the struct:
```go
	flushPending   bool
	checkoutGen    int64 // pool generation when this DWPT was checked out
```

Remove the `reset` method entirely (workers create fresh DWPTs instead of resetting).

Remove `flushPending = false` from the `reset` method body (since the whole method is removed).

- [ ] **Step 2: Delete pool files**

```bash
rm index/dwpt_pool.go index/dwpt_pool_test.go
```

- [ ] **Step 3: Verify compilation**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search && go build ./index/`

Expected: compilation succeeds.

- [ ] **Step 4: Commit**

```bash
git add -u index/dwpt.go index/dwpt_pool.go index/dwpt_pool_test.go
git commit -m "refactor: remove DWPT pool and pool-specific fields

Pool is replaced by worker model. checkoutGen, flushPending, and
reset() are no longer needed — workers create fresh DWPTs on flush."
```

---

### Task 5: Update DocumentsWriter Tests

Rewrite tests to validate the worker-based architecture.

**Files:**
- Modify: `index/documents_writer_test.go`

- [ ] **Step 1: Rewrite DocumentsWriter tests**

Replace the contents of `index/documents_writer_test.go` with:

```go
package index

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"gosearch/document"
	"gosearch/store"
)

func TestDocumentsWriterAddDocument(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	var counter atomic.Int32
	var segments []*SegmentCommitInfo
	var mu sync.Mutex

	dw := newDocumentsWriter(dir, newTestFieldAnalyzers(), 500, 0, func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	})
	defer dw.close()
	dw.onSegmentFlushed = func(info *SegmentCommitInfo) {
		mu.Lock()
		segments = append(segments, info)
		mu.Unlock()
	}

	for i := range 20 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("document number %d with some text to generate bytes", i), document.FieldTypeText)
		if err := dw.addDocument(doc); err != nil {
			t.Fatal(err)
		}
	}

	if err := dw.flushAllThreads(); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	totalDocs := 0
	for _, info := range segments {
		totalDocs += info.MaxDoc
	}
	mu.Unlock()

	if totalDocs != 20 {
		t.Errorf("expected 20 total docs across segments, got %d", totalDocs)
	}
}

func TestDocumentsWriterConcurrentAdd(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	var counter atomic.Int32
	var segments []*SegmentCommitInfo
	var mu sync.Mutex

	dw := newDocumentsWriter(dir, newTestFieldAnalyzers(), 2000, 0, func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	})
	defer dw.close()
	dw.onSegmentFlushed = func(info *SegmentCommitInfo) {
		mu.Lock()
		segments = append(segments, info)
		mu.Unlock()
	}

	const goroutines = 8
	const docsPerGoroutine = 500
	var wg sync.WaitGroup

	for g := range goroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range docsPerGoroutine {
				doc := document.NewDocument()
				doc.AddField("body", fmt.Sprintf("goroutine %d document %d text content", gid, i), document.FieldTypeText)
				if err := dw.addDocument(doc); err != nil {
					t.Errorf("addDocument error: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if err := dw.flushAllThreads(); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	totalDocs := 0
	for _, info := range segments {
		totalDocs += info.MaxDoc
	}
	mu.Unlock()

	expected := goroutines * docsPerGoroutine
	if totalDocs != expected {
		t.Errorf("expected %d total docs, got %d", expected, totalDocs)
	}
}

func TestDocumentsWriterFlushAllThreads(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	var counter atomic.Int32
	var segments []*SegmentCommitInfo
	var mu sync.Mutex

	dw := newDocumentsWriter(dir, newTestFieldAnalyzers(), 1<<30, 0, func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	})
	defer dw.close()
	dw.onSegmentFlushed = func(info *SegmentCommitInfo) {
		mu.Lock()
		segments = append(segments, info)
		mu.Unlock()
	}

	for i := range 5 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("doc %d", i), document.FieldTypeText)
		dw.addDocument(doc)
	}

	if err := dw.flushAllThreads(); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	totalDocs := 0
	for _, info := range segments {
		totalDocs += info.MaxDoc
	}
	mu.Unlock()

	if totalDocs != 5 {
		t.Errorf("expected 5 docs after flushAllThreads, got %d", totalDocs)
	}
}

func TestDocumentsWriterDeleteDocuments(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	var counter atomic.Int32

	dw := newDocumentsWriter(dir, newTestFieldAnalyzers(), 1<<30, 0, func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	})
	defer dw.close()

	doc := document.NewDocument()
	doc.AddField("id", "1", document.FieldTypeKeyword)
	doc.AddField("body", "hello world", document.FieldTypeText)
	dw.addDocument(doc)

	dw.deleteDocuments("id", "1")

	if !dw.deleteQueue.anyChanges() {
		t.Fatal("expected delete queue to have changes after deleteDocuments")
	}

	frozen := dw.freezeGlobalBuffer()
	if frozen == nil {
		t.Fatal("expected non-nil frozen buffered updates")
	}
	if len(frozen.deleteTerms) != 1 {
		t.Fatalf("expected 1 frozen delete term, got %d", len(frozen.deleteTerms))
	}
	if frozen.deleteTerms[0].Field != "id" || frozen.deleteTerms[0].Term != "1" {
		t.Errorf("unexpected frozen delete term: Field=%q Term=%q", frozen.deleteTerms[0].Field, frozen.deleteTerms[0].Term)
	}

	frozen2 := dw.freezeGlobalBuffer()
	if frozen2 != nil {
		t.Errorf("expected nil frozen updates after second freeze, got %+v", frozen2)
	}
}

func TestDoFlush_NilsSegment(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := newTestFieldAnalyzers()
	dq := newDeleteQueue()
	dwpt := newDWPT("_seg0", fa, dq)

	for i := range 5 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("word%d", i), document.FieldTypeText)
		dwpt.addDocument(doc)
	}

	if dwpt.segment == nil {
		t.Fatal("segment should not be nil before flush")
	}
	if dwpt.segment.docCount != 5 {
		t.Fatalf("expected 5 docs, got %d", dwpt.segment.docCount)
	}

	_, err = dwpt.flush(dir)
	if err != nil {
		t.Fatal(err)
	}

	if dwpt.segment != nil {
		t.Error("segment should be nil after flush to release memory")
	}
}
```

- [ ] **Step 2: Run all tests**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search && go test ./index/ -run "TestDocumentsWriter|TestDoFlush" -v -count=1`

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add index/documents_writer_test.go
git commit -m "test: update DocumentsWriter tests for worker model

Remove pool-specific tests (flush queue drain, pool active count).
Add defer dw.close() for proper worker shutdown."
```

---

### Task 6: Update IndexWriter for Worker Lifecycle

Wire up `docWriter.close()` in `IndexWriter.Close()` and set metrics on workers.

**Files:**
- Modify: `index/writer.go`

- [ ] **Step 1: Update IndexWriter.Close to stop workers**

In `index/writer.go`, update the `Close` method to call `dw.close()`:

Find:
```go
func (w *IndexWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for name, rau := range w.readerMap {
		rau.Close()
		delete(w.readerMap, name)
	}
	return nil
}
```

Replace with:
```go
func (w *IndexWriter) Close() error {
	w.docWriter.close()
	w.mu.Lock()
	defer w.mu.Unlock()
	for name, rau := range w.readerMap {
		rau.Close()
		delete(w.readerMap, name)
	}
	return nil
}
```

- [ ] **Step 2: Update NewIndexWriter to set metrics on FlushControl and workers**

In `index/writer.go`, find the block after `w.docWriter = newDocumentsWriter(...)`:

Find:
```go
	w.docWriter.metrics = w.metrics
	w.docWriter.flushControl.metrics = w.metrics
```

Replace with:
```go
	w.docWriter.metrics = w.metrics
	w.docWriter.fc.metrics = w.metrics
	for _, worker := range w.docWriter.workers {
		worker.metrics = w.metrics
	}
```

- [ ] **Step 3: Run the full test suite**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search && go test ./index/ -v -count=1 -timeout=120s`

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add index/writer.go
git commit -m "fix: wire up worker lifecycle in IndexWriter

Call docWriter.close() in IndexWriter.Close() to stop workers.
Set metrics on FlushControl and individual workers."
```

---

### Task 7: Run Full Test Suite and Benchmark

Verify correctness and measure performance improvement.

**Files:**
- No file changes — validation only.

- [ ] **Step 1: Run the full test suite**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search && go test ./... -count=1 -timeout=300s`

Expected: all tests pass.

- [ ] **Step 2: Run the concurrent indexing benchmark**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search && go test ./index/ -bench="BenchmarkConcurrentIndex" -benchtime=3x -timeout=600s`

Expected: improved throughput at G=4 and G=8 compared to the baseline numbers:
- Before: G=4 = 106,838 docs/sec, G=8 = 107,759 docs/sec
- After: should see meaningful improvement (reduced lock contention)

- [ ] **Step 3: Run the race detector**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search && go test ./index/ -race -run "TestDocumentsWriter|TestWorker" -count=1 -timeout=120s`

Expected: no data races detected.

---

### Task 8: Parallel Merge in Commit (Phase 2)

Run multiple merge candidates concurrently during `Commit()`.

**Files:**
- Modify: `index/writer.go`

- [ ] **Step 1: Write test for parallel merge**

Create a test that verifies multiple merges run in parallel. Add to an appropriate test file (e.g. `index/writer_test.go` or wherever writer tests live). First check if `index/writer_test.go` exists and find the right place.

Add this test:

```go
func TestCommitParallelMerge(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	w := NewIndexWriter(dir, fa, 100)
	w.SetMergePolicy(NewTieredMergePolicy())
	defer w.Close()

	// Create multiple small segments to trigger merge
	for batch := range 20 {
		for j := range 100 {
			doc := document.NewDocument()
			doc.AddField("body", fmt.Sprintf("batch %d doc %d content for merging", batch, j), document.FieldTypeText)
			w.AddDocument(doc)
		}
		w.Commit()
	}

	// Verify segments were merged (should be fewer than 20)
	w.mu.Lock()
	segCount := len(w.segmentInfos.Segments)
	w.mu.Unlock()

	if segCount >= 20 {
		t.Errorf("expected merge to reduce segment count below 20, got %d", segCount)
	}
}
```

- [ ] **Step 2: Update MaybeMerge for parallel execution**

In `index/writer.go`, replace the `MaybeMerge` method:

Find:
```go
func (w *IndexWriter) MaybeMerge(policy MergePolicy) error {
	candidates := policy.FindMerges(w.segmentInfos.Segments)
	if w.infoStream.IsEnabled("IW") && len(candidates) > 0 {
		w.infoStream.Message("IW", fmt.Sprintf("maybeMerge: %d candidates from %d segments",
			len(candidates), len(w.segmentInfos.Segments)))
	}
	for _, candidate := range candidates {
		if err := w.executeMerge(candidate); err != nil {
			return err
		}
	}
	return nil
}
```

Replace with:
```go
func (w *IndexWriter) MaybeMerge(policy MergePolicy) error {
	candidates := policy.FindMerges(w.segmentInfos.Segments)
	if len(candidates) == 0 {
		return nil
	}
	if w.infoStream.IsEnabled("IW") {
		w.infoStream.Message("IW", fmt.Sprintf("maybeMerge: %d candidates from %d segments",
			len(candidates), len(w.segmentInfos.Segments)))
	}

	if len(candidates) == 1 {
		return w.executeMerge(candidates[0])
	}

	// Pre-build merge inputs under w.mu to avoid concurrent readerMap access.
	type mergeWork struct {
		candidate MergeCandidate
		inputs    []MergeInput
		totalDocs int64
		newName   string
	}

	work := make([]mergeWork, len(candidates))
	for i, candidate := range candidates {
		inputs := make([]MergeInput, len(candidate.Segments))
		var totalDocs int64
		for j, info := range candidate.Segments {
			totalDocs += int64(info.MaxDoc)
			rau := w.getOrCreateRAU(info)
			reader, err := rau.getReader()
			if err != nil {
				return fmt.Errorf("open segment %s for merge: %w", info.Name, err)
			}
			inputs[j] = MergeInput{
				Segment:   reader,
				IsDeleted: rau.IsDeleted,
			}
		}
		work[i] = mergeWork{
			candidate: candidate,
			inputs:    inputs,
			totalDocs: totalDocs,
			newName:   w.nextSegmentName(),
		}
	}

	// Run merge I/O in parallel — no shared state access.
	type mergeResult struct {
		newInfo *SegmentCommitInfo
		err     error
	}
	results := make([]mergeResult, len(work))
	var wg sync.WaitGroup
	for i := range work {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			mw := &work[idx]

			if w.infoStream.IsEnabled("IW") {
				var parts []string
				for _, info := range mw.candidate.Segments {
					parts = append(parts, fmt.Sprintf("%s(%d docs)", info.Name, info.MaxDoc))
				}
				w.infoStream.Message("IW", "merging "+strings.Join(parts, " + "))
			}

			start := time.Now()
			result, err := MergeSegmentsToDisk(w.dir, mw.inputs, mw.newName)
			elapsed := time.Since(start)
			if err != nil {
				results[idx] = mergeResult{err: fmt.Errorf("merge segments: %w", err)}
				return
			}

			if w.metrics != nil {
				w.metrics.MergeCount.Add(1)
				w.metrics.MergeDocCount.Add(mw.totalDocs)
				w.metrics.MergeTimeNanos.Add(elapsed.Nanoseconds())
			}
			if w.infoStream.IsEnabled("IW") {
				w.infoStream.Message("IW", fmt.Sprintf(
					"merge done: %d docs, took %dms",
					result.DocCount, elapsed.Milliseconds()))
			}

			results[idx] = mergeResult{
				newInfo: &SegmentCommitInfo{
					Name:   mw.newName,
					MaxDoc: result.DocCount,
					Fields: result.Fields,
					Files:  result.Files,
				},
			}
		}(i)
	}
	wg.Wait()

	// Apply results: remove merged segments, add new ones
	for i, r := range results {
		if r.err != nil {
			return r.err
		}
		if r.newInfo == nil {
			continue
		}

		mergedNames := make(map[string]bool)
		for _, info := range work[i].candidate.Segments {
			mergedNames[info.Name] = true
		}

		var remaining []*SegmentCommitInfo
		for _, info := range w.segmentInfos.Segments {
			if mergedNames[info.Name] {
				if rau, ok := w.readerMap[info.Name]; ok {
					rau.Close()
					delete(w.readerMap, info.Name)
				}
				continue
			}
			remaining = append(remaining, info)
		}
		remaining = append(remaining, r.newInfo)
		w.segmentInfos.Segments = remaining
		w.segmentInfos.Version++
	}

	if w.metrics != nil {
		w.metrics.SegmentCount.Store(int64(len(w.segmentInfos.Segments)))
	}

	return nil
}
```

- [ ] **Step 3: Run tests**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search && go test ./index/ -run "TestCommitParallelMerge" -v -count=1 -timeout=120s`

Expected: test passes.

- [ ] **Step 4: Run full test suite with race detector**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search && go test ./index/ -race -count=1 -timeout=300s`

Expected: all tests pass, no data races.

- [ ] **Step 5: Commit**

```bash
git add index/writer.go
git commit -m "feat: run merge candidates in parallel during Commit

Multiple merge candidates from FindMerges() now execute concurrently.
Merge I/O runs in parallel goroutines; segment list updates are applied
after all merges complete."
```
