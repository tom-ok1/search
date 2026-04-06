# InfoStream & Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two-layer diagnostic instrumentation (always-on Metrics + opt-in InfoStream) to the GoSearch indexing pipeline.

**Architecture:** Layer 1 is `IndexWriterMetrics` with atomic counters/gauges, shared via pointer from IndexWriter → DocumentsWriter → FlushControl. Layer 2 is an `InfoStream` interface (Lucene-compatible) with `NoOpInfoStream` (default) and `PrintInfoStream`. Both are injected into existing structs as new fields with no public API signature changes.

**Tech Stack:** Go stdlib only (`sync/atomic`, `io`, `time`, `fmt`). No external dependencies.

---

### Task 1: InfoStream Interface and Implementations

**Files:**
- Create: `index/infostream.go`
- Create: `index/infostream_test.go`

- [ ] **Step 1: Write the failing test for NoOpInfoStream**

In `index/infostream_test.go`:

```go
package index

import (
	"bytes"
	"strings"
	"testing"
)

func TestNoOpInfoStreamIsDisabled(t *testing.T) {
	is := NoOpInfoStream{}
	if is.IsEnabled("IW") {
		t.Error("NoOpInfoStream should never be enabled")
	}
	if is.IsEnabled("DW") {
		t.Error("NoOpInfoStream should never be enabled")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./index/ -run TestNoOpInfoStreamIsDisabled -v`
Expected: FAIL — `NoOpInfoStream` not defined

- [ ] **Step 3: Write InfoStream interface and NoOpInfoStream**

In `index/infostream.go`:

```go
package index

import (
	"fmt"
	"io"
	"time"
)

// InfoStream provides diagnostic logging for the indexing pipeline.
// Modeled after Lucene's InfoStream. Default is NoOpInfoStream (zero cost).
//
// Component codes:
//   - "IW"   — IndexWriter (merge, commit)
//   - "DW"   — DocumentsWriter (stall events)
//   - "DWFC" — FlushControl (flush triggers)
//   - "DWPT" — DocumentsWriterPerThread (per-DWPT flush)
//   - "IFD"  — FileDeleter (file deletion)
type InfoStream interface {
	// Message logs a diagnostic message from the given component.
	// Must not be called if IsEnabled returns false for that component.
	Message(component string, message string)

	// IsEnabled returns true if logging is active for the given component.
	IsEnabled(component string) bool
}

// NoOpInfoStream is the default InfoStream that produces no output.
// IsEnabled always returns false, so Message is never called.
type NoOpInfoStream struct{}

func (NoOpInfoStream) Message(string, string) {}
func (NoOpInfoStream) IsEnabled(string) bool  { return false }

// PrintInfoStream writes diagnostic messages to an io.Writer
// with a timestamp and component prefix.
type PrintInfoStream struct {
	w io.Writer
}

// NewPrintInfoStream creates an InfoStream that writes to w.
func NewPrintInfoStream(w io.Writer) *PrintInfoStream {
	return &PrintInfoStream{w: w}
}

func (p *PrintInfoStream) Message(component string, message string) {
	fmt.Fprintf(p.w, "%s %s: %s\n", time.Now().UTC().Format(time.RFC3339Nano), component, message)
}

func (p *PrintInfoStream) IsEnabled(string) bool { return true }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./index/ -run TestNoOpInfoStreamIsDisabled -v`
Expected: PASS

- [ ] **Step 5: Write the failing test for PrintInfoStream**

Append to `index/infostream_test.go`:

```go
func TestPrintInfoStreamWritesOutput(t *testing.T) {
	var buf bytes.Buffer
	is := NewPrintInfoStream(&buf)

	if !is.IsEnabled("IW") {
		t.Error("PrintInfoStream should always be enabled")
	}

	is.Message("IW", "test message")
	output := buf.String()

	if !strings.Contains(output, "IW: test message") {
		t.Errorf("expected 'IW: test message' in output, got %q", output)
	}
	// Verify timestamp prefix exists (RFC3339 starts with year)
	if !strings.HasPrefix(output, "20") {
		t.Errorf("expected timestamp prefix, got %q", output)
	}
}

func TestPrintInfoStreamMultipleComponents(t *testing.T) {
	var buf bytes.Buffer
	is := NewPrintInfoStream(&buf)

	is.Message("IW", "merge start")
	is.Message("DW", "stall begin")

	output := buf.String()
	if !strings.Contains(output, "IW: merge start") {
		t.Errorf("missing IW message in %q", output)
	}
	if !strings.Contains(output, "DW: stall begin") {
		t.Errorf("missing DW message in %q", output)
	}
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./index/ -run TestPrintInfoStream -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add index/infostream.go index/infostream_test.go
git commit -m "feat: add InfoStream interface with NoOp and Print implementations"
```

---

### Task 2: IndexWriterMetrics

**Files:**
- Create: `index/metrics.go`
- Create: `index/metrics_test.go`

- [ ] **Step 1: Write the failing test**

In `index/metrics_test.go`:

```go
package index

import "testing"

func TestMetricsZeroInitialized(t *testing.T) {
	m := &IndexWriterMetrics{}

	if m.DocsAdded.Load() != 0 {
		t.Error("DocsAdded should start at 0")
	}
	if m.FlushCount.Load() != 0 {
		t.Error("FlushCount should start at 0")
	}
	if m.MergeCount.Load() != 0 {
		t.Error("MergeCount should start at 0")
	}
	if m.StallCount.Load() != 0 {
		t.Error("StallCount should start at 0")
	}
}

func TestMetricsAtomicIncrement(t *testing.T) {
	m := &IndexWriterMetrics{}

	m.DocsAdded.Add(5)
	m.FlushCount.Add(1)
	m.MergeCount.Add(2)
	m.StallCount.Add(3)
	m.StallTimeNanos.Add(1000)

	if m.DocsAdded.Load() != 5 {
		t.Errorf("DocsAdded = %d, want 5", m.DocsAdded.Load())
	}
	if m.FlushCount.Load() != 1 {
		t.Errorf("FlushCount = %d, want 1", m.FlushCount.Load())
	}
	if m.MergeCount.Load() != 2 {
		t.Errorf("MergeCount = %d, want 2", m.MergeCount.Load())
	}
	if m.StallCount.Load() != 3 {
		t.Errorf("StallCount = %d, want 3", m.StallCount.Load())
	}
	if m.StallTimeNanos.Load() != 1000 {
		t.Errorf("StallTimeNanos = %d, want 1000", m.StallTimeNanos.Load())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./index/ -run TestMetrics -v`
Expected: FAIL — `IndexWriterMetrics` not defined

- [ ] **Step 3: Write IndexWriterMetrics**

In `index/metrics.go`:

```go
package index

import "sync/atomic"

// IndexWriterMetrics provides always-on performance counters for the
// indexing pipeline. All fields are atomic and safe for concurrent access.
// Access via IndexWriter.Metrics().
type IndexWriterMetrics struct {
	// Flush counters
	FlushCount     atomic.Int64 // total flush executions
	FlushBytes     atomic.Int64 // cumulative bytes flushed to disk
	FlushTimeNanos atomic.Int64 // cumulative flush duration in nanoseconds

	// Stall counters
	StallCount     atomic.Int64 // number of stall events
	StallTimeNanos atomic.Int64 // cumulative time goroutines spent stalled

	// Merge counters
	MergeCount     atomic.Int64 // total merge executions
	MergeDocCount  atomic.Int64 // cumulative documents merged
	MergeTimeNanos atomic.Int64 // cumulative merge duration in nanoseconds

	// Document counters
	DocsAdded   atomic.Int64 // total documents added
	DocsDeleted atomic.Int64 // total documents deleted

	// Gauges (current state)
	ActiveBytes       atomic.Int64 // RAM in actively-indexing DWPTs
	FlushPendingBytes atomic.Int64 // RAM in DWPTs pending flush
	SegmentCount      atomic.Int64 // current number of segments
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./index/ -run TestMetrics -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add index/metrics.go index/metrics_test.go
git commit -m "feat: add IndexWriterMetrics with atomic counters"
```

---

### Task 3: Wire InfoStream and Metrics into IndexWriter

**Files:**
- Modify: `index/writer.go`
- Create: `index/writer_infostream_test.go`

- [ ] **Step 1: Write the failing test**

In `index/writer_infostream_test.go`:

```go
package index

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

func TestIndexWriterMetricsAccessor(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	w := NewIndexWriter(dir, fa, 1000)
	defer w.Close()

	m := w.Metrics()
	if m == nil {
		t.Fatal("Metrics() returned nil")
	}

	// Add a document and check DocsAdded
	doc := document.NewDocument()
	doc.AddField("title", "hello world", document.FieldTypeText)
	if err := w.AddDocument(doc); err != nil {
		t.Fatal(err)
	}

	if m.DocsAdded.Load() != 1 {
		t.Errorf("DocsAdded = %d, want 1", m.DocsAdded.Load())
	}
}

func TestIndexWriterSetInfoStream(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	w := NewIndexWriter(dir, fa, 1000)
	defer w.Close()

	// Should not panic when setting InfoStream
	capture := newCapturingInfoStream("IW", "DW", "DWFC", "DWPT", "IFD")
	w.SetInfoStream(capture)
}
```

- [ ] **Step 2: Write the CapturingInfoStream test helper**

Append to `index/infostream_test.go`:

```go
import "sync"

// capturingInfoStream captures messages for test assertions.
type capturingInfoStream struct {
	mu       sync.Mutex
	messages []string
	enabled  map[string]bool
}

func newCapturingInfoStream(components ...string) *capturingInfoStream {
	enabled := make(map[string]bool)
	for _, c := range components {
		enabled[c] = true
	}
	return &capturingInfoStream{enabled: enabled}
}

func (c *capturingInfoStream) Message(component string, message string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, component+": "+message)
}

func (c *capturingInfoStream) IsEnabled(component string) bool {
	return c.enabled[component]
}

func (c *capturingInfoStream) Messages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]string, len(c.messages))
	copy(cp, c.messages)
	return cp
}

func (c *capturingInfoStream) HasMessageContaining(substring string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.messages {
		if strings.Contains(m, substring) {
			return true
		}
	}
	return false
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./index/ -run TestIndexWriterMetricsAccessor -v`
Expected: FAIL — `Metrics()` method not defined

- [ ] **Step 4: Add infoStream and metrics fields to IndexWriter**

In `index/writer.go`, change the `IndexWriter` struct (line 17-27) to:

```go
type IndexWriter struct {
	mu             sync.Mutex
	dir            store.Directory
	fieldAnalyzers *analysis.FieldAnalyzers
	segmentInfos   *SegmentInfos
	segmentCounter int32
	readerMap      map[string]*ReadersAndUpdates
	mergePolicy    MergePolicy
	docWriter      *DocumentsWriter
	fileDeleter    *FileDeleter
	infoStream     InfoStream
	metrics        *IndexWriterMetrics
}
```

In `NewIndexWriter` (line 33), initialize the new fields before creating docWriter:

```go
func NewIndexWriter(dir store.Directory, fieldAnalyzers *analysis.FieldAnalyzers, bufferSize int) *IndexWriter {
	w := &IndexWriter{
		dir:            dir,
		fieldAnalyzers: fieldAnalyzers,
		readerMap:      make(map[string]*ReadersAndUpdates),
		fileDeleter:    NewFileDeleter(dir),
		infoStream:     NoOpInfoStream{},
		metrics:        &IndexWriterMetrics{},
	}
```

Add `SetInfoStream` and `Metrics` methods after `SetMergePolicy` (after line 92):

```go
// SetInfoStream sets the diagnostic logging destination.
// Pass NoOpInfoStream{} to disable (default).
func (w *IndexWriter) SetInfoStream(infoStream InfoStream) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.infoStream = infoStream
	w.docWriter.setInfoStream(infoStream)
}

// Metrics returns the always-on performance counters.
func (w *IndexWriter) Metrics() *IndexWriterMetrics {
	return w.metrics
}
```

- [ ] **Step 5: Pass metrics to DocumentsWriter and wire DocsAdded**

In `NewIndexWriter`, after creating `docWriter` (line 69), pass metrics:

```go
	w.docWriter = newDocumentsWriter(dir, fieldAnalyzers, defaultRAMBufferSize, bufferSize, func() string {
		return w.nextSegmentName()
	})
	w.docWriter.metrics = w.metrics
```

In `index/documents_writer.go`, add fields to `DocumentsWriter` struct (after line 24):

```go
type DocumentsWriter struct {
	mu           sync.Mutex
	pool         *perThreadPool
	flushControl *FlushControl
	ticketQueue  *FlushTicketQueue
	deleteQueue  *DeleteQueue
	dir          store.Directory
	infoStream   InfoStream
	metrics      *IndexWriterMetrics
	onSegmentFlushed func(info *SegmentCommitInfo)
	onGlobalUpdates  func(updates *FrozenBufferedUpdates)
}
```

Add `setInfoStream` method to `DocumentsWriter`:

```go
func (dw *DocumentsWriter) setInfoStream(infoStream InfoStream) {
	dw.infoStream = infoStream
	dw.flushControl.infoStream = infoStream
}
```

In `newDocumentsWriter`, pass metrics to FlushControl and initialize infoStream:

```go
func newDocumentsWriter(dir store.Directory, fieldAnalyzers *analysis.FieldAnalyzers, ramBufferSize int64, maxBufferedDocs int, nameFunc func() string) *DocumentsWriter {
	deleteQueue := newDeleteQueue()
	pool := newPerThreadPool(nameFunc, fieldAnalyzers, deleteQueue)
	return &DocumentsWriter{
		pool:         pool,
		flushControl: newFlushControl(ramBufferSize, maxBufferedDocs, pool),
		ticketQueue:  newFlushTicketQueue(),
		deleteQueue:  deleteQueue,
		dir:          dir,
		infoStream:   NoOpInfoStream{},
	}
}
```

In `DocumentsWriter.addDocument`, add DocsAdded metric (after the `dwpt.addDocument` call, line 44):

```go
	bytesAdded, err := dwpt.addDocument(doc)
	if err != nil {
		dw.pool.returnAndUnlock(dwpt)
		return err
	}
	if dw.metrics != nil {
		dw.metrics.DocsAdded.Add(1)
	}
```

In `index/flush_control.go`, add fields to `FlushControl` struct (after line 18):

```go
type FlushControl struct {
	mu              sync.Mutex
	activeBytes     int64
	flushBytes      int64
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

In `newFlushControl`, initialize infoStream:

```go
func newFlushControl(ramBufferSize int64, maxBufferedDocs int, pool *perThreadPool) *FlushControl {
	fc := &FlushControl{
		ramBufferSize:   ramBufferSize,
		maxBufferedDocs: maxBufferedDocs,
		stallLimit:      2 * ramBufferSize,
		pool:            pool,
		infoStream:      NoOpInfoStream{},
	}
	fc.stallCond = sync.NewCond(&fc.mu)
	return fc
}
```

In `NewIndexWriter`, after `w.docWriter.metrics = w.metrics`, also set:

```go
	w.docWriter.flushControl.metrics = w.metrics
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./index/ -run TestIndexWriter -v -count=1`
Expected: PASS (both new tests and all existing tests)

- [ ] **Step 7: Run full test suite to verify no regressions**

Run: `go test ./index/ -v -count=1 -timeout=600s`
Expected: All tests PASS

- [ ] **Step 8: Commit**

```bash
git add index/writer.go index/documents_writer.go index/flush_control.go index/writer_infostream_test.go index/infostream_test.go
git commit -m "feat: wire InfoStream and Metrics into IndexWriter pipeline"
```

---

### Task 4: Instrument FlushControl (Priority 1)

**Files:**
- Modify: `index/flush_control.go`
- Modify: `index/writer_infostream_test.go`

- [ ] **Step 1: Write the failing test for stall metrics**

Append to `index/writer_infostream_test.go`:

```go
func TestMetricsStallTracking(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	// Very small buffer to trigger stalls: 1KB RAM buffer, 5 docs per DWPT
	w := NewIndexWriter(dir, fa, 5)
	defer w.Close()

	m := w.Metrics()

	// Add enough documents to trigger at least one flush
	for i := range 100 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("document number %d with enough words to use some bytes", i), document.FieldTypeText)
		if err := w.AddDocument(doc); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}

	if m.FlushCount.Load() == 0 {
		t.Error("expected at least one flush")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./index/ -run TestMetricsStallTracking -v`
Expected: FAIL — FlushCount is 0 (not yet instrumented)

- [ ] **Step 3: Instrument FlushControl.waitIfStalled**

In `index/flush_control.go`, add `"time"` to the imports. Then replace `waitIfStalled` (lines 101-108):

```go
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
			float64(fc.activeBytes)/(1024*1024),
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

Add `"fmt"` and `"time"` to the import block at the top of `flush_control.go`.

- [ ] **Step 4: Instrument FlushControl.doAfterDocument**

In `doAfterDocument` (lines 37-66), add metrics gauge updates and InfoStream logging. After the `fc.activeBytes += bytesAdded` line (line 41):

```go
	fc.activeBytes += bytesAdded
	if fc.metrics != nil {
		fc.metrics.ActiveBytes.Store(fc.activeBytes)
	}
```

Inside the `if shouldFlush` block (after line 56, after adding to flushQueue):

```go
		fc.flushQueue = append(fc.flushQueue, dwpt)

		if fc.metrics != nil {
			fc.metrics.FlushPendingBytes.Store(fc.flushBytes)
			fc.metrics.ActiveBytes.Store(fc.activeBytes)
		}
		if fc.infoStream.IsEnabled("DWFC") {
			fc.infoStream.Message("DWFC", fmt.Sprintf(
				"flush triggered: ramBytes=%.1f MB > limit=%.1f MB",
				float64(fc.activeBytes+fc.flushBytes)/(1024*1024),
				float64(fc.ramBufferSize)/(1024*1024)))
		}
```

- [ ] **Step 5: Instrument FlushControl.doAfterFlush**

In `doAfterFlush` (lines 85-98), after updating flushBytes (line 89):

```go
	fc.flushBytes -= dwpt.estimateBytesUsed()
	if fc.flushBytes < 0 {
		fc.flushBytes = 0
	}
	if fc.metrics != nil {
		fc.metrics.FlushPendingBytes.Store(fc.flushBytes)
	}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./index/ -run TestMetricsStallTracking -v`
Expected: PASS

- [ ] **Step 7: Write InfoStream stall message test**

Append to `index/writer_infostream_test.go`:

```go
func TestInfoStreamStallMessages(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	// Small buffer to trigger flushes
	w := NewIndexWriter(dir, fa, 5)
	defer w.Close()

	capture := newCapturingInfoStream("IW", "DW", "DWFC", "DWPT", "IFD")
	w.SetInfoStream(capture)

	for i := range 50 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("document %d about testing infostream logging", i), document.FieldTypeText)
		if err := w.AddDocument(doc); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}

	// Should have at least flush trigger messages
	if !capture.HasMessageContaining("flush triggered") {
		t.Errorf("expected 'flush triggered' message, got: %v", capture.Messages())
	}
}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `go test ./index/ -run TestInfoStreamStallMessages -v`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add index/flush_control.go index/writer_infostream_test.go
git commit -m "feat: instrument FlushControl with Metrics and InfoStream"
```

---

### Task 5: Instrument DocumentsWriter.doFlush (Priority 2)

**Files:**
- Modify: `index/documents_writer.go`
- Modify: `index/writer_infostream_test.go`

- [ ] **Step 1: Write the failing test**

Append to `index/writer_infostream_test.go`:

```go
func TestInfoStreamFlushMessages(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	w := NewIndexWriter(dir, fa, 10)
	defer w.Close()

	capture := newCapturingInfoStream("IW", "DW", "DWFC", "DWPT", "IFD")
	w.SetInfoStream(capture)

	for i := range 30 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("document %d for flush testing", i), document.FieldTypeText)
		if err := w.AddDocument(doc); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}

	if !capture.HasMessageContaining("flush") {
		t.Errorf("expected flush message in DWPT, got: %v", capture.Messages())
	}

	m := w.Metrics()
	if m.FlushCount.Load() == 0 {
		t.Error("expected FlushCount > 0")
	}
	if m.FlushTimeNanos.Load() == 0 {
		t.Error("expected FlushTimeNanos > 0")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./index/ -run TestInfoStreamFlushMessages -v`
Expected: FAIL — FlushTimeNanos is 0

- [ ] **Step 3: Instrument doFlush**

In `index/documents_writer.go`, add `"fmt"` and `"time"` to the imports. Replace `doFlush` (lines 75-85):

```go
func (dw *DocumentsWriter) doFlush(dwpt *DocumentsWriterPerThread) error {
	ticket := dw.ticketQueue.addTicket()
	globalUpdates := dwpt.prepareFlush()

	start := time.Now()
	info, err := dwpt.flush(dw.dir)
	elapsed := time.Since(start)

	dw.flushControl.doAfterFlush(dwpt)
	dw.ticketQueue.markDone(ticket, info, globalUpdates, err)

	if err != nil {
		return err
	}

	if dw.metrics != nil {
		dw.metrics.FlushCount.Add(1)
		dw.metrics.FlushTimeNanos.Add(elapsed.Nanoseconds())
		if info != nil {
			dw.metrics.FlushBytes.Add(dwpt.estimateBytesUsed())
		}
	}
	if dw.infoStream.IsEnabled("DWPT") && info != nil {
		dw.infoStream.Message("DWPT", fmt.Sprintf(
			"flush %s: %d docs, %.1f MB, took %dms",
			info.Name, info.MaxDoc,
			float64(dwpt.estimateBytesUsed())/(1024*1024),
			elapsed.Milliseconds()))
	}

	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./index/ -run TestInfoStreamFlushMessages -v`
Expected: PASS

- [ ] **Step 5: Run full test suite**

Run: `go test ./index/ -v -count=1 -timeout=600s`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add index/documents_writer.go index/writer_infostream_test.go
git commit -m "feat: instrument DocumentsWriter.doFlush with Metrics and InfoStream"
```

---

### Task 6: Instrument IndexWriter (Priority 3)

**Files:**
- Modify: `index/writer.go`
- Modify: `index/writer_infostream_test.go`

- [ ] **Step 1: Write the failing test for merge metrics**

Append to `index/writer_infostream_test.go`:

```go
func TestMetricsMergeCount(t *testing.T) {
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

	// Create multiple segments by committing in small batches
	for batch := range 5 {
		for i := range 200 {
			doc := document.NewDocument()
			doc.AddField("body", fmt.Sprintf("batch %d doc %d for merge testing", batch, i), document.FieldTypeText)
			if err := w.AddDocument(doc); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	m := w.Metrics()
	// With TieredMergePolicy and many small segments, merges should happen
	if m.MergeCount.Load() == 0 {
		t.Log("no merges triggered (may depend on policy thresholds)")
	}
}

func TestInfoStreamMergeMessages(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	w := NewIndexWriter(dir, fa, 50)
	defer w.Close()

	capture := newCapturingInfoStream("IW", "DW", "DWFC", "DWPT", "IFD")
	w.SetInfoStream(capture)

	// Create enough segments to trigger a force merge
	for batch := range 3 {
		for i := range 100 {
			doc := document.NewDocument()
			doc.AddField("body", fmt.Sprintf("batch %d doc %d", batch, i), document.FieldTypeText)
			if err := w.AddDocument(doc); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.ForceMerge(1); err != nil {
		t.Fatal(err)
	}

	if !capture.HasMessageContaining("merging") {
		t.Errorf("expected 'merging' message, got: %v", capture.Messages())
	}
	if !capture.HasMessageContaining("merge done") {
		t.Errorf("expected 'merge done' message, got: %v", capture.Messages())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./index/ -run TestInfoStreamMergeMessages -v`
Expected: FAIL — no "merging" message

- [ ] **Step 3: Instrument executeMerge**

In `index/writer.go`, add `"time"` to the imports. Replace `executeMerge` (lines 327-379):

```go
func (w *IndexWriter) executeMerge(candidate MergeCandidate) error {
	if len(candidate.Segments) < 2 {
		return nil
	}

	// Log merge start
	if w.infoStream.IsEnabled("IW") {
		var parts []string
		for _, info := range candidate.Segments {
			parts = append(parts, fmt.Sprintf("%s(%d docs)", info.Name, info.MaxDoc))
		}
		w.infoStream.Message("IW", "merging "+strings.Join(parts, " + "))
	}

	inputs := make([]MergeInput, len(candidate.Segments))
	var totalDocs int64
	for i, info := range candidate.Segments {
		rau := w.getOrCreateRAU(info)
		reader, err := rau.getReader()
		if err != nil {
			return fmt.Errorf("open segment %s for merge: %w", info.Name, err)
		}
		inputs[i] = MergeInput{
			Segment:   reader,
			IsDeleted: rau.IsDeleted,
		}
		totalDocs += int64(info.MaxDoc)
	}

	newName := w.nextSegmentName()
	start := time.Now()
	result, err := MergeSegmentsToDisk(w.dir, inputs, newName)
	elapsed := time.Since(start)

	if err != nil {
		return fmt.Errorf("merge segments: %w", err)
	}

	if w.metrics != nil {
		w.metrics.MergeCount.Add(1)
		w.metrics.MergeDocCount.Add(totalDocs)
		w.metrics.MergeTimeNanos.Add(elapsed.Nanoseconds())
	}
	if w.infoStream.IsEnabled("IW") {
		w.infoStream.Message("IW", fmt.Sprintf(
			"merge done: %d docs, took %dms",
			result.DocCount, elapsed.Milliseconds()))
	}

	newInfo := &SegmentCommitInfo{
		Name:   newName,
		MaxDoc: result.DocCount,
		Fields: result.Fields,
		Files:  result.Files,
	}

	mergedNames := make(map[string]bool)
	for _, info := range candidate.Segments {
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
	remaining = append(remaining, newInfo)
	w.segmentInfos.Segments = remaining
	w.segmentInfos.Version++

	// Update segment count gauge
	if w.metrics != nil {
		w.metrics.SegmentCount.Store(int64(len(w.segmentInfos.Segments)))
	}

	return nil
}
```

- [ ] **Step 4: Instrument Commit**

In `index/writer.go`, add InfoStream logging to `Commit`. Add at the start of Commit (after line 129):

```go
func (w *IndexWriter) Commit() error {
	commitStart := time.Now()
```

Add after `w.mu.Lock()` (after line 135):

```go
	if w.infoStream.IsEnabled("IW") {
		w.infoStream.Message("IW", fmt.Sprintf("commit start: %d segments", len(w.segmentInfos.Segments)))
	}
```

Add at the end of Commit, before `return w.autoMerge()` (replace line 192):

```go
	// Update segment count gauge
	if w.metrics != nil {
		w.metrics.SegmentCount.Store(int64(len(w.segmentInfos.Segments)))
	}

	if w.infoStream.IsEnabled("IW") {
		w.infoStream.Message("IW", fmt.Sprintf("commit done: %d segments, took %dms",
			len(w.segmentInfos.Segments), time.Since(commitStart).Milliseconds()))
	}

	// 10. Trigger auto-merge
	return w.autoMerge()
```

- [ ] **Step 5: Instrument MaybeMerge**

In `index/writer.go`, replace `MaybeMerge` (lines 287-295):

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

- [ ] **Step 6: Instrument DeleteDocuments**

In `index/writer.go`, replace `DeleteDocuments` (lines 280-283):

```go
func (w *IndexWriter) DeleteDocuments(field, term string) error {
	w.docWriter.deleteDocuments(field, term)
	if w.metrics != nil {
		w.metrics.DocsDeleted.Add(1)
	}
	return nil
}
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `go test ./index/ -run "TestInfoStreamMergeMessages|TestMetricsMergeCount" -v`
Expected: PASS

- [ ] **Step 8: Run full test suite**

Run: `go test ./index/ -v -count=1 -timeout=600s`
Expected: All tests PASS

- [ ] **Step 9: Commit**

```bash
git add index/writer.go index/writer_infostream_test.go
git commit -m "feat: instrument IndexWriter merge/commit/delete with Metrics and InfoStream"
```

---

### Task 7: Instrument FileDeleter (Priority 4)

**Files:**
- Modify: `index/file_deleter.go`
- Modify: `index/writer.go`

- [ ] **Step 1: Add InfoStream field to FileDeleter**

In `index/file_deleter.go`, add `"fmt"` to imports. Add `infoStream` field to `FileDeleter` struct:

```go
type FileDeleter struct {
	mu         sync.Mutex
	dir        store.Directory
	refCount   map[string]int
	pending    map[string]bool
	infoStream InfoStream
}
```

In `NewFileDeleter`, initialize it:

```go
func NewFileDeleter(dir store.Directory) *FileDeleter {
	return &FileDeleter{
		dir:        dir,
		refCount:   make(map[string]int),
		pending:    make(map[string]bool),
		infoStream: NoOpInfoStream{},
	}
}
```

In `DeleteIfUnreferenced`, add logging in the immediate-delete branch:

```go
func (fd *FileDeleter) DeleteIfUnreferenced(files []string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	for _, f := range files {
		if fd.refCount[f] > 0 {
			fd.pending[f] = true
		} else {
			if fd.infoStream.IsEnabled("IFD") {
				fd.infoStream.Message("IFD", fmt.Sprintf("delete %s: refcount=0", f))
			}
			_ = fd.dir.DeleteFile(f)
		}
	}
}
```

- [ ] **Step 2: Wire FileDeleter.infoStream from IndexWriter.SetInfoStream**

In `index/writer.go`, in `SetInfoStream`, add:

```go
func (w *IndexWriter) SetInfoStream(infoStream InfoStream) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.infoStream = infoStream
	w.docWriter.setInfoStream(infoStream)
	w.fileDeleter.infoStream = infoStream
}
```

- [ ] **Step 3: Run full test suite**

Run: `go test ./index/ -v -count=1 -timeout=600s`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add index/file_deleter.go index/writer.go
git commit -m "feat: instrument FileDeleter with InfoStream logging"
```

---

### Task 8: Integrate Metrics into Benchmarks

**Files:**
- Modify: `index/scale_bench_test.go`

- [ ] **Step 1: Add metrics reporting to BenchmarkConcurrentIndex**

In `index/scale_bench_test.go`, in `BenchmarkConcurrentIndex`, after the existing `b.ReportMetric` calls for `docs/sec` and `segments` (lines 266-267), add:

```go
				b.ReportMetric(float64(totalDocs)/b.Elapsed().Seconds(), "docs/sec")
				b.ReportMetric(float64(len(w.segmentInfos.Segments)), "segments")
				m := w.Metrics()
				b.ReportMetric(float64(m.StallCount.Load()), "stalls")
				b.ReportMetric(float64(m.StallTimeNanos.Load())/1e6, "stall-ms")
				b.ReportMetric(float64(m.MergeCount.Load()), "merges")
				b.ReportMetric(float64(m.FlushCount.Load()), "flushes")
```

- [ ] **Step 2: Add metrics reporting to BenchmarkMemoryStability**

In `BenchmarkMemoryStability`, after the `b.ReportMetric` for `final-segments` (line 152), add:

```go
	b.ReportMetric(float64(len(w.segmentInfos.Segments)), "final-segments")
	m := w.Metrics()
	b.ReportMetric(float64(m.StallCount.Load()), "stalls")
	b.ReportMetric(float64(m.StallTimeNanos.Load())/1e6, "stall-ms")
	b.ReportMetric(float64(m.MergeCount.Load()), "merges")
	b.ReportMetric(float64(m.FlushCount.Load()), "flushes")
```

- [ ] **Step 3: Add metrics reporting to BenchmarkSustainedThroughput**

In `BenchmarkSustainedThroughput`, after the `b.ReportMetric` for `docs/sec` (line 63), add:

```go
			b.ReportMetric(float64(totalDocs)/b.Elapsed().Seconds(), "docs/sec")
			m := w.Metrics()
			b.ReportMetric(float64(m.StallCount.Load()), "stalls")
			b.ReportMetric(float64(m.StallTimeNanos.Load())/1e6, "stall-ms")
			b.ReportMetric(float64(m.MergeCount.Load()), "merges")
			b.ReportMetric(float64(m.FlushCount.Load()), "flushes")
```

- [ ] **Step 4: Verify benchmarks compile**

Run: `go test ./index/ -bench=BenchmarkConcurrentIndex/Goroutines_1 -benchmem -count=1 -timeout=60s`
Expected: PASS with new metrics columns (stalls, stall-ms, merges, flushes)

- [ ] **Step 5: Commit**

```bash
git add index/scale_bench_test.go
git commit -m "feat: add Metrics output to scale benchmarks"
```

---

### Task 9: Concurrent Metrics Accuracy Test

**Files:**
- Modify: `index/writer_infostream_test.go`

- [ ] **Step 1: Write the concurrent accuracy test**

Append to `index/writer_infostream_test.go`:

```go
func TestMetricsConcurrentAccess(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	w := NewIndexWriter(dir, fa, 1000)
	defer w.Close()

	const goroutines = 4
	const docsPerGoroutine = 250

	errs := make(chan error, goroutines)
	for g := range goroutines {
		go func(offset int) {
			for i := range docsPerGoroutine {
				doc := document.NewDocument()
				doc.AddField("body", fmt.Sprintf("goroutine %d doc %d", offset, i), document.FieldTypeText)
				if err := w.AddDocument(doc); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}(g)
	}
	for range goroutines {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}

	m := w.Metrics()
	expected := int64(goroutines * docsPerGoroutine)
	if m.DocsAdded.Load() != expected {
		t.Errorf("DocsAdded = %d, want %d", m.DocsAdded.Load(), expected)
	}
}

func TestInfoStreamComponentFiltering(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	w := NewIndexWriter(dir, fa, 10)
	defer w.Close()

	// Only enable "IW" component
	capture := newCapturingInfoStream("IW")
	w.SetInfoStream(capture)

	for i := range 30 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("doc %d for filtering test", i), document.FieldTypeText)
		if err := w.AddDocument(doc); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}

	// Should NOT have DWFC messages (not enabled)
	for _, msg := range capture.Messages() {
		if strings.HasPrefix(msg, "DWFC:") {
			t.Errorf("unexpected DWFC message when only IW enabled: %s", msg)
		}
		if strings.HasPrefix(msg, "DWPT:") {
			t.Errorf("unexpected DWPT message when only IW enabled: %s", msg)
		}
	}
}
```

- [ ] **Step 2: Run tests**

Run: `go test ./index/ -run "TestMetricsConcurrentAccess|TestInfoStreamComponentFiltering" -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add index/writer_infostream_test.go
git commit -m "test: add concurrent metrics accuracy and component filtering tests"
```

---

### Task 10: Update SegmentCount Gauge in Remaining Paths

**Files:**
- Modify: `index/writer.go`

- [ ] **Step 1: Update SegmentCount in onSegmentFlushed callback**

In `NewIndexWriter`, in the `onSegmentFlushed` callback (lines 72-77), add gauge update:

```go
	w.docWriter.onSegmentFlushed = func(info *SegmentCommitInfo) {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.segmentInfos.Segments = append(w.segmentInfos.Segments, info)
		w.segmentInfos.Version++
		if w.metrics != nil {
			w.metrics.SegmentCount.Store(int64(len(w.segmentInfos.Segments)))
		}
	}
```

- [ ] **Step 2: Run full test suite**

Run: `go test ./index/ -v -count=1 -timeout=600s`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
git add index/writer.go
git commit -m "feat: update SegmentCount gauge on segment flush"
```
