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
	dw.onSegmentFlushed = func(info *SegmentCommitInfo) {
		mu.Lock()
		segments = append(segments, info)
		mu.Unlock()
	}

	// Add enough docs to trigger at least one flush
	for i := range 20 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("document number %d with some text to generate bytes", i), document.FieldTypeText)
		if err := dw.addDocument(doc); err != nil {
			t.Fatal(err)
		}
	}

	// Flush remaining
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

	// Large buffer so nothing auto-flushes
	dw := newDocumentsWriter(dir, newTestFieldAnalyzers(), 1<<30, 0, func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	})
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

// TestDocumentsWriterFlushQueueDrained verifies that DWPTs added to the
// flush queue during addDocument are properly dequeued and released.
// Before the fix, doAfterDocument appended DWPTs to flushQueue but
// addDocument never called nextPendingFlush, causing a memory leak.
func TestDocumentsWriterFlushQueueDrained(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	var counter atomic.Int32

	// Use a small RAM buffer to trigger multiple flushes.
	dw := newDocumentsWriter(dir, newTestFieldAnalyzers(), 200, 0, func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	})
	var flushedCount atomic.Int32
	dw.onSegmentFlushed = func(info *SegmentCommitInfo) {
		flushedCount.Add(1)
	}

	for i := range 50 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("document %d with enough text to accumulate bytes and trigger flush", i), document.FieldTypeText)
		if err := dw.addDocument(doc); err != nil {
			t.Fatal(err)
		}
	}

	if flushedCount.Load() == 0 {
		t.Fatal("test setup error: expected at least one flush to have been triggered")
	}

	// The flush queue must be empty — every DWPT that was enqueued should
	// have been dequeued by nextPendingFlush inside addDocument.
	if pending := dw.flushControl.nextPendingFlush(); pending != nil {
		t.Fatalf("flush queue is not empty: found a leaked DWPT (segment %s)", pending.segment.name)
	}
}

// TestDocumentsWriterFlushQueueDrainedConcurrent verifies the flush queue
// is properly drained when multiple goroutines add documents concurrently,
// triggering flushes in parallel.
func TestDocumentsWriterFlushQueueDrainedConcurrent(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	var counter atomic.Int32

	dw := newDocumentsWriter(dir, newTestFieldAnalyzers(), 500, 0, func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	})
	var flushedCount atomic.Int32
	dw.onSegmentFlushed = func(info *SegmentCommitInfo) {
		flushedCount.Add(1)
	}

	const goroutines = 8
	const docsPerGoroutine = 200
	var wg sync.WaitGroup

	for g := range goroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range docsPerGoroutine {
				doc := document.NewDocument()
				doc.AddField("body", fmt.Sprintf("goroutine %d document %d with text to generate bytes", gid, i), document.FieldTypeText)
				if err := dw.addDocument(doc); err != nil {
					t.Errorf("addDocument error: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if flushedCount.Load() == 0 {
		t.Fatal("test setup error: expected at least one flush to have been triggered")
	}

	// Drain must have happened during addDocument — queue should be empty.
	if pending := dw.flushControl.nextPendingFlush(); pending != nil {
		t.Fatalf("flush queue is not empty after concurrent adds: leaked DWPT (segment %s)", pending.segment.name)
	}

	// Pool active map should have no stale entries for flushed DWPTs.
	dw.pool.mu.Lock()
	activeCount := len(dw.pool.active)
	dw.pool.mu.Unlock()
	if activeCount != 0 {
		t.Errorf("expected 0 active DWPTs after all goroutines finished, got %d", activeCount)
	}
}

func TestDocumentsWriterDeleteDocuments(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	var counter atomic.Int32

	dw := newDocumentsWriter(dir, newTestFieldAnalyzers(), 1<<30, 0, func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	})

	doc := document.NewDocument()
	doc.AddField("id", "1", document.FieldTypeKeyword)
	doc.AddField("body", "hello world", document.FieldTypeText)
	dw.addDocument(doc)

	dw.deleteDocuments("id", "1")

	// Verify deletes are buffered in the delete queue
	if !dw.deleteQueue.anyChanges() {
		t.Fatal("expected delete queue to have changes after deleteDocuments")
	}

	// Freeze and verify the global buffer contains the delete
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

	// Second freeze should be empty (buffer was cleared)
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
