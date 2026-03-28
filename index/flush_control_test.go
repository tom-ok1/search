package index

import (
	"fmt"
	"testing"
	"time"

	"gosearch/document"
)

func TestFlushControlMarksFlushPending(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	// Small RAM buffer to trigger flush quickly
	fc := newFlushControl(100, 0, pool)

	dwpt := pool.getAndLock()

	// Add a doc that generates enough bytes to exceed threshold
	doc := document.NewDocument()
	doc.AddField("body", "hello world this is a test document with enough text to exceed the tiny buffer", document.FieldTypeText)
	bytesAdded, _ := dwpt.addDocument(doc)

	result := fc.doAfterDocument(dwpt, bytesAdded)
	if result == nil {
		t.Fatal("expected DWPT to be returned as flush-pending")
	}
	if !result.flushPending {
		t.Error("expected flushPending to be true")
	}
}

func TestFlushControlDoAfterFlush(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	fc := newFlushControl(100, 0, pool)
	dwpt := pool.getAndLock()

	doc := document.NewDocument()
	doc.AddField("body", "hello world test document with enough text to trigger flush", document.FieldTypeText)
	bytesAdded, _ := dwpt.addDocument(doc)

	fc.doAfterDocument(dwpt, bytesAdded)

	if fc.flushBytes <= 0 {
		t.Fatalf("expected positive flushBytes, got %d", fc.flushBytes)
	}

	fc.doAfterFlush(dwpt)

	if fc.flushBytes != 0 {
		t.Errorf("expected flushBytes=0 after doAfterFlush, got %d", fc.flushBytes)
	}
}

func TestFlushControlStallAndUnstall(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	// ramBufferSize=50, stallLimit=2*50=100
	fc := newFlushControl(50, 0, pool)

	// A single large doc whose bytesAdded >= stallLimit(100) triggers both flush and stall.
	dwpt := pool.getAndLock()
	doc := document.NewDocument()
	doc.AddField("body", "this is a document with a lot of text that will definitely exceed the tiny ram buffer and cause stalling because we need enough bytes to surpass the stall limit threshold which is two times the ram buffer size so we need a really long sentence here", document.FieldTypeText)
	bytesAdded, _ := dwpt.addDocument(doc)
	if bytesAdded < fc.stallLimit {
		t.Fatalf("test setup error: bytesAdded(%d) < stallLimit(%d), increase document text", bytesAdded, fc.stallLimit)
	}
	fc.doAfterDocument(dwpt, bytesAdded)

	if !fc.stalled {
		t.Fatal("expected stalled to be true after exceeding stallLimit")
	}

	// Verify waitIfStalled blocks, then unblocks after flush
	done := make(chan struct{})
	go func() {
		fc.waitIfStalled()
		close(done)
	}()

	// Should be blocked
	select {
	case <-done:
		t.Fatal("should be stalled but wasn't")
	case <-time.After(50 * time.Millisecond):
	}

	// Flush to unstall
	fc.doAfterFlush(dwpt)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("should have unstalled after flush")
	}
}

func TestFlushControlMarkForFullFlush(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	fc := newFlushControl(1<<30, 0, pool) // large buffer so nothing auto-flushes

	// Create several DWPTs with documents
	var dwpts []*DocumentsWriterPerThread
	for i := range 3 {
		dwpt := pool.getAndLock()
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("doc %d content", i), document.FieldTypeText)
		dwpt.addDocument(doc)
		dwpts = append(dwpts, dwpt)
	}

	// Return some to pool — dwpts[2] remains active
	pool.returnAndUnlock(dwpts[0])
	pool.returnAndUnlock(dwpts[1])

	// markForFullFlush should block until dwpts[2] is returned
	done := make(chan []*DocumentsWriterPerThread, 1)
	go func() {
		done <- fc.markForFullFlush()
	}()

	// Should be blocked because dwpts[2] is still active
	select {
	case <-done:
		t.Fatal("markForFullFlush should block while active DWPTs exist")
	case <-time.After(50 * time.Millisecond):
	}

	// Return the active DWPT
	pool.returnAndUnlock(dwpts[2])

	select {
	case toFlush := <-done:
		if len(toFlush) != 3 {
			t.Errorf("expected 3 DWPTs for full flush, got %d", len(toFlush))
		}
		// All should be marked flush pending
		for _, d := range toFlush {
			if !d.flushPending {
				t.Error("expected all DWPTs to be flushPending after markForFullFlush")
			}
		}
	case <-time.After(time.Second):
		t.Fatal("markForFullFlush timed out")
	}

	// Pool should be empty
	all := pool.drainFreeAndMarkActive()
	if len(all) != 0 {
		t.Errorf("pool should be empty after markForFullFlush, got %d", len(all))
	}
}
