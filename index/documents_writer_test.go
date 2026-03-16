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
	var counter int32
	var segments []*SegmentCommitInfo
	var mu sync.Mutex

	dw := newDocumentsWriter(dir, newTestAnalyzer(), 500, 0, func() string {
		n := atomic.AddInt32(&counter, 1)
		return fmt.Sprintf("_seg%d", n)
	})
	dw.onSegmentFlushed = func(info *SegmentCommitInfo) {
		mu.Lock()
		segments = append(segments, info)
		mu.Unlock()
	}

	// Add enough docs to trigger at least one flush
	for i := 0; i < 20; i++ {
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
	var counter int32
	var segments []*SegmentCommitInfo
	var mu sync.Mutex

	dw := newDocumentsWriter(dir, newTestAnalyzer(), 2000, 0, func() string {
		n := atomic.AddInt32(&counter, 1)
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

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < docsPerGoroutine; i++ {
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
	var counter int32
	var segments []*SegmentCommitInfo
	var mu sync.Mutex

	// Large buffer so nothing auto-flushes
	dw := newDocumentsWriter(dir, newTestAnalyzer(), 1<<30, 0, func() string {
		n := atomic.AddInt32(&counter, 1)
		return fmt.Sprintf("_seg%d", n)
	})
	dw.onSegmentFlushed = func(info *SegmentCommitInfo) {
		mu.Lock()
		segments = append(segments, info)
		mu.Unlock()
	}

	for i := 0; i < 5; i++ {
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
	var counter int32

	dw := newDocumentsWriter(dir, newTestAnalyzer(), 1<<30, 0, func() string {
		n := atomic.AddInt32(&counter, 1)
		return fmt.Sprintf("_seg%d", n)
	})

	doc := document.NewDocument()
	doc.AddField("id", "1", document.FieldTypeKeyword)
	doc.AddField("body", "hello world", document.FieldTypeText)
	dw.addDocument(doc)

	dw.deleteDocuments("id", "1")

	deletes := dw.takePendingDeletes()
	if len(deletes) != 1 {
		t.Fatalf("expected 1 pending delete, got %d", len(deletes))
	}
	if deletes[0].Field != "id" || deletes[0].Term != "1" {
		t.Errorf("unexpected delete term: %+v", deletes[0])
	}

	// Second take should be empty
	deletes2 := dw.takePendingDeletes()
	if len(deletes2) != 0 {
		t.Errorf("expected 0 pending deletes after take, got %d", len(deletes2))
	}
}
