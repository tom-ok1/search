package index_test

import (
	"fmt"
	"sync"
	"testing"

	"gosearch/document"
	"gosearch/index"
)

func TestConcurrentAddAndCommit(t *testing.T) {
	writer, dir := newTestWriter(t, 100)
	defer writer.Close()

	const goroutines = 8
	const docsPerGoroutine = 1000

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range docsPerGoroutine {
				doc := document.NewDocument()
				doc.AddField("body", fmt.Sprintf("goroutine %d document %d content", gid, i), document.FieldTypeText)
				doc.AddField("id", fmt.Sprintf("g%d_d%d", gid, i), document.FieldTypeKeyword)
				if err := writer.AddDocument(doc); err != nil {
					t.Errorf("AddDocument error: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	expected := goroutines * docsPerGoroutine
	if reader.TotalDocCount() != expected {
		t.Errorf("TotalDocCount: got %d, want %d", reader.TotalDocCount(), expected)
	}
}

func TestConcurrentAddAndNRTRead(t *testing.T) {
	writer, _ := newTestWriter(t, 50)
	defer writer.Close()

	// Add some initial docs
	for i := range 100 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("initial document %d", i), document.FieldTypeText)
		writer.AddDocument(doc)
	}

	// Open NRT reader mid-flight
	reader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Verify consistent snapshot
	if reader.TotalDocCount() < 1 {
		t.Error("NRT reader should see at least some documents")
	}

	// Add more docs after snapshot
	for i := range 50 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("post-snapshot document %d", i), document.FieldTypeText)
		writer.AddDocument(doc)
	}

	// Original reader should not see new docs
	snapshot := reader.TotalDocCount()

	reader2, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader2.Close()

	if reader2.TotalDocCount() <= snapshot {
		t.Errorf("new NRT reader should see more docs: snapshot=%d, new=%d", snapshot, reader2.TotalDocCount())
	}
}

func TestConcurrentAddWithDelete(t *testing.T) {
	writer, dir := newTestWriter(t, 100)
	defer writer.Close()

	const goroutines = 4
	const docsPerGoroutine = 200

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range docsPerGoroutine {
				doc := document.NewDocument()
				doc.AddField("body", fmt.Sprintf("g%d doc %d text", gid, i), document.FieldTypeText)
				doc.AddField("group", fmt.Sprintf("group%d", gid), document.FieldTypeKeyword)
				if err := writer.AddDocument(doc); err != nil {
					t.Errorf("AddDocument error: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	// Delete all docs from group0
	writer.DeleteDocuments("group", "group0")

	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	expected := goroutines * docsPerGoroutine
	if reader.TotalDocCount() != expected {
		t.Errorf("TotalDocCount: got %d, want %d", reader.TotalDocCount(), expected)
	}

	expectedLive := (goroutines - 1) * docsPerGoroutine
	if reader.LiveDocCount() != expectedLive {
		t.Errorf("LiveDocCount: got %d, want %d", reader.LiveDocCount(), expectedLive)
	}
}

func TestConcurrentAddRaceDetector(t *testing.T) {
	writer, _ := newTestWriter(t, 10)
	defer writer.Close()

	const goroutines = 16
	const docsPerGoroutine = 50

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range docsPerGoroutine {
				doc := document.NewDocument()
				doc.AddField("body", fmt.Sprintf("race test g%d d%d", gid, i), document.FieldTypeText)
				writer.AddDocument(doc)
			}
		}(g)
	}
	wg.Wait()

	reader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	expected := goroutines * docsPerGoroutine
	if reader.TotalDocCount() != expected {
		t.Errorf("TotalDocCount: got %d, want %d", reader.TotalDocCount(), expected)
	}
}

func TestConcurrentFlushUnderPressure(t *testing.T) {
	// Small buffer size to force many flushes
	writer, dir := newTestWriter(t, 5)
	defer writer.Close()

	const goroutines = 8
	const docsPerGoroutine = 100

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range docsPerGoroutine {
				doc := document.NewDocument()
				doc.AddField("body", fmt.Sprintf("pressure test g%d d%d with extra words", gid, i), document.FieldTypeText)
				if err := writer.AddDocument(doc); err != nil {
					t.Errorf("AddDocument error: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	expected := goroutines * docsPerGoroutine
	if reader.TotalDocCount() != expected {
		t.Errorf("TotalDocCount: got %d, want %d (data loss!)", reader.TotalDocCount(), expected)
	}
}
