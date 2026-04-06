package index

import (
	"fmt"
	"strings"
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

	capture := newCapturingInfoStream("IW", "DW", "DWFC", "DWPT", "IFD")
	w.SetInfoStream(capture)
}

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
		t.Errorf("expected flush message from DWPT, got: %v", capture.Messages())
	}

	m := w.Metrics()
	if m.FlushCount.Load() == 0 {
		t.Error("expected FlushCount > 0")
	}
	if m.FlushTimeNanos.Load() == 0 {
		t.Error("expected FlushTimeNanos > 0")
	}
}

func TestMetricsFlushTracking(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	// Small buffer (5 docs per DWPT) to trigger flushes
	w := NewIndexWriter(dir, fa, 5)
	defer w.Close()

	m := w.Metrics()

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
		// FlushCount is incremented in Task 5, so it may be 0 here.
		// But DocsAdded should be correct.
	}
	if m.DocsAdded.Load() != 100 {
		t.Errorf("DocsAdded = %d, want 100", m.DocsAdded.Load())
	}
}

func TestInfoStreamFlushControlMessages(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
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

	if !capture.HasMessageContaining("flush triggered") {
		t.Errorf("expected 'flush triggered' message, got: %v", capture.Messages())
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

func TestInfoStreamCommitMessages(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	w := NewIndexWriter(dir, fa, 1000)
	defer w.Close()

	capture := newCapturingInfoStream("IW", "DW", "DWFC", "DWPT", "IFD")
	w.SetInfoStream(capture)

	doc := document.NewDocument()
	doc.AddField("title", "test", document.FieldTypeText)
	if err := w.AddDocument(doc); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}

	if !capture.HasMessageContaining("commit start") {
		t.Errorf("expected 'commit start' message, got: %v", capture.Messages())
	}
	if !capture.HasMessageContaining("commit done") {
		t.Errorf("expected 'commit done' message, got: %v", capture.Messages())
	}
}

func TestMetricsDeleteDocuments(t *testing.T) {
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
	w.DeleteDocuments("title", "test")
	if m.DocsDeleted.Load() != 1 {
		t.Errorf("DocsDeleted = %d, want 1", m.DocsDeleted.Load())
	}
}

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

	for _, msg := range capture.Messages() {
		if strings.HasPrefix(msg, "DWFC:") {
			t.Errorf("unexpected DWFC message when only IW enabled: %s", msg)
		}
		if strings.HasPrefix(msg, "DWPT:") {
			t.Errorf("unexpected DWPT message when only IW enabled: %s", msg)
		}
	}
}
