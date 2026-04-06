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
