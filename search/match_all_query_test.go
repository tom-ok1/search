package search

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/store"
)

func TestMatchAllQuery(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	fa := analysis.NewFieldAnalyzers(analysis.DefaultRegistry().Get("standard"))
	w := index.NewIndexWriter(dir, fa, 100)

	for i := 0; i < 5; i++ {
		doc := document.NewDocument()
		doc.AddField("title", "doc", document.FieldTypeText)
		w.AddDocument(doc)
	}
	w.Flush()

	reader, _ := index.OpenNRTReader(w)
	defer reader.Close()
	defer w.Close()

	searcher := NewIndexSearcher(reader)
	query := NewMatchAllQuery()
	collector := NewTopKCollector(10)
	results := searcher.Search(query, collector)

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Score != 1.0 {
			t.Errorf("expected score 1.0, got %f", r.Score)
		}
	}
}
