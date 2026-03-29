package search

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/store"
)

func TestMatchNoneQuery(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	fa := analysis.NewFieldAnalyzers(analysis.DefaultRegistry().Get("standard"))
	w := index.NewIndexWriter(dir, fa, 100)

	for i := 0; i < 5; i++ {
		doc := document.NewDocument()
		doc.AddField("body", "hello world", document.FieldTypeText)
		w.AddDocument(doc)
	}
	w.Flush()

	reader, _ := index.OpenNRTReader(w)
	defer reader.Close()
	defer w.Close()

	searcher := NewIndexSearcher(reader)
	q := NewMatchNoneQuery()
	collector := NewTopKCollector(10)
	results := searcher.Search(q, collector)

	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
	if collector.TotalHits() != 0 {
		t.Errorf("expected 0 total hits, got %d", collector.TotalHits())
	}
}
