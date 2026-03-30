package search

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/store"
)

func TestFieldExistsQuery_Norms(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	writer := index.NewIndexWriter(dir, fa, 1000)

	doc0 := document.NewDocument()
	doc0.AddField("_id", "0", document.FieldTypeKeyword)
	doc0.AddField("title", "hello world", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("_id", "1", document.FieldTypeKeyword)
	// doc1 has no "title" field
	writer.AddDocument(doc1)

	doc2 := document.NewDocument()
	doc2.AddField("_id", "2", document.FieldTypeKeyword)
	doc2.AddField("title", "goodbye", document.FieldTypeText)
	writer.AddDocument(doc2)

	writer.Flush()
	reader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	searcher := NewIndexSearcher(reader)
	q := NewFieldExistsQuery("title")
	collector := NewTopKCollector(10)
	results := searcher.Search(q, collector)

	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}

	ids := map[int]bool{results[0].DocID: true, results[1].DocID: true}
	if !ids[0] || !ids[2] {
		t.Errorf("expected docIDs {0, 2}, got %v", ids)
	}
}

func TestFieldExistsQuery_DocValues(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	writer := index.NewIndexWriter(dir, fa, 1000)

	doc0 := document.NewDocument()
	doc0.AddField("_id", "0", document.FieldTypeKeyword)
	doc0.AddField("status", "active", document.FieldTypeKeyword)
	doc0.AddSortedDocValuesField("status", "active")
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("_id", "1", document.FieldTypeKeyword)
	// doc1 has no "status" field
	writer.AddDocument(doc1)

	doc2 := document.NewDocument()
	doc2.AddField("_id", "2", document.FieldTypeKeyword)
	doc2.AddField("status", "inactive", document.FieldTypeKeyword)
	doc2.AddSortedDocValuesField("status", "inactive")
	writer.AddDocument(doc2)

	writer.Flush()
	reader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	searcher := NewIndexSearcher(reader)
	q := NewFieldExistsQuery("status")
	collector := NewTopKCollector(10)
	results := searcher.Search(q, collector)

	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}

	ids := map[int]bool{results[0].DocID: true, results[1].DocID: true}
	if !ids[0] || !ids[2] {
		t.Errorf("expected docIDs {0, 2}, got %v", ids)
	}
}
