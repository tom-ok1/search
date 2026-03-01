package index

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
)

func TestSegmentFlush(t *testing.T) {
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	// Buffer size 2: auto-flush after 2 documents
	writer := NewIndexWriter(analyzer, 2)

	doc0 := document.NewDocument()
	doc0.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("body", "hello go", document.FieldTypeText)
	writer.AddDocument(doc1) // auto-flush here

	doc2 := document.NewDocument()
	doc2.AddField("body", "world go", document.FieldTypeText)
	writer.AddDocument(doc2)

	writer.Flush()

	segments := writer.Segments()
	if len(segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(segments))
	}
	if segments[0].docCount != 2 {
		t.Errorf("segment 0: expected 2 docs, got %d", segments[0].docCount)
	}
	if segments[1].docCount != 1 {
		t.Errorf("segment 1: expected 1 doc, got %d", segments[1].docCount)
	}
}

func TestDeleteDocument(t *testing.T) {
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	writer := NewIndexWriter(analyzer, 100)

	doc0 := document.NewDocument()
	doc0.AddField("id", "1", document.FieldTypeKeyword)
	doc0.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("id", "2", document.FieldTypeKeyword)
	doc1.AddField("body", "hello go", document.FieldTypeText)
	writer.AddDocument(doc1)

	writer.Flush()

	// Delete the document with id=1
	writer.DeleteDocuments("id", "1")

	reader := NewIndexReader(writer.Segments())
	if reader.LiveDocCount() != 1 {
		t.Errorf("expected 1 live doc, got %d", reader.LiveDocCount())
	}
}

func TestMultiSegmentSearch(t *testing.T) {
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	writer := NewIndexWriter(analyzer, 2)

	// Add 3 documents (auto-flush after 2nd -> 2 segments)
	texts := []string{"hello world", "hello go", "world go"}
	for _, text := range texts {
		doc := document.NewDocument()
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc)
	}
	writer.Flush()

	reader := NewIndexReader(writer.Segments())
	leaves := reader.Leaves()

	// Should have 2 segments
	if len(leaves) != 2 {
		t.Fatalf("expected 2 leaves, got %d", len(leaves))
	}

	// DocBase should be correct
	if leaves[0].DocBase != 0 {
		t.Errorf("leaf 0 docBase: expected 0, got %d", leaves[0].DocBase)
	}
	if leaves[1].DocBase != 2 {
		t.Errorf("leaf 1 docBase: expected 2, got %d", leaves[1].DocBase)
	}
}
