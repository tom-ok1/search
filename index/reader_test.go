package index

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

func TestMultiSegmentSearch(t *testing.T) {
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, analyzer, 2)

	// Add 3 documents (auto-flush after 2nd -> 2 segments)
	texts := []string{"hello world", "hello go", "world go"}
	for _, text := range texts {
		doc := document.NewDocument()
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
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
