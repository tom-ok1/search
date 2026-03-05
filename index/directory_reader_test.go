package index

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

func TestCommitAndOpenDirectoryReader(t *testing.T) {
	tmpDir := t.TempDir()
	dir, _ := store.NewFSDirectory(tmpDir)

	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	writer := NewIndexWriter(dir, analyzer, 2)

	docs := []string{"hello world", "hello go", "world go"}
	for _, text := range docs {
		doc := document.NewDocument()
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc)
	}

	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	// Open from committed index on disk
	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 3 {
		t.Errorf("expected 3 docs, got %d", reader.TotalDocCount())
	}

	leaves := reader.Leaves()
	if len(leaves) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(leaves))
	}

	// Verify segments_N file exists
	if !dir.FileExists("segments_1") {
		t.Error("expected segments_1 file to exist")
	}
}
