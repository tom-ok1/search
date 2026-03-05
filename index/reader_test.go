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

func TestIndexReaderTotalDocCount(t *testing.T) {
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, analyzer, 2)

	texts := []string{"hello world", "hello go", "world go"}
	for _, text := range texts {
		doc := document.NewDocument()
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc)
	}
	writer.Flush()

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 3 {
		t.Errorf("TotalDocCount: got %d, want 3", reader.TotalDocCount())
	}
}

func TestIndexReaderLiveDocCountWithDeletions(t *testing.T) {
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, analyzer, 100)

	ids := []string{"1", "2", "3"}
	texts := []string{"hello world", "hello go", "world go"}
	for i, text := range texts {
		doc := document.NewDocument()
		doc.AddField("id", ids[i], document.FieldTypeKeyword)
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc)
	}
	writer.Flush()

	writer.DeleteDocuments("id", "1")

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 3 {
		t.Errorf("TotalDocCount: got %d, want 3", reader.TotalDocCount())
	}
	if reader.LiveDocCount() != 2 {
		t.Errorf("LiveDocCount: got %d, want 2", reader.LiveDocCount())
	}
}

func TestIndexReaderGetStoredFields(t *testing.T) {
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, analyzer, 2)

	texts := []string{"hello world", "hello go", "world go"}
	for _, text := range texts {
		doc := document.NewDocument()
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc)
	}
	writer.Flush()

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Doc 0 in seg0
	fields := reader.GetStoredFields(0)
	if fields == nil {
		t.Fatal("expected stored fields for global docID 0")
	}
	if fields["body"] != "hello world" {
		t.Errorf("global doc 0 body: got %q, want %q", fields["body"], "hello world")
	}

	// Doc 2 in seg1 (global docID 2, local docID 0 in seg1)
	fields = reader.GetStoredFields(2)
	if fields == nil {
		t.Fatal("expected stored fields for global docID 2")
	}
	if fields["body"] != "world go" {
		t.Errorf("global doc 2 body: got %q, want %q", fields["body"], "world go")
	}

	// Out-of-range global docID
	fields = reader.GetStoredFields(100)
	if fields != nil {
		t.Error("expected nil for out-of-range global docID")
	}
}

func TestIndexReaderEmptyReader(t *testing.T) {
	reader := NewIndexReader(nil)
	defer reader.Close()

	if reader.TotalDocCount() != 0 {
		t.Errorf("TotalDocCount: got %d, want 0", reader.TotalDocCount())
	}
	if reader.LiveDocCount() != 0 {
		t.Errorf("LiveDocCount: got %d, want 0", reader.LiveDocCount())
	}
	if len(reader.Leaves()) != 0 {
		t.Errorf("Leaves: got %d, want 0", len(reader.Leaves()))
	}
	if reader.GetStoredFields(0) != nil {
		t.Error("expected nil stored fields from empty reader")
	}
}

func TestIndexReaderSingleSegment(t *testing.T) {
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, analyzer, 100)

	doc := document.NewDocument()
	doc.AddField("body", "single doc", document.FieldTypeText)
	writer.AddDocument(doc)
	writer.Flush()

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	leaves := reader.Leaves()
	if len(leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(leaves))
	}
	if leaves[0].DocBase != 0 {
		t.Errorf("DocBase: got %d, want 0", leaves[0].DocBase)
	}
	if reader.TotalDocCount() != 1 {
		t.Errorf("TotalDocCount: got %d, want 1", reader.TotalDocCount())
	}
}
