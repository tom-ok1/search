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

func TestDirectoryReaderWithDeletions(t *testing.T) {
	tmpDir := t.TempDir()
	dir, _ := store.NewFSDirectory(tmpDir)
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	writer := NewIndexWriter(dir, analyzer, 100)

	doc0 := document.NewDocument()
	doc0.AddField("id", "1", document.FieldTypeKeyword)
	doc0.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("id", "2", document.FieldTypeKeyword)
	doc1.AddField("body", "hello go", document.FieldTypeText)
	writer.AddDocument(doc1)

	doc2 := document.NewDocument()
	doc2.AddField("id", "3", document.FieldTypeKeyword)
	doc2.AddField("body", "world go", document.FieldTypeText)
	writer.AddDocument(doc2)

	writer.DeleteDocuments("id", "2")

	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	writer.Close()

	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 3 {
		t.Errorf("TotalDocCount: got %d, want 3", reader.TotalDocCount())
	}

	seg := reader.Leaves()[0].Segment
	if !seg.IsDeleted(1) {
		t.Error("doc 1 (id=2) should be deleted")
	}
	if seg.IsDeleted(0) {
		t.Error("doc 0 (id=1) should not be deleted")
	}
}

func TestDirectoryReaderMultipleCommits(t *testing.T) {
	tmpDir := t.TempDir()
	dir, _ := store.NewFSDirectory(tmpDir)
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)

	// First commit
	writer := NewIndexWriter(dir, analyzer, 100)
	doc := document.NewDocument()
	doc.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc)
	writer.Commit()
	writer.Close()

	reader1, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	if reader1.TotalDocCount() != 1 {
		t.Errorf("after commit 1: TotalDocCount got %d, want 1", reader1.TotalDocCount())
	}
	reader1.Close()

	// Second commit with new writer
	writer2 := NewIndexWriter(dir, analyzer, 100)
	// Load existing segment infos
	si, err := ReadLatestSegmentInfos(dir)
	if err != nil {
		t.Fatal(err)
	}
	writer2.segmentInfos = si
	writer2.segmentCounter = len(si.Segments)

	doc2 := document.NewDocument()
	doc2.AddField("body", "hello go", document.FieldTypeText)
	writer2.AddDocument(doc2)
	writer2.Commit()
	writer2.Close()

	reader2, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader2.Close()

	if reader2.TotalDocCount() != 2 {
		t.Errorf("after commit 2: TotalDocCount got %d, want 2", reader2.TotalDocCount())
	}
}

func TestOpenDirectoryReaderNoSegments(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	_, err := OpenDirectoryReader(dir)
	if err == nil {
		t.Error("expected error when no segments file exists")
	}
}

func TestNRTReaderReflectsRecentWrites(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	writer := NewIndexWriter(dir, analyzer, 100)

	doc := document.NewDocument()
	doc.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc)

	// NRT reader should see the doc even without commit
	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 1 {
		t.Errorf("NRT TotalDocCount: got %d, want 1", reader.TotalDocCount())
	}

	fields := reader.GetStoredFields(0)
	if fields["body"] != "hello world" {
		t.Errorf("NRT stored field: got %q, want %q", fields["body"], "hello world")
	}
}

func TestNRTReaderWithDeletions(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	writer := NewIndexWriter(dir, analyzer, 100)

	doc0 := document.NewDocument()
	doc0.AddField("id", "a", document.FieldTypeKeyword)
	doc0.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("id", "b", document.FieldTypeKeyword)
	doc1.AddField("body", "hello go", document.FieldTypeText)
	writer.AddDocument(doc1)

	writer.DeleteDocuments("id", "a")

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 2 {
		t.Errorf("TotalDocCount: got %d, want 2", reader.TotalDocCount())
	}
	if reader.LiveDocCount() != 1 {
		t.Errorf("LiveDocCount: got %d, want 1", reader.LiveDocCount())
	}
}
