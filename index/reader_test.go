package index

import (
	"fmt"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

func TestMultiSegmentSearch(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 2)

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
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 2)

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
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

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
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 2)

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
	if string(fields["body"]) != "hello world" {
		t.Errorf("global doc 0 body: got %q, want %q", fields["body"], "hello world")
	}

	// Doc 2 in seg1 (global docID 2, local docID 0 in seg1)
	fields = reader.GetStoredFields(2)
	if fields == nil {
		t.Fatal("expected stored fields for global docID 2")
	}
	if string(fields["body"]) != "world go" {
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
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

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

func TestCommitAndOpenDirectoryReader(t *testing.T) {
	tmpDir := t.TempDir()
	dir, _ := store.NewFSDirectory(tmpDir)

	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	writer := NewIndexWriter(dir, fa, 2)

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
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	writer := NewIndexWriter(dir, fa, 100)

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
	liveDocs := seg.LiveDocs()
	if liveDocs == nil {
		t.Fatal("LiveDocs should not be nil for segment with deletions")
	}
	if !liveDocs.Get(1) {
		t.Error("doc 1 (id=2) should be deleted")
	}
	if liveDocs.Get(0) {
		t.Error("doc 0 (id=1) should not be deleted")
	}
}

func TestDirectoryReaderMultipleCommits(t *testing.T) {
	tmpDir := t.TempDir()
	dir, _ := store.NewFSDirectory(tmpDir)
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))

	// First commit
	writer := NewIndexWriter(dir, fa, 100)
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

	// Second commit with new writer (loads existing state automatically)
	writer2 := NewIndexWriter(dir, fa, 100)

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
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	writer := NewIndexWriter(dir, fa, 100)

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
	if string(fields["body"]) != "hello world" {
		t.Errorf("NRT stored field: got %q, want %q", fields["body"], "hello world")
	}
}

func TestNRTReaderWithDeletions(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	writer := NewIndexWriter(dir, fa, 100)

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

// TestNRTReaderProtectsFilesFromDeletion verifies that segment files
// are not deleted while an NRT reader still holds references to them.
func TestNRTReaderProtectsFilesFromDeletion(t *testing.T) {
	tmpDir := t.TempDir()
	dir, _ := store.NewFSDirectory(tmpDir)
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	// bufferSize=1 so each doc creates its own segment
	writer := NewIndexWriter(dir, fa, 1)

	// Add initial documents and commit
	for i := range 3 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("doc %d", i), document.FieldTypeText)
		writer.AddDocument(doc)
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	// Open an NRT reader — this should protect current segment files
	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}

	// Collect segment files the reader depends on
	files, _ := dir.ListAll()
	var segFiles []string
	for _, f := range files {
		if len(f) > 0 && f[0] == '_' {
			segFiles = append(segFiles, f)
		}
	}
	if len(segFiles) == 0 {
		t.Fatal("expected segment files to exist")
	}

	// Force merge to create new segment and make old ones stale
	if err := writer.ForceMerge(1); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	// Old segment files should still exist because the reader holds references
	for _, f := range segFiles {
		if !dir.FileExists(f) {
			t.Errorf("expected %s to still exist while reader is open", f)
		}
	}

	// The reader should still be functional
	if reader.TotalDocCount() != 3 {
		t.Errorf("expected 3 docs, got %d", reader.TotalDocCount())
	}

	// Close the reader — old files should now be deleted
	reader.Close()

	for _, f := range segFiles {
		if dir.FileExists(f) {
			t.Errorf("expected %s to be deleted after reader closed", f)
		}
	}

	writer.Close()
}
