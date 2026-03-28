package index

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

// helper: create a flushed segment on disk and return its info and dir path
func setupRAUSegment(t *testing.T) (*SegmentCommitInfo, string, store.Directory) {
	t.Helper()
	tmpDir := t.TempDir()
	dir, _ := store.NewFSDirectory(tmpDir)
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	writer := NewIndexWriter(dir, fa, 100)

	for _, text := range []string{"hello world", "hello go", "world go"} {
		doc := document.NewDocument()
		doc.AddField("id", text[:5], document.FieldTypeKeyword)
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc)
	}
	writer.Flush()

	info := writer.segmentInfos.Segments[0]
	return info, tmpDir, dir
}

func TestReadersAndUpdatesDelete(t *testing.T) {
	info, dirPath, _ := setupRAUSegment(t)
	rau := NewReadersAndUpdates(info, dirPath)
	defer rau.Close()

	if rau.HasPendingDeletes() {
		t.Error("should not have pending deletes initially")
	}

	// First delete
	if !rau.Delete(0) {
		t.Error("first Delete(0) should return true")
	}
	if !rau.HasPendingDeletes() {
		t.Error("should have pending deletes after Delete")
	}

	// Duplicate delete
	if rau.Delete(0) {
		t.Error("duplicate Delete(0) should return false")
	}
}

func TestReadersAndUpdatesWriteLiveDocs(t *testing.T) {
	info, dirPath, dir := setupRAUSegment(t)
	rau := NewReadersAndUpdates(info, dirPath)
	defer rau.Close()

	// No deletions — should write nothing
	name, err := rau.WriteLiveDocs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if name != "" {
		t.Errorf("expected empty name, got %q", name)
	}

	// Delete and write
	rau.Delete(0)
	rau.Delete(2)
	name, err = rau.WriteLiveDocs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if name == "" {
		t.Error("expected non-empty del file name")
	}
	if !dir.FileExists(name) {
		t.Errorf("expected %s to exist", name)
	}
}

func TestReadersAndUpdatesClose(t *testing.T) {
	info, dirPath, _ := setupRAUSegment(t)
	rau := NewReadersAndUpdates(info, dirPath)

	// Close before opening reader — should be safe
	if err := rau.Close(); err != nil {
		t.Errorf("Close without reader: %v", err)
	}

	// Close after opening reader
	rau2 := NewReadersAndUpdates(info, dirPath)
	rau2.getReader() // force reader open
	if err := rau2.Close(); err != nil {
		t.Errorf("Close with reader: %v", err)
	}
}

func TestReadersAndUpdatesLazyOpen(t *testing.T) {
	info, dirPath, _ := setupRAUSegment(t)
	rau := NewReadersAndUpdates(info, dirPath)
	defer rau.Close()

	// reader should be nil before any access
	if rau.reader != nil {
		t.Error("reader should be nil before first access")
	}

	// Delete doesn't open the reader
	rau.Delete(0)
	if rau.reader != nil {
		t.Error("reader should still be nil after Delete (lazy)")
	}

	// getReader opens the reader
	_, err := rau.getReader()
	if err != nil {
		t.Fatal(err)
	}
	if rau.reader == nil {
		t.Error("reader should be opened after getReader")
	}
}
