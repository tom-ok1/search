package index

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

// helper: create a DiskSegment with test documents
func createTestDiskSegment(t *testing.T) *DiskSegment {
	t.Helper()
	tmpDir := t.TempDir()
	dir, _ := store.NewFSDirectory(tmpDir)
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	writer := NewIndexWriter(dir, analyzer, 100)

	docs := []struct {
		id   string
		body string
	}{
		{"1", "hello world"},
		{"2", "hello go"},
		{"3", "world go"},
		{"4", "hello world go"},
	}
	for _, d := range docs {
		doc := document.NewDocument()
		doc.AddField("id", d.id, document.FieldTypeKeyword)
		doc.AddField("body", d.body, document.FieldTypeText)
		writer.AddDocument(doc)
	}
	writer.Flush()

	ds, err := OpenDiskSegment(tmpDir, writer.segmentInfos.Segments[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

func TestLiveDocsSegmentReaderDelegation(t *testing.T) {
	ds := createTestDiskSegment(t)
	defer ds.Close()

	liveDocs := NewBitset(ds.DocCount())
	liveDocs.Set(1) // mark doc 1 as deleted

	reader := &LiveDocsSegmentReader{inner: ds, liveDocs: liveDocs}

	if reader.Name() != ds.Name() {
		t.Errorf("Name: got %q, want %q", reader.Name(), ds.Name())
	}
	if reader.DocCount() != ds.DocCount() {
		t.Errorf("DocCount: got %d, want %d", reader.DocCount(), ds.DocCount())
	}
}

func TestLiveDocsSegmentReaderLiveDocs(t *testing.T) {
	ds := createTestDiskSegment(t)
	defer ds.Close()

	liveDocs := NewBitset(ds.DocCount())
	liveDocs.Set(0)
	liveDocs.Set(2)

	reader := &LiveDocsSegmentReader{inner: ds, liveDocs: liveDocs}

	got := reader.LiveDocs()
	if got == nil {
		t.Fatal("LiveDocs should not be nil")
	}
	if !got.Get(0) {
		t.Error("doc 0 should be deleted")
	}
	if got.Get(1) {
		t.Error("doc 1 should not be deleted")
	}
	if !got.Get(2) {
		t.Error("doc 2 should be deleted")
	}
	if got.Get(3) {
		t.Error("doc 3 should not be deleted")
	}

	// Verify live doc count via LiveDocs
	expectedLive := ds.DocCount() - got.Count()
	if expectedLive != 2 {
		t.Errorf("live doc count: got %d, want 2", expectedLive)
	}
}

func TestLiveDocsSegmentReaderDocFreq(t *testing.T) {
	ds := createTestDiskSegment(t)
	defer ds.Close()

	liveDocs := NewBitset(ds.DocCount())
	reader := &LiveDocsSegmentReader{inner: ds, liveDocs: liveDocs}

	// DocFreq is delegated to inner (doesn't filter by liveDocs)
	if reader.DocFreq("body", "hello") != ds.DocFreq("body", "hello") {
		t.Error("DocFreq should be delegated to inner")
	}
}

func TestLiveDocsSegmentReaderStoredFields(t *testing.T) {
	ds := createTestDiskSegment(t)
	defer ds.Close()

	liveDocs := NewBitset(ds.DocCount())
	reader := &LiveDocsSegmentReader{inner: ds, liveDocs: liveDocs}

	// StoredFields is delegated to inner
	fields, err := reader.StoredFields(0)
	if err != nil {
		t.Fatal(err)
	}
	innerFields, _ := ds.StoredFields(0)
	if string(fields["body"]) != string(innerFields["body"]) {
		t.Errorf("StoredFields delegation mismatch")
	}
}

func TestLiveDocsSegmentReaderPostingsIterator(t *testing.T) {
	ds := createTestDiskSegment(t)
	defer ds.Close()

	liveDocs := NewBitset(ds.DocCount())
	reader := &LiveDocsSegmentReader{inner: ds, liveDocs: liveDocs}

	// PostingsIterator is delegated to inner
	iter := reader.PostingsIterator("body", "hello")
	count := 0
	for iter.Next() {
		count++
	}
	if count == 0 {
		t.Error("expected some postings for 'hello'")
	}
}

func TestLiveDocsSegmentReaderFieldLength(t *testing.T) {
	ds := createTestDiskSegment(t)
	defer ds.Close()

	liveDocs := NewBitset(ds.DocCount())
	reader := &LiveDocsSegmentReader{inner: ds, liveDocs: liveDocs}

	if reader.FieldLength("body", 0) != ds.FieldLength("body", 0) {
		t.Error("FieldLength should be delegated to inner")
	}
}

func TestLiveDocsSegmentReaderTotalFieldLength(t *testing.T) {
	ds := createTestDiskSegment(t)
	defer ds.Close()

	liveDocs := NewBitset(ds.DocCount())
	reader := &LiveDocsSegmentReader{inner: ds, liveDocs: liveDocs}

	if reader.TotalFieldLength("body") != ds.TotalFieldLength("body") {
		t.Error("TotalFieldLength should be delegated to inner")
	}
}

func TestLiveDocsSegmentReaderClose(t *testing.T) {
	ds := createTestDiskSegment(t)
	defer ds.Close()

	liveDocs := NewBitset(ds.DocCount())
	reader := &LiveDocsSegmentReader{inner: ds, liveDocs: liveDocs}

	// Close is a no-op — should not error
	if err := reader.Close(); err != nil {
		t.Errorf("Close: unexpected error: %v", err)
	}
}
