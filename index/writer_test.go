package index

import (
	"fmt"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

func TestInvertedIndex(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc0 := document.NewDocument()
	doc0.AddField("title", "The Quick Brown Fox", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("title", "The Lazy Brown Dog", document.FieldTypeText)
	writer.AddDocument(doc1)

	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seg := reader.Leaves()[0].Segment

	// "brown" appears in both documents
	if seg.DocFreq("title", "brown") != 2 {
		t.Fatalf("expected docFreq 2 for 'brown', got %d", seg.DocFreq("title", "brown"))
	}

	// "fox" appears only in doc0
	if seg.DocFreq("title", "fox") != 1 {
		t.Fatalf("expected docFreq 1 for 'fox', got %d", seg.DocFreq("title", "fox"))
	}
	iter := seg.PostingsIterator("title", "fox")
	if !iter.Next() {
		t.Fatal("expected at least one posting for 'fox'")
	}
	if iter.DocID() != 0 {
		t.Errorf("expected docID 0, got %d", iter.DocID())
	}

	// non-existent term
	if seg.DocFreq("title", "cat") != 0 {
		t.Error("expected docFreq 0 for 'cat'")
	}
}

func TestPostingFreqAndPositions(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("body", "the fox and the fox", document.FieldTypeText)
	writer.AddDocument(doc)

	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seg := reader.Leaves()[0].Segment

	iter := seg.PostingsIterator("body", "fox")
	if !iter.Next() {
		t.Fatal("expected at least one posting for 'fox'")
	}
	if iter.Freq() != 2 {
		t.Errorf("expected freq 2, got %d", iter.Freq())
	}
	// "fox" appears at positions 1 and 4
	positions := iter.Positions()
	if positions[0] != 1 || positions[1] != 4 {
		t.Errorf("expected positions [1,4], got %v", positions)
	}
}

func TestSegmentFlush(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	// Buffer size 2: auto-flush after 2 documents
	writer := NewIndexWriter(dir, fa, 2)

	doc0 := document.NewDocument()
	doc0.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("body", "hello go", document.FieldTypeText)
	writer.AddDocument(doc1) // auto-flush here

	doc2 := document.NewDocument()
	doc2.AddField("body", "world go", document.FieldTypeText)
	writer.AddDocument(doc2)

	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	leaves := reader.Leaves()
	if len(leaves) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(leaves))
	}
	if leaves[0].Segment.DocCount() != 2 {
		t.Errorf("segment 0: expected 2 docs, got %d", leaves[0].Segment.DocCount())
	}
	if leaves[1].Segment.DocCount() != 1 {
		t.Errorf("segment 1: expected 1 doc, got %d", leaves[1].Segment.DocCount())
	}
}

func TestDeleteDocument(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc0 := document.NewDocument()
	doc0.AddField("id", "1", document.FieldTypeKeyword)
	doc0.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("id", "2", document.FieldTypeKeyword)
	doc1.AddField("body", "hello go", document.FieldTypeText)
	writer.AddDocument(doc1)

	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}

	// Delete the document with id=1
	if err := writer.DeleteDocuments("id", "1"); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.LiveDocCount() != 1 {
		t.Errorf("expected 1 live doc, got %d", reader.LiveDocCount())
	}
}

func TestFlushEmptyBuffer(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	// Flush with no documents should be a no-op
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(writer.segmentInfos.Segments) != 0 {
		t.Errorf("expected 0 segments after empty flush, got %d", len(writer.segmentInfos.Segments))
	}
}

func TestKeywordField(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("status", "active", document.FieldTypeKeyword)
	writer.AddDocument(doc)

	doc2 := document.NewDocument()
	doc2.AddField("status", "inactive", document.FieldTypeKeyword)
	writer.AddDocument(doc2)

	doc3 := document.NewDocument()
	doc3.AddField("status", "active", document.FieldTypeKeyword)
	writer.AddDocument(doc3)

	writer.Flush()

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seg := reader.Leaves()[0].Segment

	// "active" should match 2 docs (exact match, no analysis)
	if seg.DocFreq("status", "active") != 2 {
		t.Errorf("DocFreq for 'active': got %d, want 2", seg.DocFreq("status", "active"))
	}
	if seg.DocFreq("status", "inactive") != 1 {
		t.Errorf("DocFreq for 'inactive': got %d, want 1", seg.DocFreq("status", "inactive"))
	}
	// Keyword is not analyzed — "Active" should not match
	if seg.DocFreq("status", "Active") != 0 {
		t.Error("keyword field should not be analyzed")
	}
}

func TestStoredFieldForTextType(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc)
	writer.Flush()

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	fields, err := reader.Leaves()[0].Segment.StoredFields(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(fields["body"]) != "hello world" {
		t.Errorf("stored body: got %q, want %q", fields["body"], "hello world")
	}
}

func TestStoredFieldType(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("url", "https://example.com", document.FieldTypeStored)
	doc.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc)
	writer.Flush()

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	fields, err := reader.Leaves()[0].Segment.StoredFields(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(fields["url"]) != "https://example.com" {
		t.Errorf("stored url: got %q, want %q", fields["url"], "https://example.com")
	}
	// Stored field should NOT be in the inverted index
	if reader.Leaves()[0].Segment.DocFreq("url", "https://example.com") != 0 {
		t.Error("stored-only field should not be indexed")
	}
}

func TestDeleteInBufferBeforeFlush(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc0 := document.NewDocument()
	doc0.AddField("id", "1", document.FieldTypeKeyword)
	doc0.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("id", "2", document.FieldTypeKeyword)
	doc1.AddField("body", "hello go", document.FieldTypeText)
	writer.AddDocument(doc1)

	// Delete while still in buffer (before flush)
	writer.DeleteDocuments("id", "1")

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.LiveDocCount() != 1 {
		t.Errorf("LiveDocCount: got %d, want 1", reader.LiveDocCount())
	}
}

func TestDeleteMultipleMatchingDocs(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	// Add docs with duplicate keyword values
	for _, text := range []string{"hello world", "hello go", "hello rust"} {
		doc := document.NewDocument()
		doc.AddField("lang", "en", document.FieldTypeKeyword)
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc)
	}
	doc := document.NewDocument()
	doc.AddField("lang", "ja", document.FieldTypeKeyword)
	doc.AddField("body", "konnichiwa sekai", document.FieldTypeText)
	writer.AddDocument(doc)

	writer.Flush()

	// Delete all "en" docs
	writer.DeleteDocuments("lang", "en")

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 4 {
		t.Errorf("TotalDocCount: got %d, want 4", reader.TotalDocCount())
	}
	if reader.LiveDocCount() != 1 {
		t.Errorf("LiveDocCount: got %d, want 1", reader.LiveDocCount())
	}
}

func TestWriterClose(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("body", "hello", document.FieldTypeText)
	writer.AddDocument(doc)
	writer.Flush()

	if err := writer.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestMultiFieldDocument(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("title", "Go Programming", document.FieldTypeText)
	doc.AddField("body", "Go is a programming language", document.FieldTypeText)
	doc.AddField("id", "doc1", document.FieldTypeKeyword)
	writer.AddDocument(doc)
	writer.Flush()

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seg := reader.Leaves()[0].Segment

	// Each field should have its own inverted index
	if seg.DocFreq("title", "go") != 1 {
		t.Errorf("title DocFreq 'go': got %d, want 1", seg.DocFreq("title", "go"))
	}
	if seg.DocFreq("body", "language") != 1 {
		t.Errorf("body DocFreq 'language': got %d, want 1", seg.DocFreq("body", "language"))
	}
	if seg.DocFreq("id", "doc1") != 1 {
		t.Errorf("id DocFreq 'doc1': got %d, want 1", seg.DocFreq("id", "doc1"))
	}
	// Cross-field query should not match
	if seg.DocFreq("title", "language") != 0 {
		t.Error("'language' should not appear in title field")
	}
}

func TestDeleteAcrossMultipleSegments(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 2)

	// Segment 0: docs with id=a
	doc := document.NewDocument()
	doc.AddField("id", "a", document.FieldTypeKeyword)
	doc.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc)

	doc = document.NewDocument()
	doc.AddField("id", "b", document.FieldTypeKeyword)
	doc.AddField("body", "hello go", document.FieldTypeText)
	writer.AddDocument(doc) // auto-flush (seg0)

	// Segment 1: another doc with id=a
	doc = document.NewDocument()
	doc.AddField("id", "a", document.FieldTypeKeyword)
	doc.AddField("body", "world go", document.FieldTypeText)
	writer.AddDocument(doc)
	writer.Flush() // seg1

	// Delete id=a should affect both segments
	writer.DeleteDocuments("id", "a")

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 3 {
		t.Errorf("TotalDocCount: got %d, want 3", reader.TotalDocCount())
	}
	if reader.LiveDocCount() != 1 {
		t.Errorf("LiveDocCount: got %d, want 1 (only id=b)", reader.LiveDocCount())
	}
}

func TestCommitPersistsDeletes(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc0 := document.NewDocument()
	doc0.AddField("id", "1", document.FieldTypeKeyword)
	doc0.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("id", "2", document.FieldTypeKeyword)
	doc1.AddField("body", "hello go", document.FieldTypeText)
	writer.AddDocument(doc1)

	writer.DeleteDocuments("id", "1")

	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	writer.Close()

	// Reopen from disk
	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 2 {
		t.Errorf("TotalDocCount: got %d, want 2", reader.TotalDocCount())
	}
	// The deletion bitmap should be readable from disk via LiveDocs()
	seg := reader.Leaves()[0].Segment
	liveDocs := seg.LiveDocs()
	if liveDocs == nil {
		t.Fatal("LiveDocs should not be nil for segment with deletions")
	}
	if !liveDocs.Get(0) {
		t.Error("doc 0 (id=1) should be deleted after commit + reopen")
	}
	if liveDocs.Get(1) {
		t.Error("doc 1 (id=2) should not be deleted")
	}
}

// ---------------------------------------------------------------------------
// Tests for NewIndexWriter loading existing state from directory
// ---------------------------------------------------------------------------

// TestNewWriterLoadsExistingSegments verifies that a new IndexWriter
// loads committed segments from disk and can read them via NRT reader.
func TestNewWriterLoadsExistingSegments(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())

	// Session 1: add documents and commit
	w1 := NewIndexWriter(dir, fa, 100)
	doc0 := document.NewDocument()
	doc0.AddField("body", "hello world", document.FieldTypeText)
	w1.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("body", "hello go", document.FieldTypeText)
	w1.AddDocument(doc1)

	if err := w1.Commit(); err != nil {
		t.Fatal(err)
	}
	w1.Close()

	// Session 2: open a new writer on the same directory
	w2 := NewIndexWriter(dir, fa, 100)
	defer w2.Close()

	// The new writer should have loaded the committed segments
	reader, err := OpenNRTReader(w2)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 2 {
		t.Errorf("TotalDocCount: got %d, want 2", reader.TotalDocCount())
	}
	seg := reader.Leaves()[0].Segment
	if seg.DocFreq("body", "hello") != 2 {
		t.Errorf("DocFreq 'hello': got %d, want 2", seg.DocFreq("body", "hello"))
	}
}

// TestNewWriterCanAddDocumentsAfterLoad verifies that after loading
// existing state, the writer can add new documents without segment
// name collisions and the new documents are queryable alongside old ones.
func TestNewWriterCanAddDocumentsAfterLoad(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())

	// Session 1: add and commit
	w1 := NewIndexWriter(dir, fa, 100)
	doc0 := document.NewDocument()
	doc0.AddField("body", "hello world", document.FieldTypeText)
	w1.AddDocument(doc0)
	if err := w1.Commit(); err != nil {
		t.Fatal(err)
	}
	w1.Close()

	// Session 2: open, add more, commit
	w2 := NewIndexWriter(dir, fa, 100)
	doc1 := document.NewDocument()
	doc1.AddField("body", "hello go", document.FieldTypeText)
	w2.AddDocument(doc1)
	if err := w2.Commit(); err != nil {
		t.Fatal(err)
	}
	w2.Close()

	// Session 3: verify all documents are visible
	w3 := NewIndexWriter(dir, fa, 100)
	defer w3.Close()

	reader, err := OpenNRTReader(w3)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 2 {
		t.Errorf("TotalDocCount: got %d, want 2", reader.TotalDocCount())
	}
	if reader.LiveDocCount() != 2 {
		t.Errorf("LiveDocCount: got %d, want 2", reader.LiveDocCount())
	}
}

// TestNewWriterPreservesDeletes verifies that deletions committed in
// a prior session are visible after reopening the writer.
func TestNewWriterPreservesDeletes(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())

	// Session 1: add docs, delete one, commit
	w1 := NewIndexWriter(dir, fa, 100)
	doc0 := document.NewDocument()
	doc0.AddField("id", "1", document.FieldTypeKeyword)
	doc0.AddField("body", "hello world", document.FieldTypeText)
	w1.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("id", "2", document.FieldTypeKeyword)
	doc1.AddField("body", "hello go", document.FieldTypeText)
	w1.AddDocument(doc1)

	w1.DeleteDocuments("id", "1")
	if err := w1.Commit(); err != nil {
		t.Fatal(err)
	}
	w1.Close()

	// Session 2: reopen and verify deletion is preserved
	w2 := NewIndexWriter(dir, fa, 100)
	defer w2.Close()

	reader, err := OpenNRTReader(w2)
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

// TestNewWriterDeletesAcrossSessions verifies that a new writer can
// delete documents from segments committed in a prior session.
func TestNewWriterDeletesAcrossSessions(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())

	// Session 1: add docs and commit
	w1 := NewIndexWriter(dir, fa, 100)
	doc0 := document.NewDocument()
	doc0.AddField("id", "1", document.FieldTypeKeyword)
	doc0.AddField("body", "hello world", document.FieldTypeText)
	w1.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("id", "2", document.FieldTypeKeyword)
	doc1.AddField("body", "hello go", document.FieldTypeText)
	w1.AddDocument(doc1)

	if err := w1.Commit(); err != nil {
		t.Fatal(err)
	}
	w1.Close()

	// Session 2: delete a document from the prior session's segment
	w2 := NewIndexWriter(dir, fa, 100)
	w2.DeleteDocuments("id", "1")
	if err := w2.Commit(); err != nil {
		t.Fatal(err)
	}
	w2.Close()

	// Session 3: verify deletion persisted
	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.LiveDocCount() != 1 {
		t.Errorf("LiveDocCount: got %d, want 1", reader.LiveDocCount())
	}
	seg := reader.Leaves()[0].Segment
	liveDocs := seg.LiveDocs()
	if liveDocs == nil {
		t.Fatal("LiveDocs should not be nil for segment with deletions")
	}
	if !liveDocs.Get(0) {
		t.Error("doc 0 (id=1) should be deleted")
	}
	if liveDocs.Get(1) {
		t.Error("doc 1 (id=2) should not be deleted")
	}
}

// TestNewWriterSegmentNameContinuity verifies that the segment counter
// continues from the highest existing segment number, avoiding name collisions.
func TestNewWriterSegmentNameContinuity(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())

	// Session 1: add docs across multiple segments (bufferSize=1 forces flush per doc)
	w1 := NewIndexWriter(dir, fa, 1)
	for range 3 {
		doc := document.NewDocument()
		doc.AddField("body", "hello", document.FieldTypeText)
		w1.AddDocument(doc) // auto-flush creates _seg0, _seg1, _seg2
	}
	if err := w1.Commit(); err != nil {
		t.Fatal(err)
	}
	w1.Close()

	// Session 2: add another doc — its segment name must not collide
	w2 := NewIndexWriter(dir, fa, 100)
	doc := document.NewDocument()
	doc.AddField("body", "world", document.FieldTypeText)
	w2.AddDocument(doc)
	if err := w2.Commit(); err != nil {
		t.Fatal(err)
	}
	w2.Close()

	// Verify all 4 docs are present across 4 segments
	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 4 {
		t.Errorf("TotalDocCount: got %d, want 4", reader.TotalDocCount())
	}
	leaves := reader.Leaves()
	if len(leaves) != 4 {
		t.Fatalf("expected 4 segments, got %d", len(leaves))
	}

	// Verify no duplicate segment names
	names := make(map[string]bool)
	for _, leaf := range leaves {
		name := leaf.Segment.Name()
		if names[name] {
			t.Errorf("duplicate segment name: %s", name)
		}
		names[name] = true
	}
}

// TestNewWriterMultipleSegmentsLoad verifies that a writer correctly
// loads multiple segments committed in a prior session.
func TestNewWriterMultipleSegmentsLoad(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())

	// Session 1: create multiple segments (bufferSize=2)
	w1 := NewIndexWriter(dir, fa, 2)
	for i, text := range []string{"hello world", "hello go", "world go"} {
		doc := document.NewDocument()
		doc.AddField("id", fmt.Sprintf("%d", i), document.FieldTypeKeyword)
		doc.AddField("body", text, document.FieldTypeText)
		w1.AddDocument(doc)
	}
	if err := w1.Commit(); err != nil {
		t.Fatal(err)
	}
	w1.Close()

	// Session 2: load and verify
	w2 := NewIndexWriter(dir, fa, 100)
	defer w2.Close()

	reader, err := OpenNRTReader(w2)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 3 {
		t.Errorf("TotalDocCount: got %d, want 3", reader.TotalDocCount())
	}
	if len(reader.Leaves()) != 2 {
		t.Errorf("expected 2 segments, got %d", len(reader.Leaves()))
	}
}

// TestNewWriterEmptyDirectory verifies that creating a writer on a
// fresh directory (no existing segments) still works as before.
func TestNewWriterEmptyDirectory(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())

	w := NewIndexWriter(dir, fa, 100)
	defer w.Close()

	doc := document.NewDocument()
	doc.AddField("body", "hello", document.FieldTypeText)
	w.AddDocument(doc)

	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 1 {
		t.Errorf("TotalDocCount: got %d, want 1", reader.TotalDocCount())
	}
}

// TestNewWriterGenerationContinuity verifies that commits after
// reopening use the correct generation number (continuing from the
// prior session's generation, not restarting from 0).
func TestNewWriterGenerationContinuity(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())

	// Session 1: commit (generation becomes 1)
	w1 := NewIndexWriter(dir, fa, 100)
	doc := document.NewDocument()
	doc.AddField("body", "hello", document.FieldTypeText)
	w1.AddDocument(doc)
	if err := w1.Commit(); err != nil {
		t.Fatal(err)
	}
	w1.Close()

	// Session 2: commit again (generation should become 2, not 1)
	w2 := NewIndexWriter(dir, fa, 100)
	doc2 := document.NewDocument()
	doc2.AddField("body", "world", document.FieldTypeText)
	w2.AddDocument(doc2)
	if err := w2.Commit(); err != nil {
		t.Fatal(err)
	}
	w2.Close()

	// Verify: segments_2 should exist (generation 2)
	if !dir.FileExists("segments_2") {
		t.Error("expected segments_2 to exist (generation continuity)")
	}

	// And both commits should be readable
	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 2 {
		t.Errorf("TotalDocCount: got %d, want 2", reader.TotalDocCount())
	}
}

// ---------------------------------------------------------------------------
// Tests for stale file cleanup
// ---------------------------------------------------------------------------

func TestCommitCleansUpOldSegmentsFile(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	// First commit → segments_1
	doc := document.NewDocument()
	doc.AddField("body", "hello", document.FieldTypeText)
	writer.AddDocument(doc)
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	if !dir.FileExists("segments_1") {
		t.Fatal("segments_1 should exist after first commit")
	}

	// Second commit → segments_2, should delete segments_1
	doc2 := document.NewDocument()
	doc2.AddField("body", "world", document.FieldTypeText)
	writer.AddDocument(doc2)
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	if !dir.FileExists("segments_2") {
		t.Error("segments_2 should exist after second commit")
	}
	if dir.FileExists("segments_1") {
		t.Error("segments_1 should have been cleaned up after second commit")
	}
}

func TestCommitCleansUpStalePendingFiles(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())

	// Simulate a leftover pending file from a prior crash
	out, _ := dir.CreateOutput("pending_segments_99")
	out.Write([]byte("stale"))
	out.Close()

	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("body", "hello", document.FieldTypeText)
	writer.AddDocument(doc)
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	if dir.FileExists("pending_segments_99") {
		t.Error("pending_segments_99 should have been cleaned up after commit")
	}
}

func TestNewWriterCleansUpStalePendingFiles(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())

	// Simulate leftover pending files
	out, _ := dir.CreateOutput("pending_segments_1")
	out.Write([]byte("stale"))
	out.Close()
	out2, _ := dir.CreateOutput("pending_segments_5")
	out2.Write([]byte("stale"))
	out2.Close()

	// Creating a new writer should clean up pending files
	_ = NewIndexWriter(dir, fa, 100)

	if dir.FileExists("pending_segments_1") {
		t.Error("pending_segments_1 should have been cleaned up on writer startup")
	}
	if dir.FileExists("pending_segments_5") {
		t.Error("pending_segments_5 should have been cleaned up on writer startup")
	}
}

// ---------------------------------------------------------------------------
// Tests for Merge
// ---------------------------------------------------------------------------

func TestForceMergeToOneSegment(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 2)

	// Create 3 segments
	for _, text := range []string{"hello world", "hello go", "world go"} {
		doc := document.NewDocument()
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc)
	}
	writer.Flush()

	if len(writer.segmentInfos.Segments) < 2 {
		t.Fatalf("expected at least 2 segments before merge, got %d", len(writer.segmentInfos.Segments))
	}

	if err := writer.ForceMerge(1); err != nil {
		t.Fatal(err)
	}

	if len(writer.segmentInfos.Segments) != 1 {
		t.Errorf("expected 1 segment after ForceMerge(1), got %d", len(writer.segmentInfos.Segments))
	}

	// Verify search results are correct after merge
	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 3 {
		t.Errorf("TotalDocCount: got %d, want 3", reader.TotalDocCount())
	}

	seg := reader.Leaves()[0].Segment
	if seg.DocFreq("body", "hello") != 2 {
		t.Errorf("DocFreq 'hello': got %d, want 2", seg.DocFreq("body", "hello"))
	}
	if seg.DocFreq("body", "world") != 2 {
		t.Errorf("DocFreq 'world': got %d, want 2", seg.DocFreq("body", "world"))
	}
	if seg.DocFreq("body", "go") != 2 {
		t.Errorf("DocFreq 'go': got %d, want 2", seg.DocFreq("body", "go"))
	}
}

func TestForceMergeWithDeletions(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 2)

	// Add 3 docs across 2 segments
	doc0 := document.NewDocument()
	doc0.AddField("id", "1", document.FieldTypeKeyword)
	doc0.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("id", "2", document.FieldTypeKeyword)
	doc1.AddField("body", "hello go", document.FieldTypeText)
	writer.AddDocument(doc1) // auto-flush

	doc2 := document.NewDocument()
	doc2.AddField("id", "3", document.FieldTypeKeyword)
	doc2.AddField("body", "world go", document.FieldTypeText)
	writer.AddDocument(doc2)
	writer.Flush()

	// Delete doc with id=1
	writer.DeleteDocuments("id", "1")

	if err := writer.ForceMerge(1); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// After merge, deleted doc should be physically removed
	if reader.TotalDocCount() != 2 {
		t.Errorf("TotalDocCount after merge: got %d, want 2", reader.TotalDocCount())
	}
	if reader.LiveDocCount() != 2 {
		t.Errorf("LiveDocCount after merge: got %d, want 2", reader.LiveDocCount())
	}
}

func TestMaybeMergeWithTieredPolicy(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 1) // flush every doc

	// Create many small segments to trigger merge
	for i := range 15 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("term%d common", i), document.FieldTypeText)
		writer.AddDocument(doc) // auto-flush
	}
	writer.Flush()

	policy := NewTieredMergePolicy()
	policy.SegmentsPerTier = 5
	policy.MaxMergeAtOnce = 5

	segsBefore := len(writer.segmentInfos.Segments)

	if err := writer.MaybeMerge(policy); err != nil {
		t.Fatal(err)
	}

	segsAfter := len(writer.segmentInfos.Segments)
	if segsAfter >= segsBefore {
		t.Errorf("expected fewer segments after MaybeMerge: before=%d, after=%d",
			segsBefore, segsAfter)
	}
}

func TestForceMergeAndCommit(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 2)

	for _, text := range []string{"hello world", "hello go", "world go"} {
		doc := document.NewDocument()
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc)
	}
	writer.Flush()

	if err := writer.ForceMerge(1); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	writer.Close()

	// Reopen and verify
	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 3 {
		t.Errorf("TotalDocCount after commit: got %d, want 3", reader.TotalDocCount())
	}
	if len(reader.Leaves()) != 1 {
		t.Errorf("expected 1 segment after ForceMerge+Commit, got %d", len(reader.Leaves()))
	}
}

func TestCommitPreservesCurrentSegmentFiles(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("body", "hello world", document.FieldTypeText)
	writer.AddDocument(doc)
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	// Collect all files referenced by the current commit
	refs := writer.segmentInfos.ReferencedFiles()

	// Verify all referenced files still exist on disk
	for f := range refs {
		if !dir.FileExists(f) {
			t.Errorf("referenced file %q should still exist after commit", f)
		}
	}
}

func TestCommitCleansUpOrphanedSegmentFiles(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())

	// Create an orphaned segment file (not referenced by any commit)
	out, _ := dir.CreateOutput("_seg99.meta")
	out.Write([]byte("orphaned"))
	out.Close()

	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("body", "hello", document.FieldTypeText)
	writer.AddDocument(doc)
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	if dir.FileExists("_seg99.meta") {
		t.Error("orphaned _seg99.meta should have been cleaned up after commit")
	}

	// Verify real segment files still exist
	for _, info := range writer.segmentInfos.Segments {
		for _, f := range info.Files {
			if !dir.FileExists(f) {
				t.Errorf("current segment file %q should still exist", f)
			}
		}
	}
}

func TestInvertedIndexJapanese(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc0 := document.NewDocument()
	doc0.AddField("title", "東京 大阪 名古屋", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("title", "東京 京都", document.FieldTypeText)
	writer.AddDocument(doc1)

	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seg := reader.Leaves()[0].Segment

	// "東京" appears in both documents
	if seg.DocFreq("title", "東京") != 2 {
		t.Errorf("DocFreq(東京): got %d, want 2", seg.DocFreq("title", "東京"))
	}
	// "名古屋" appears only in doc0
	if seg.DocFreq("title", "名古屋") != 1 {
		t.Errorf("DocFreq(名古屋): got %d, want 1", seg.DocFreq("title", "名古屋"))
	}
	// Non-existent Japanese term
	if seg.DocFreq("title", "福岡") != 0 {
		t.Error("expected docFreq 0 for '福岡'")
	}
}

func TestPostingFreqAndPositionsJapanese(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("body", "東京 大阪 東京", document.FieldTypeText)
	writer.AddDocument(doc)

	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seg := reader.Leaves()[0].Segment

	iter := seg.PostingsIterator("body", "東京")
	if !iter.Next() {
		t.Fatal("expected at least one posting for '東京'")
	}
	if iter.Freq() != 2 {
		t.Errorf("expected freq 2, got %d", iter.Freq())
	}
	// "東京" appears at positions 0 and 2
	positions := iter.Positions()
	if positions[0] != 0 || positions[1] != 2 {
		t.Errorf("expected positions [0,2], got %v", positions)
	}
}

func TestKeywordFieldJapanese(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("category", "技術", document.FieldTypeKeyword)
	writer.AddDocument(doc)

	doc2 := document.NewDocument()
	doc2.AddField("category", "科学", document.FieldTypeKeyword)
	writer.AddDocument(doc2)

	doc3 := document.NewDocument()
	doc3.AddField("category", "技術", document.FieldTypeKeyword)
	writer.AddDocument(doc3)

	writer.Flush()

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seg := reader.Leaves()[0].Segment

	if seg.DocFreq("category", "技術") != 2 {
		t.Errorf("DocFreq(技術): got %d, want 2", seg.DocFreq("category", "技術"))
	}
	if seg.DocFreq("category", "科学") != 1 {
		t.Errorf("DocFreq(科学): got %d, want 1", seg.DocFreq("category", "科学"))
	}
}

func TestStoredFieldJapanese(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("body", "東京タワー スカイツリー", document.FieldTypeText)
	writer.AddDocument(doc)
	writer.Flush()

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	fields, err := reader.Leaves()[0].Segment.StoredFields(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(fields["body"]) != "東京タワー スカイツリー" {
		t.Errorf("stored body: got %q, want %q", fields["body"], "東京タワー スカイツリー")
	}
}

func TestKeywordWithSpaces(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("city", "New York", document.FieldTypeKeyword)
	writer.AddDocument(doc)

	doc2 := document.NewDocument()
	doc2.AddField("city", "Los Angeles", document.FieldTypeKeyword)
	writer.AddDocument(doc2)

	writer.Flush()
	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seg := reader.Leaves()[0].Segment

	// Keyword with spaces should match exactly
	if seg.DocFreq("city", "New York") != 1 {
		t.Errorf("DocFreq(New York): got %d, want 1", seg.DocFreq("city", "New York"))
	}
	// Partial should not match
	if seg.DocFreq("city", "New") != 0 {
		t.Error("keyword 'New' should not match partial")
	}
}

func TestKeywordSpecialChars(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	keywords := []string{"C++", "007", "@#$%", "path\\to\\file", "user@example.com"}
	for _, kw := range keywords {
		doc := document.NewDocument()
		doc.AddField("tag", kw, document.FieldTypeKeyword)
		writer.AddDocument(doc)
	}

	writer.Flush()
	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seg := reader.Leaves()[0].Segment

	for _, kw := range keywords {
		if seg.DocFreq("tag", kw) != 1 {
			t.Errorf("DocFreq(%q): got %d, want 1", kw, seg.DocFreq("tag", kw))
		}
	}
}

func TestStoredFieldSpecialChars(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	values := []string{
		"hello\tworld\nnewline",
		"path\\to\\file",
		"he said \"hello\"",
		"{\"key\": \"value\"}",
		"café résumé naïve",
		"🔍 emoji test 🔎",
		"𠮷野家",
		"ＨＥＬＬＯ",
	}
	for _, val := range values {
		doc := document.NewDocument()
		doc.AddField("data", val, document.FieldTypeStored)
		doc.AddField("id", val, document.FieldTypeKeyword)
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
	seg := reader.Leaves()[0].Segment

	for i, val := range values {
		fields, err := seg.StoredFields(i)
		if err != nil {
			t.Fatalf("StoredFields(%d): %v", i, err)
		}
		if string(fields["data"]) != val {
			t.Errorf("doc %d: stored = %q, want %q", i, fields["data"], val)
		}
	}
}

func TestStoredFieldEmptyString(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("data", "", document.FieldTypeStored)
	doc.AddField("body", "hello", document.FieldTypeText)
	writer.AddDocument(doc)

	writer.Flush()
	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seg := reader.Leaves()[0].Segment

	fields, err := seg.StoredFields(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(fields["data"]) != "" {
		t.Errorf("stored empty string: got %q, want %q", fields["data"], "")
	}
}

func TestDeleteWithSpecialCharKeyword(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc0 := document.NewDocument()
	doc0.AddField("id", "user@example.com", document.FieldTypeKeyword)
	doc0.AddField("body", "first doc", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("id", "C++", document.FieldTypeKeyword)
	doc1.AddField("body", "second doc", document.FieldTypeText)
	writer.AddDocument(doc1)

	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}

	// Delete by special char keyword
	if err := writer.DeleteDocuments("id", "user@example.com"); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.LiveDocCount() != 1 {
		t.Errorf("expected 1 live doc, got %d", reader.LiveDocCount())
	}
}

func TestInvertedIndexAccentedChars(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("body", "Café Résumé Naïve", document.FieldTypeText)
	writer.AddDocument(doc)

	writer.Flush()
	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seg := reader.Leaves()[0].Segment

	// LowerCaseFilter should lowercase accented chars
	if seg.DocFreq("body", "café") != 1 {
		t.Errorf("DocFreq(café): got %d, want 1", seg.DocFreq("body", "café"))
	}
	if seg.DocFreq("body", "résumé") != 1 {
		t.Errorf("DocFreq(résumé): got %d, want 1", seg.DocFreq("body", "résumé"))
	}
}

func TestDeleteDocumentJapanese(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc0 := document.NewDocument()
	doc0.AddField("id", "東京", document.FieldTypeKeyword)
	doc0.AddField("body", "東京タワー", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("id", "大阪", document.FieldTypeKeyword)
	doc1.AddField("body", "大阪城", document.FieldTypeText)
	writer.AddDocument(doc1)

	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}

	// Delete the document with id=東京
	if err := writer.DeleteDocuments("id", "東京"); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.LiveDocCount() != 1 {
		t.Errorf("expected 1 live doc, got %d", reader.LiveDocCount())
	}
}

// TestDeleteThenAdd verifies that when a delete is issued before an add with the
// same ID, the newly added document survives (the delete only affects prior docs).
func TestDeleteThenAdd(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	// First, add a document and flush so it's on disk.
	doc0 := document.NewDocument()
	doc0.AddField("_id", "1", document.FieldTypeKeyword)
	doc0.AddField("body", "original version", document.FieldTypeText)
	writer.AddDocument(doc0)
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}

	// Now delete then re-add (update pattern) without flushing in between.
	writer.DeleteDocuments("_id", "1")

	doc1 := document.NewDocument()
	doc1.AddField("_id", "1", document.FieldTypeKeyword)
	doc1.AddField("body", "updated version", document.FieldTypeText)
	writer.AddDocument(doc1)

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.LiveDocCount() != 1 {
		t.Errorf("LiveDocCount: got %d, want 1", reader.LiveDocCount())
	}
}

// TestAddThenDelete verifies that when a document is added and then deleted
// (both before flush), the document is properly deleted.
func TestAddThenDelete(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("_id", "1", document.FieldTypeKeyword)
	doc.AddField("body", "some content", document.FieldTypeText)
	writer.AddDocument(doc)

	writer.DeleteDocuments("_id", "1")

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.LiveDocCount() != 0 {
		t.Errorf("LiveDocCount: got %d, want 0", reader.LiveDocCount())
	}
}

// TestUpdateSameDocTwice verifies that two consecutive updates to the same
// document ID (both before flush) result in only the latest version surviving.
func TestUpdateSameDocTwice(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, fa, 100)

	// First update: delete + add v1
	writer.DeleteDocuments("_id", "1")

	v1 := document.NewDocument()
	v1.AddField("_id", "1", document.FieldTypeKeyword)
	v1.AddField("body", "version one", document.FieldTypeText)
	writer.AddDocument(v1)

	// Second update: delete + add v2
	writer.DeleteDocuments("_id", "1")

	v2 := document.NewDocument()
	v2.AddField("_id", "1", document.FieldTypeKeyword)
	v2.AddField("body", "version two", document.FieldTypeText)
	writer.AddDocument(v2)

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Only v2 should survive
	if reader.LiveDocCount() != 1 {
		t.Errorf("LiveDocCount: got %d, want 1", reader.LiveDocCount())
	}

	// Verify "version one" docs are all deleted via liveDocs
	for _, leaf := range reader.Leaves() {
		seg := leaf.Segment
		liveDocs := seg.LiveDocs()
		iter := seg.PostingsIterator("body", "one")
		for iter.Next() {
			if liveDocs == nil || !liveDocs.Get(iter.DocID()) {
				t.Errorf("doc %d with 'one' should be deleted", iter.DocID())
			}
		}
	}
}

func TestExecuteMerge_DeletesStaleFiles(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))

	w := NewIndexWriter(dir, fa, 2) // flush every 2 docs
	defer w.Close()

	// Create 4 segments (8 docs, 2 per segment)
	for i := range 8 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("word%d", i), document.FieldTypeText)
		if err := w.AddDocument(doc); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	// Force merge all segments into 1
	if err := w.ForceMerge(1); err != nil {
		t.Fatal(err)
	}

	// Without calling Commit, stale files from merged-away segments
	// should already be deleted
	postFiles, _ := dir.ListAll()
	referenced := w.segmentInfos.ReferencedFiles()

	for _, f := range postFiles {
		if len(f) > 4 && f[:4] == "_seg" && !referenced[f] {
			t.Errorf("stale file %s not cleaned up after merge", f)
		}
	}
}
