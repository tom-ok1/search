package index

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

func newTestAnalyzer() *analysis.Analyzer {
	return analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
}

func TestDWPTAddDocument(t *testing.T) {
	dwpt := newDWPT("_seg0", newTestAnalyzer())

	doc := document.NewDocument()
	doc.AddField("body", "hello world", document.FieldTypeText)
	if _, err := dwpt.addDocument(doc); err != nil {
		t.Fatal(err)
	}

	if dwpt.segment.docCount != 1 {
		t.Errorf("docCount: got %d, want 1", dwpt.segment.docCount)
	}
	fi, exists := dwpt.segment.fields["body"]
	if !exists {
		t.Fatal("expected 'body' field to exist")
	}
	if _, ok := fi.postings["hello"]; !ok {
		t.Error("expected posting for 'hello'")
	}
	if _, ok := fi.postings["world"]; !ok {
		t.Error("expected posting for 'world'")
	}
}

func TestDWPTAddMultipleDocuments(t *testing.T) {
	dwpt := newDWPT("_seg0", newTestAnalyzer())

	doc0 := document.NewDocument()
	doc0.AddField("title", "Go Programming", document.FieldTypeText)
	doc0.AddField("id", "doc0", document.FieldTypeKeyword)
	doc0.AddField("url", "https://example.com", document.FieldTypeStored)
	dwpt.addDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("title", "Rust Programming", document.FieldTypeText)
	doc1.AddField("id", "doc1", document.FieldTypeKeyword)
	dwpt.addDocument(doc1)

	if dwpt.segment.docCount != 2 {
		t.Errorf("docCount: got %d, want 2", dwpt.segment.docCount)
	}

	// Check stored fields
	if dwpt.segment.storedFields[0]["url"] != "https://example.com" {
		t.Error("stored field 'url' missing for doc0")
	}
	if dwpt.segment.storedFields[0]["title"] != "Go Programming" {
		t.Error("stored field 'title' missing for doc0")
	}

	// Check keyword field
	fi := dwpt.segment.fields["id"]
	if fi == nil {
		t.Fatal("expected 'id' field")
	}
	if len(fi.postings["doc0"].Postings) != 1 {
		t.Error("expected 1 posting for keyword 'doc0'")
	}
}

func TestDWPTFlush(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	dwpt := newDWPT("_seg0", newTestAnalyzer())

	doc := document.NewDocument()
	doc.AddField("body", "hello world", document.FieldTypeText)
	dwpt.addDocument(doc)

	doc2 := document.NewDocument()
	doc2.AddField("body", "hello go", document.FieldTypeText)
	dwpt.addDocument(doc2)

	info, err := dwpt.flush(dir)
	if err != nil {
		t.Fatal(err)
	}

	if info == nil {
		t.Fatal("expected non-nil SegmentCommitInfo")
	}
	if info.Name != "_seg0" {
		t.Errorf("name: got %q, want _seg0", info.Name)
	}
	if info.MaxDoc != 2 {
		t.Errorf("MaxDoc: got %d, want 2", info.MaxDoc)
	}
	if len(info.Files) == 0 {
		t.Error("expected files to be created")
	}

	// Verify files exist on disk
	for _, f := range info.Files {
		if !dir.FileExists(f) {
			t.Errorf("expected file %q to exist", f)
		}
	}
}

func TestDWPTEstimateBytesUsed(t *testing.T) {
	dwpt := newDWPT("_seg0", newTestAnalyzer())

	if dwpt.estimateBytesUsed() != 0 {
		t.Error("expected 0 bytes before adding docs")
	}

	doc := document.NewDocument()
	doc.AddField("body", "hello world", document.FieldTypeText)
	dwpt.addDocument(doc)

	bytes1 := dwpt.estimateBytesUsed()
	if bytes1 <= 0 {
		t.Errorf("expected positive bytes after adding doc, got %d", bytes1)
	}

	doc2 := document.NewDocument()
	doc2.AddField("body", "another document with more text for testing", document.FieldTypeText)
	dwpt.addDocument(doc2)

	bytes2 := dwpt.estimateBytesUsed()
	if bytes2 <= bytes1 {
		t.Errorf("expected bytes to grow: %d -> %d", bytes1, bytes2)
	}
}
