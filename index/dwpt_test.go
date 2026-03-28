package index

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

func newTestFieldAnalyzers() *analysis.FieldAnalyzers {
	return analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{}),
	)
}

func TestDWPTAddDocument(t *testing.T) {
	dwpt := newDWPT("_seg0", newTestFieldAnalyzers(), newDeleteQueue())

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
	dwpt := newDWPT("_seg0", newTestFieldAnalyzers(), newDeleteQueue())

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
	if string(dwpt.segment.storedFields[0]["url"]) != "https://example.com" {
		t.Error("stored field 'url' missing for doc0")
	}
	if string(dwpt.segment.storedFields[0]["title"]) != "Go Programming" {
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
	dwpt := newDWPT("_seg0", newTestFieldAnalyzers(), newDeleteQueue())

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
	dwpt := newDWPT("_seg0", newTestFieldAnalyzers(), newDeleteQueue())

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

func TestDWPTAddDocumentJapanese(t *testing.T) {
	dwpt := newDWPT("_seg0", newTestFieldAnalyzers(), newDeleteQueue())

	doc := document.NewDocument()
	doc.AddField("body", "東京 大阪", document.FieldTypeText)
	if _, err := dwpt.addDocument(doc); err != nil {
		t.Fatal(err)
	}

	if dwpt.segment.docCount != 1 {
		t.Errorf("docCount: got %d, want 1", dwpt.segment.docCount)
	}
	fi := dwpt.segment.fields["body"]
	if fi == nil {
		t.Fatal("expected 'body' field to exist")
	}
	if _, ok := fi.postings["東京"]; !ok {
		t.Error("expected posting for '東京'")
	}
	if _, ok := fi.postings["大阪"]; !ok {
		t.Error("expected posting for '大阪'")
	}
}

func TestDWPTPerFieldAnalyzerJapanese(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{}),
	)
	// Use ngram analyzer for "title" field
	fa.SetFieldAnalyzer("title", analysis.NewAnalyzer(
		analysis.NewNGramTokenizer(2, 3), &analysis.LowerCaseFilter{},
	))

	dwpt := newDWPT("_seg0", fa, newDeleteQueue())

	doc := document.NewDocument()
	doc.AddField("title", "東京都", document.FieldTypeText)
	doc.AddField("body", "東京 大阪", document.FieldTypeText)
	if _, err := dwpt.addDocument(doc); err != nil {
		t.Fatal(err)
	}

	// "title" with ngram(2,3) on "東京都" should produce: "東京", "京都", "東京都"
	titleField := dwpt.segment.fields["title"]
	if titleField == nil {
		t.Fatal("expected 'title' field")
	}
	for _, term := range []string{"東京", "京都", "東京都"} {
		if _, ok := titleField.postings[term]; !ok {
			t.Errorf("expected ngram posting for %q in title", term)
		}
	}

	// "body" with whitespace tokenizer should produce: "東京", "大阪"
	bodyField := dwpt.segment.fields["body"]
	if bodyField == nil {
		t.Fatal("expected 'body' field")
	}
	if _, ok := bodyField.postings["東京"]; !ok {
		t.Error("expected posting for '東京' in body")
	}
	if _, ok := bodyField.postings["大阪"]; !ok {
		t.Error("expected posting for '大阪' in body")
	}
}

func TestDWPTPerFieldAnalyzer(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{}),
	)
	// Use ngram analyzer for "title" field
	fa.SetFieldAnalyzer("title", analysis.NewAnalyzer(
		analysis.NewNGramTokenizer(2, 3), &analysis.LowerCaseFilter{},
	))

	dwpt := newDWPT("_seg0", fa, newDeleteQueue())

	doc := document.NewDocument()
	doc.AddField("title", "abc", document.FieldTypeText)
	doc.AddField("body", "hello world", document.FieldTypeText)
	if _, err := dwpt.addDocument(doc); err != nil {
		t.Fatal(err)
	}

	// "title" with ngram(2,3) on "abc" should produce: "ab", "bc", "abc"
	titleField := dwpt.segment.fields["title"]
	if titleField == nil {
		t.Fatal("expected 'title' field")
	}
	for _, term := range []string{"ab", "bc", "abc"} {
		if _, ok := titleField.postings[term]; !ok {
			t.Errorf("expected ngram posting for %q in title", term)
		}
	}

	// "body" with standard analyzer should produce: "hello", "world"
	bodyField := dwpt.segment.fields["body"]
	if bodyField == nil {
		t.Fatal("expected 'body' field")
	}
	if _, ok := bodyField.postings["hello"]; !ok {
		t.Error("expected posting for 'hello' in body")
	}
	if _, ok := bodyField.postings["world"]; !ok {
		t.Error("expected posting for 'world' in body")
	}
}
