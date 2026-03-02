package index

import (
	"os"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

func TestWriteAndReadSegment(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gosearch-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dir, _ := store.NewFSDirectory(tmpDir)

	// Build a segment via IndexWriter
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	writer := NewIndexWriter(analyzer, 100)

	doc0 := document.NewDocument()
	doc0.AddField("title", "The Quick Brown Fox", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("title", "The Lazy Dog", document.FieldTypeText)
	writer.AddDocument(doc1)

	writer.Flush()

	// Write to disk
	seg := writer.Segments()[0]
	if err := WriteSegment(dir, seg); err != nil {
		t.Fatal(err)
	}

	// Read from disk
	readSeg, err := ReadSegment(dir, seg.name)
	if err != nil {
		t.Fatal(err)
	}

	// Verify doc count
	if readSeg.docCount != 2 {
		t.Errorf("expected 2 docs, got %d", readSeg.docCount)
	}

	// Verify postings for "fox"
	pl := readSeg.fields["title"].postings["fox"]
	if pl == nil {
		t.Fatal("expected postings for 'fox'")
	}
	if len(pl.Postings) != 1 || pl.Postings[0].DocID != 0 {
		t.Errorf("unexpected postings for 'fox': %+v", pl.Postings)
	}

	// Verify stored fields
	stored := readSeg.storedFields[0]
	if stored["title"] != "The Quick Brown Fox" {
		t.Errorf("expected original text, got %q", stored["title"])
	}

	// Verify field lengths
	lengths := readSeg.fieldLengths["title"]
	if len(lengths) != 2 {
		t.Fatalf("expected 2 field lengths, got %d", len(lengths))
	}
	if lengths[0] != 4 {
		t.Errorf("expected field length 4 for doc0, got %d", lengths[0])
	}
	if lengths[1] != 3 {
		t.Errorf("expected field length 3 for doc1, got %d", lengths[1])
	}
}
