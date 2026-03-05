package index

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

func TestInvertedIndex(t *testing.T) {
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, analyzer, 100)

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
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, analyzer, 100)

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
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	dir, _ := store.NewFSDirectory(t.TempDir())
	// Buffer size 2: auto-flush after 2 documents
	writer := NewIndexWriter(dir, analyzer, 2)

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
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	dir, _ := store.NewFSDirectory(t.TempDir())
	writer := NewIndexWriter(dir, analyzer, 100)

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
