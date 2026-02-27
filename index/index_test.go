package index

import (
	"gosearch/analysis"
	"gosearch/document"
	"testing"
)

func TestInvertedIndex(t *testing.T) {
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	idx := NewInMemoryIndex(analyzer)

	doc0 := document.NewDocument()
	doc0.AddField("title", "The Quick Brown Fox", document.FieldTypeText)
	idx.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("title", "The Lazy Brown Dog", document.FieldTypeText)
	idx.AddDocument(doc1)

	// "brown" appears in both documents
	pl := idx.GetPostings("title", "brown")
	if pl == nil {
		t.Fatal("expected postings for 'brown'")
	}
	if len(pl.Postings) != 2 {
		t.Fatalf("expected 2 postings, got %d", len(pl.Postings))
	}

	// "fox" appears only in doc0
	pl = idx.GetPostings("title", "fox")
	if pl == nil {
		t.Fatal("expected postings for 'fox'")
	}
	if len(pl.Postings) != 1 {
		t.Fatalf("expected 1 posting, got %d", len(pl.Postings))
	}
	if pl.Postings[0].DocID != 0 {
		t.Errorf("expected docID 0, got %d", pl.Postings[0].DocID)
	}

	// non-existent term
	pl = idx.GetPostings("title", "cat")
	if pl != nil {
		t.Error("expected nil for 'cat'")
	}
}

func TestPostingFreqAndPositions(t *testing.T) {
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	idx := NewInMemoryIndex(analyzer)

	doc := document.NewDocument()
	doc.AddField("body", "the fox and the fox", document.FieldTypeText)
	idx.AddDocument(doc)

	pl := idx.GetPostings("body", "fox")
	if pl.Postings[0].Freq != 2 {
		t.Errorf("expected freq 2, got %d", pl.Postings[0].Freq)
	}
	// "fox" appears at positions 1 and 4
	if pl.Postings[0].Positions[0] != 1 || pl.Postings[0].Positions[1] != 4 {
		t.Errorf("expected positions [1,4], got %v", pl.Postings[0].Positions)
	}
}
