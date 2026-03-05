package search

import (
	"slices"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/store"
)

func setupTestSegment(t *testing.T) index.SegmentReader {
	t.Helper()
	dir, _ := store.NewFSDirectory(t.TempDir())
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	writer := index.NewIndexWriter(dir, analyzer, 100)

	docs := []string{
		"the quick brown fox",     // doc0
		"the lazy brown dog",      // doc1
		"the quick red fox jumps", // doc2
		"brown fox brown fox",     // doc3
	}

	for _, text := range docs {
		doc := document.NewDocument()
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc)
	}
	writer.Flush()
	reader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })
	return reader.Leaves()[0].Segment
}

func TestBooleanMust(t *testing.T) {
	seg := setupTestSegment(t)

	// "brown" AND "fox"
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "brown"), OccurMust).
		Add(NewTermQuery("body", "fox"), OccurMust)

	results := q.Execute(seg)

	docIDs := extractDocIDs(results)
	// doc0 and doc3 contain both "brown" and "fox"
	if !containsDocID(docIDs, 0) || !containsDocID(docIDs, 3) {
		t.Errorf("expected doc0 and doc3, got %v", docIDs)
	}
	if containsDocID(docIDs, 1) {
		t.Error("doc1 should not match (no 'fox')")
	}
}

func TestBooleanMustNot(t *testing.T) {
	seg := setupTestSegment(t)

	// "brown" AND NOT "fox"
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "brown"), OccurMust).
		Add(NewTermQuery("body", "fox"), OccurMustNot)

	results := q.Execute(seg)

	docIDs := extractDocIDs(results)
	// only doc1 has "brown" but not "fox"
	if len(docIDs) != 1 || docIDs[0] != 1 {
		t.Errorf("expected [1], got %v", docIDs)
	}
}

func TestBooleanShould(t *testing.T) {
	seg := setupTestSegment(t)

	// "quick" OR "lazy"
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "quick"), OccurShould).
		Add(NewTermQuery("body", "lazy"), OccurShould)

	results := q.Execute(seg)

	docIDs := extractDocIDs(results)
	// doc0: quick, doc1: lazy, doc2: quick
	if !containsDocID(docIDs, 0) || !containsDocID(docIDs, 1) || !containsDocID(docIDs, 2) {
		t.Errorf("expected doc0, doc1, doc2, got %v", docIDs)
	}
	if containsDocID(docIDs, 3) {
		t.Error("doc3 should not match (no 'quick' or 'lazy')")
	}
}

func TestPhraseQuery(t *testing.T) {
	seg := setupTestSegment(t)

	// phrase "brown fox"
	q := NewPhraseQuery("body", "brown", "fox")
	results := q.Execute(seg)

	docIDs := extractDocIDs(results)
	// doc0: "... brown fox" (positions 2,3) -> match
	// doc3: "brown fox brown fox" (positions 0,1 and 2,3) -> match
	if !containsDocID(docIDs, 0) {
		t.Error("doc0 should match 'brown fox'")
	}
	if !containsDocID(docIDs, 3) {
		t.Error("doc3 should match 'brown fox'")
	}
	// doc1: "... brown dog" -> no match
	if containsDocID(docIDs, 1) {
		t.Error("doc1 should not match 'brown fox'")
	}
}

func TestPhraseQueryNoMatch(t *testing.T) {
	seg := setupTestSegment(t)

	// "quick fox" - not adjacent in any doc
	// doc0: quick(pos=1), fox(pos=3) -> diff=2, no match
	// doc2: quick(pos=1), fox(pos=3) -> diff=2, no match
	q := NewPhraseQuery("body", "quick", "fox")
	results := q.Execute(seg)

	if len(results) != 0 {
		docIDs := extractDocIDs(results)
		t.Errorf("expected no matches for 'quick fox', got %v", docIDs)
	}
}

func extractDocIDs(results []DocScore) []int {
	var ids []int
	for _, r := range results {
		ids = append(ids, r.DocID)
	}
	return ids
}

func containsDocID(ids []int, target int) bool {
	return slices.Contains(ids, target)
}
