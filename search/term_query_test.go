package search

import (
	"testing"
)

func TestTermQueryMatch(t *testing.T) {
	seg := setupTestSegment(t)

	q := NewTermQuery("body", "brown")
	results := q.Execute(seg)

	docIDs := extractDocIDs(results)
	// "brown" appears in doc0, doc1, doc3
	if len(docIDs) != 3 {
		t.Fatalf("expected 3 matches, got %d: %v", len(docIDs), docIDs)
	}
	for _, expected := range []int{0, 1, 3} {
		if !containsDocID(docIDs, expected) {
			t.Errorf("expected doc%d in results, got %v", expected, docIDs)
		}
	}
}

func TestTermQueryNoMatch(t *testing.T) {
	seg := setupTestSegment(t)

	q := NewTermQuery("body", "nonexistent")
	results := q.Execute(seg)

	if len(results) != 0 {
		t.Errorf("expected no matches, got %v", extractDocIDs(results))
	}
}

func TestTermQueryScoring(t *testing.T) {
	seg := setupTestSegment(t)

	// "brown" appears twice in doc3, once in doc0 and doc1
	// doc3 should have a higher score due to higher term frequency
	q := NewTermQuery("body", "brown")
	results := q.Execute(seg)

	scoreMap := make(map[int]float64)
	for _, r := range results {
		scoreMap[r.DocID] = r.Score
	}

	if scoreMap[3] <= scoreMap[0] {
		t.Errorf("doc3 (tf=2) should score higher than doc0 (tf=1), got doc3=%.4f doc0=%.4f", scoreMap[3], scoreMap[0])
	}
}
