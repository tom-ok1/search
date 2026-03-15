package search

import (
	"testing"
)

func TestBooleanMust(t *testing.T) {
	seg := setupTestSegment(t)

	// "brown" AND "fox"
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "brown"), OccurMust).
		Add(NewTermQuery("body", "fox"), OccurMust)

	results := collectDocs(t, q, seg)

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

	results := collectDocs(t, q, seg)

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

	results := collectDocs(t, q, seg)

	docIDs := extractDocIDs(results)
	// doc0: quick, doc1: lazy, doc2: quick
	if !containsDocID(docIDs, 0) || !containsDocID(docIDs, 1) || !containsDocID(docIDs, 2) {
		t.Errorf("expected doc0, doc1, doc2, got %v", docIDs)
	}
	if containsDocID(docIDs, 3) {
		t.Error("doc3 should not match (no 'quick' or 'lazy')")
	}
}

func TestBooleanMustWithShould(t *testing.T) {
	seg := setupTestSegment(t)

	// "brown" MUST, "quick" SHOULD — SHOULD acts as a score boost
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "brown"), OccurMust).
		Add(NewTermQuery("body", "quick"), OccurShould)

	results := collectDocs(t, q, seg)

	docIDs := extractDocIDs(results)
	// All docs with "brown": doc0, doc1, doc3
	if !containsDocID(docIDs, 0) || !containsDocID(docIDs, 1) || !containsDocID(docIDs, 3) {
		t.Errorf("expected doc0, doc1, doc3, got %v", docIDs)
	}

	// doc0 has both "brown" and "quick", so should score higher than doc1
	scoreMap := make(map[int]float64)
	for _, r := range results {
		scoreMap[r.DocID] = r.Score
	}
	if scoreMap[0] <= scoreMap[1] {
		t.Errorf("doc0 (brown+quick) should score higher than doc1 (brown only), got doc0=%.4f doc1=%.4f", scoreMap[0], scoreMap[1])
	}
}
