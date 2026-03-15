package search

import (
	"testing"
)

func TestPhraseQueryMatch(t *testing.T) {
	seg := setupTestSegment(t)

	q := NewPhraseQuery("body", "brown", "fox")
	results := collectDocs(t, q, seg)

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
	results := collectDocs(t, q, seg)

	if len(results) != 0 {
		t.Errorf("expected no matches for 'quick fox', got %v", extractDocIDs(results))
	}
}

func TestPhraseQuerySingleTerm(t *testing.T) {
	seg := setupTestSegment(t)

	// A single-term phrase query should behave like a term query
	q := NewPhraseQuery("body", "quick")
	results := collectDocs(t, q, seg)

	docIDs := extractDocIDs(results)
	// "quick" appears in doc0, doc2
	if len(docIDs) != 2 {
		t.Fatalf("expected 2 matches, got %d: %v", len(docIDs), docIDs)
	}
	if !containsDocID(docIDs, 0) || !containsDocID(docIDs, 2) {
		t.Errorf("expected doc0 and doc2, got %v", docIDs)
	}
}

func TestPhraseQueryEmptyTerms(t *testing.T) {
	seg := setupTestSegment(t)

	q := NewPhraseQuery("body")
	results := collectDocs(t, q, seg)

	if len(results) != 0 {
		t.Errorf("expected no matches for empty phrase, got %v", extractDocIDs(results))
	}
}

func TestPhraseQueryThreeTerms(t *testing.T) {
	seg := setupTestSegment(t)

	// "the quick brown" appears in doc0 at positions 0,1,2
	q := NewPhraseQuery("body", "the", "quick", "brown")
	results := collectDocs(t, q, seg)

	docIDs := extractDocIDs(results)
	if !containsDocID(docIDs, 0) {
		t.Error("doc0 should match 'the quick brown'")
	}
	// doc2 has "the quick red" not "the quick brown"
	if containsDocID(docIDs, 2) {
		t.Error("doc2 should not match 'the quick brown'")
	}
}
