package search

import (
	"testing"
)

func TestTermQueryMatch(t *testing.T) {
	seg := setupTestSegment(t)

	q := NewTermQuery("body", "brown")
	results := collectDocs(t, q, seg)

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
	results := collectDocs(t, q, seg)

	if len(results) != 0 {
		t.Errorf("expected no matches, got %v", extractDocIDs(results))
	}
}

func TestTermQueryScoring(t *testing.T) {
	seg := setupTestSegment(t)

	// "brown" appears twice in doc3, once in doc0 and doc1
	// doc3 should have a higher score due to higher term frequency
	q := NewTermQuery("body", "brown")
	results := collectDocs(t, q, seg)

	scoreMap := make(map[int]float64)
	for _, r := range results {
		scoreMap[r.DocID] = r.Score
	}

	if scoreMap[3] <= scoreMap[0] {
		t.Errorf("doc3 (tf=2) should score higher than doc0 (tf=1), got doc3=%.4f doc0=%.4f", scoreMap[3], scoreMap[0])
	}
}

func TestTermQueryMatchJapanese(t *testing.T) {
	seg := setupJapaneseTestSegment(t)

	q := NewTermQuery("body", "東京")
	results := collectDocs(t, q, seg)

	docIDs := extractDocIDs(results)
	// "東京" appears in doc0, doc1, doc3
	if len(docIDs) != 3 {
		t.Fatalf("expected 3 matches, got %d: %v", len(docIDs), docIDs)
	}
	for _, expected := range []int{0, 1, 3} {
		if !containsDocID(docIDs, expected) {
			t.Errorf("expected doc%d in results, got %v", expected, docIDs)
		}
	}
}

func TestTermQueryNoMatchJapanese(t *testing.T) {
	seg := setupJapaneseTestSegment(t)

	q := NewTermQuery("body", "横浜")
	results := collectDocs(t, q, seg)

	if len(results) != 0 {
		t.Errorf("expected no matches, got %v", extractDocIDs(results))
	}
}

func TestTermQuerySpecialChars(t *testing.T) {
	seg := setupSpecialCharsTestSegment(t)

	tests := []struct {
		term     string
		expected int
	}{
		{"user@example.com", 1},
		{"#tag", 1},
		{"state-of-the-art", 1},
		{"node.js", 1},
		{"🔍", 1},
		{"🔎", 1},
		{"𠮷野家", 1},
		{"café", 2},   // doc1 (lowered from Café) and doc3
		{"résumé", 1}, // doc1 (lowered from Résumé)
		{"nonexistent", 0},
	}

	for _, tt := range tests {
		q := NewTermQuery("body", tt.term)
		results := collectDocs(t, q, seg)
		if len(results) != tt.expected {
			t.Errorf("term %q: expected %d matches, got %d", tt.term, tt.expected, len(results))
		}
	}
}

func TestTermQueryScoringJapanese(t *testing.T) {
	seg := setupJapaneseTestSegment(t)

	// "東京" appears twice in doc3, once in doc0 and doc1
	q := NewTermQuery("body", "東京")
	results := collectDocs(t, q, seg)

	scoreMap := make(map[int]float64)
	for _, r := range results {
		scoreMap[r.DocID] = r.Score
	}

	if scoreMap[3] <= scoreMap[0] {
		t.Errorf("doc3 (tf=2) should score higher than doc0 (tf=1), got doc3=%.4f doc0=%.4f", scoreMap[3], scoreMap[0])
	}
}
