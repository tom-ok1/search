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

func TestBooleanMustJapanese(t *testing.T) {
	seg := setupJapaneseTestSegment(t)

	// "東京" AND "大阪"
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "東京"), OccurMust).
		Add(NewTermQuery("body", "大阪"), OccurMust)

	results := collectDocs(t, q, seg)

	docIDs := extractDocIDs(results)
	// doc0 and doc3 contain both "東京" and "大阪"
	if !containsDocID(docIDs, 0) || !containsDocID(docIDs, 3) {
		t.Errorf("expected doc0 and doc3, got %v", docIDs)
	}
	if containsDocID(docIDs, 1) {
		t.Error("doc1 should not match (no '大阪')")
	}
}

func TestBooleanMustNotJapanese(t *testing.T) {
	seg := setupJapaneseTestSegment(t)

	// "東京" AND NOT "大阪"
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "東京"), OccurMust).
		Add(NewTermQuery("body", "大阪"), OccurMustNot)

	results := collectDocs(t, q, seg)

	docIDs := extractDocIDs(results)
	// only doc1 has "東京" but not "大阪"
	if len(docIDs) != 1 || docIDs[0] != 1 {
		t.Errorf("expected [1], got %v", docIDs)
	}
}

func TestBooleanMustSpecialChars(t *testing.T) {
	seg := setupSpecialCharsTestSegment(t)

	// "café" AND "résumé"
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "café"), OccurMust).
		Add(NewTermQuery("body", "résumé"), OccurMust)

	results := collectDocs(t, q, seg)
	docIDs := extractDocIDs(results)
	// Only doc1 has both (after lowercasing)
	if len(docIDs) != 1 || docIDs[0] != 1 {
		t.Errorf("expected [1], got %v", docIDs)
	}
}

func TestBooleanMustNotSpecialChars(t *testing.T) {
	seg := setupSpecialCharsTestSegment(t)

	// "café" AND NOT "résumé" — should match doc3 (has café but not résumé)
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "café"), OccurMust).
		Add(NewTermQuery("body", "résumé"), OccurMustNot)

	results := collectDocs(t, q, seg)
	docIDs := extractDocIDs(results)
	if len(docIDs) != 1 || docIDs[0] != 3 {
		t.Errorf("expected [3], got %v", docIDs)
	}
}

func TestBooleanShouldSpecialChars(t *testing.T) {
	seg := setupSpecialCharsTestSegment(t)

	// "🔍" OR "𠮷野家"
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "🔍"), OccurShould).
		Add(NewTermQuery("body", "𠮷野家"), OccurShould)

	results := collectDocs(t, q, seg)
	docIDs := extractDocIDs(results)
	// doc2 has 🔍, doc3 has 𠮷野家
	if !containsDocID(docIDs, 2) || !containsDocID(docIDs, 3) {
		t.Errorf("expected doc2 and doc3, got %v", docIDs)
	}
}

func TestBooleanShouldJapanese(t *testing.T) {
	seg := setupJapaneseTestSegment(t)

	// "名古屋" OR "福岡"
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "名古屋"), OccurShould).
		Add(NewTermQuery("body", "福岡"), OccurShould)

	results := collectDocs(t, q, seg)

	docIDs := extractDocIDs(results)
	// doc0: 名古屋, doc1: 福岡
	if !containsDocID(docIDs, 0) || !containsDocID(docIDs, 1) {
		t.Errorf("expected doc0 and doc1, got %v", docIDs)
	}
	if containsDocID(docIDs, 2) {
		t.Error("doc2 should not match (no '名古屋' or '福岡')")
	}
}
