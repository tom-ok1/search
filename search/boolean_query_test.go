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

func TestBooleanFilterOnly(t *testing.T) {
	seg := setupTestSegment(t)

	// FILTER only: "brown" — matches like MUST but score should be 0
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "brown"), OccurFilter)

	results := collectDocs(t, q, seg)

	docIDs := extractDocIDs(results)
	// doc0, doc1, doc3 contain "brown"
	if !containsDocID(docIDs, 0) || !containsDocID(docIDs, 1) || !containsDocID(docIDs, 3) {
		t.Errorf("expected doc0, doc1, doc3, got %v", docIDs)
	}

	// All scores should be 0 for filter-only queries
	for _, r := range results {
		if r.Score != 0.0 {
			t.Errorf("filter-only query should have score 0, doc%d got %.4f", r.DocID, r.Score)
		}
	}
}

func TestBooleanFilterWithMust(t *testing.T) {
	seg := setupTestSegment(t)

	// MUST "brown" + FILTER "fox" — only MUST contributes to score
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "brown"), OccurMust).
		Add(NewTermQuery("body", "fox"), OccurFilter)

	results := collectDocs(t, q, seg)

	docIDs := extractDocIDs(results)
	// doc0 and doc3 contain both "brown" and "fox"
	if !containsDocID(docIDs, 0) || !containsDocID(docIDs, 3) {
		t.Errorf("expected doc0 and doc3, got %v", docIDs)
	}
	if containsDocID(docIDs, 1) {
		t.Error("doc1 should not match (no 'fox')")
	}

	// Score should come only from "brown" MUST clause, not from "fox" FILTER clause
	// Compare with a pure MUST query for "brown" to verify scores match
	mustOnly := NewBooleanQuery().
		Add(NewTermQuery("body", "brown"), OccurMust)
	mustResults := collectDocs(t, mustOnly, seg)

	mustScoreMap := make(map[int]float64)
	for _, r := range mustResults {
		mustScoreMap[r.DocID] = r.Score
	}

	for _, r := range results {
		expected := mustScoreMap[r.DocID]
		if r.Score != expected {
			t.Errorf("doc%d: filter+must score=%.4f, must-only score=%.4f — FILTER should not affect scoring",
				r.DocID, r.Score, expected)
		}
	}
}

func TestBooleanFilterWithMustScoreNotAffectedByCostSort(t *testing.T) {
	seg := setupTestSegment(t)

	// This test verifies the bug fix: ConjunctionScorer sorts by cost internally,
	// so a naive index-based scoring approach would break.
	// Use a high-cost MUST term and a low-cost FILTER term to trigger reordering.

	// "the" appears in all 4 docs (high cost), "fox" in 2 docs (low cost)
	// If cost-sorting puts "fox" first, a naive scoringIdx=0 would use fox's score
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "the"), OccurMust).
		Add(NewTermQuery("body", "fox"), OccurFilter)

	results := collectDocs(t, q, seg)

	docIDs := extractDocIDs(results)
	// "the" AND "fox" → doc0 ("the quick brown fox"), doc2 ("the quick red fox jumps")
	if !containsDocID(docIDs, 0) || !containsDocID(docIDs, 2) {
		t.Errorf("expected doc0, doc2, got %v", docIDs)
	}
	if containsDocID(docIDs, 3) {
		t.Error("doc3 should not match (no 'the')")
	}

	// Score should come only from "the" (MUST), not "fox" (FILTER)
	mustOnly := NewBooleanQuery().
		Add(NewTermQuery("body", "the"), OccurMust)
	mustResults := collectDocs(t, mustOnly, seg)

	mustScoreMap := make(map[int]float64)
	for _, r := range mustResults {
		mustScoreMap[r.DocID] = r.Score
	}

	for _, r := range results {
		expected := mustScoreMap[r.DocID]
		if r.Score != expected {
			t.Errorf("doc%d: filter+must score=%.4f, must-only score=%.4f — cost-sort bug detected",
				r.DocID, r.Score, expected)
		}
	}
}

func TestBooleanMultipleFilters(t *testing.T) {
	seg := setupTestSegment(t)

	// Multiple FILTER clauses, no scoring — score should be 0
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "brown"), OccurFilter).
		Add(NewTermQuery("body", "fox"), OccurFilter)

	results := collectDocs(t, q, seg)

	docIDs := extractDocIDs(results)
	if !containsDocID(docIDs, 0) || !containsDocID(docIDs, 3) {
		t.Errorf("expected doc0 and doc3, got %v", docIDs)
	}
	if containsDocID(docIDs, 1) {
		t.Error("doc1 should not match")
	}

	for _, r := range results {
		if r.Score != 0.0 {
			t.Errorf("multi-filter query should have score 0, doc%d got %.4f", r.DocID, r.Score)
		}
	}
}

func TestBooleanFilterWithShould(t *testing.T) {
	seg := setupTestSegment(t)

	// FILTER "brown" + SHOULD "quick" — FILTER constrains, SHOULD scores
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "brown"), OccurFilter).
		Add(NewTermQuery("body", "quick"), OccurShould)

	results := collectDocs(t, q, seg)

	docIDs := extractDocIDs(results)
	// "brown" matches doc0, doc1, doc3
	// but SHOULD "quick" makes it act as MUST when combined with FILTER
	// Actually: when there are required clauses, SHOULD is optional boost
	// So all "brown" docs match, but doc0 gets boosted by "quick"
	if !containsDocID(docIDs, 0) || !containsDocID(docIDs, 1) || !containsDocID(docIDs, 3) {
		t.Errorf("expected doc0, doc1, doc3, got %v", docIDs)
	}

	scoreMap := make(map[int]float64)
	for _, r := range results {
		scoreMap[r.DocID] = r.Score
	}
	// doc0 has "quick" boost, doc1 does not
	if scoreMap[0] <= scoreMap[1] {
		t.Errorf("doc0 (brown+quick) should score higher than doc1 (brown only), got doc0=%.4f doc1=%.4f",
			scoreMap[0], scoreMap[1])
	}
}

func TestBooleanFilterWithMustNot(t *testing.T) {
	seg := setupTestSegment(t)

	// FILTER "brown" + MUST_NOT "fox"
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "brown"), OccurFilter).
		Add(NewTermQuery("body", "fox"), OccurMustNot)

	results := collectDocs(t, q, seg)

	docIDs := extractDocIDs(results)
	// doc1 has "brown" but not "fox"
	if len(docIDs) != 1 || docIDs[0] != 1 {
		t.Errorf("expected [1], got %v", docIDs)
	}

	// Score should be 0 (filter only, no scoring clauses)
	for _, r := range results {
		if r.Score != 0.0 {
			t.Errorf("filter+must_not query should have score 0, doc%d got %.4f", r.DocID, r.Score)
		}
	}
}

func TestBooleanFilterExtractTerms(t *testing.T) {
	// FILTER terms should be included in ExtractTerms (unlike MUST_NOT)
	q := NewBooleanQuery().
		Add(NewTermQuery("body", "brown"), OccurMust).
		Add(NewTermQuery("body", "fox"), OccurFilter).
		Add(NewTermQuery("body", "dog"), OccurMustNot)

	terms := q.ExtractTerms()

	termSet := make(map[string]bool)
	for _, ft := range terms {
		termSet[ft.Term] = true
	}

	if !termSet["brown"] {
		t.Error("MUST term 'brown' should be extracted")
	}
	if !termSet["fox"] {
		t.Error("FILTER term 'fox' should be extracted")
	}
	if termSet["dog"] {
		t.Error("MUST_NOT term 'dog' should NOT be extracted")
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
