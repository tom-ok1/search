package search

import (
	"testing"

	"gosearch/index"
)

func TestEmptyDocIdSetIterator(t *testing.T) {
	iter := EmptyDocIdSetIterator{}

	if iter.DocID() != NoMoreDocs {
		t.Errorf("DocID() = %d, want NoMoreDocs", iter.DocID())
	}
	if iter.NextDoc() != NoMoreDocs {
		t.Errorf("NextDoc() = %d, want NoMoreDocs", iter.NextDoc())
	}
	if iter.Advance(100) != NoMoreDocs {
		t.Errorf("Advance(100) = %d, want NoMoreDocs", iter.Advance(100))
	}
	if iter.Cost() != 0 {
		t.Errorf("Cost() = %d, want 0", iter.Cost())
	}
}

func TestPostingsDocIdSetIterator(t *testing.T) {
	postings := []index.Posting{
		{DocID: 1, Freq: 2, Positions: []int{0, 5}},
		{DocID: 3, Freq: 1, Positions: []int{2}},
		{DocID: 7, Freq: 3, Positions: []int{1, 3, 8}},
	}
	iter := NewPostingsDocIdSetIterator(
		index.NewSlicePostingsIterator(postings),
		int64(len(postings)),
	)

	// Initial state
	if iter.DocID() != -1 {
		t.Errorf("initial DocID() = %d, want -1", iter.DocID())
	}

	// NextDoc sequence
	expectedDocs := []int{1, 3, 7, NoMoreDocs}
	for _, expected := range expectedDocs {
		got := iter.NextDoc()
		if got != expected {
			t.Errorf("NextDoc() = %d, want %d", got, expected)
		}
	}
}

func TestPostingsDocIdSetIterator_Advance(t *testing.T) {
	postings := []index.Posting{
		{DocID: 1, Freq: 1},
		{DocID: 3, Freq: 1},
		{DocID: 5, Freq: 1},
		{DocID: 10, Freq: 1},
		{DocID: 15, Freq: 1},
	}
	iter := NewPostingsDocIdSetIterator(
		index.NewSlicePostingsIterator(postings),
		int64(len(postings)),
	)

	// Advance to exact match
	if got := iter.Advance(3); got != 3 {
		t.Errorf("Advance(3) = %d, want 3", got)
	}

	// Advance to non-exact (should return next valid)
	if got := iter.Advance(6); got != 10 {
		t.Errorf("Advance(6) = %d, want 10", got)
	}

	// Advance past all docs
	if got := iter.Advance(100); got != NoMoreDocs {
		t.Errorf("Advance(100) = %d, want NoMoreDocs", got)
	}
}

func TestConjunctionScorer(t *testing.T) {
	// Create two term scorers that share some docs
	seg := newMockSegment("test", 10)
	seg.postings["field"] = map[string][]index.Posting{
		"a": {{DocID: 1, Freq: 1}, {DocID: 3, Freq: 1}, {DocID: 5, Freq: 1}, {DocID: 7, Freq: 1}},
		"b": {{DocID: 2, Freq: 1}, {DocID: 3, Freq: 1}, {DocID: 5, Freq: 1}, {DocID: 8, Freq: 1}},
	}

	ctx := index.LeafReaderContext{Segment: seg, DocBase: 0}

	scorer1 := NewTermQuery("field", "a").CreateScorer(ctx, ScoreModeNone)
	scorer2 := NewTermQuery("field", "b").CreateScorer(ctx, ScoreModeNone)

	conj := NewConjunctionScorer([]Scorer{scorer1, scorer2})
	if conj == nil {
		t.Fatal("expected non-nil ConjunctionScorer")
	}

	iter := conj.Iterator()

	// Should return only docs present in both: 3 and 5
	if got := iter.NextDoc(); got != 3 {
		t.Errorf("first NextDoc() = %d, want 3", got)
	}
	if got := iter.NextDoc(); got != 5 {
		t.Errorf("second NextDoc() = %d, want 5", got)
	}
	if got := iter.NextDoc(); got != NoMoreDocs {
		t.Errorf("third NextDoc() = %d, want NoMoreDocs", got)
	}
}

func TestDisjunctionScorer(t *testing.T) {
	// Create two term scorers with different docs
	seg := newMockSegment("test", 10)
	seg.postings["field"] = map[string][]index.Posting{
		"a": {{DocID: 1, Freq: 1}, {DocID: 3, Freq: 1}},
		"b": {{DocID: 2, Freq: 1}, {DocID: 3, Freq: 1}, {DocID: 5, Freq: 1}},
	}

	ctx := index.LeafReaderContext{Segment: seg, DocBase: 0}

	scorer1 := NewTermQuery("field", "a").CreateScorer(ctx, ScoreModeNone)
	scorer2 := NewTermQuery("field", "b").CreateScorer(ctx, ScoreModeNone)

	disj := NewDisjunctionScorer([]Scorer{scorer1, scorer2})
	if disj == nil {
		t.Fatal("expected non-nil DisjunctionScorer")
	}

	iter := disj.Iterator()

	// Should return all unique docs: 1, 2, 3, 5
	expectedDocs := []int{1, 2, 3, 5, NoMoreDocs}
	for _, expected := range expectedDocs {
		got := iter.NextDoc()
		if got != expected {
			t.Errorf("NextDoc() = %d, want %d", got, expected)
		}
	}
}

func TestTermQueryCreateScorer(t *testing.T) {
	seg := newMockSegment("test", 10)
	seg.postings["body"] = map[string][]index.Posting{
		"fox": {
			{DocID: 0, Freq: 2, Positions: []int{0, 5}},
			{DocID: 2, Freq: 1, Positions: []int{3}},
		},
	}
	seg.fieldLens["body"] = map[int]int{0: 5, 2: 5}
	seg.totalFldLen["body"] = 10

	ctx := index.LeafReaderContext{Segment: seg, DocBase: 0}

	q := NewTermQuery("body", "fox")
	scorer := q.CreateScorer(ctx, ScoreModeComplete)

	if scorer == nil {
		t.Fatal("expected non-nil scorer")
	}

	iter := scorer.Iterator()

	// First doc
	doc := iter.NextDoc()
	if doc != 0 {
		t.Errorf("first doc = %d, want 0", doc)
	}
	score := scorer.Score()
	if score <= 0 {
		t.Errorf("score should be positive, got %f", score)
	}

	// Second doc
	doc = iter.NextDoc()
	if doc != 2 {
		t.Errorf("second doc = %d, want 2", doc)
	}

	// End
	doc = iter.NextDoc()
	if doc != NoMoreDocs {
		t.Errorf("expected NoMoreDocs, got %d", doc)
	}
}

func TestTermQueryWithScoreModeNone(t *testing.T) {
	seg := newMockSegment("test", 10)
	seg.postings["body"] = map[string][]index.Posting{
		"test": {{DocID: 0, Freq: 1}},
	}

	ctx := index.LeafReaderContext{Segment: seg, DocBase: 0}

	q := NewTermQuery("body", "test")
	scorer := q.CreateScorer(ctx, ScoreModeNone)

	if scorer == nil {
		t.Fatal("expected non-nil scorer")
	}

	iter := scorer.Iterator()
	iter.NextDoc()

	// Score should be 0 when ScoreModeNone
	if score := scorer.Score(); score != 0 {
		t.Errorf("score with ScoreModeNone should be 0, got %f", score)
	}
}
