package search

import (
	"testing"

	"gosearch/index"
)

// mockScorable is a test helper that returns a configurable score.
type mockScorable struct {
	score float64
	docID int
}

func (m *mockScorable) Score() float64 { return m.score }
func (m *mockScorable) DocID() int     { return m.docID }

// leafCtx creates a LeafReaderContext with the given docBase for testing.
func leafCtx(docBase int) index.LeafReaderContext {
	return index.LeafReaderContext{
		Segment: newMockSegment("test", 0),
		DocBase: docBase,
	}
}

// collectWithScorer is a test helper: sets the scorer, sets score, then collects.
func collectWithScorer(lc LeafCollector, ms *mockScorable, docID int, score float64) {
	ms.score = score
	lc.Collect(docID)
}

func TestTopKCollector_CollectLessThanK(t *testing.T) {
	c := NewTopKCollector(5)
	lc := c.GetLeafCollector(leafCtx(0))
	ms := &mockScorable{}
	lc.SetScorer(ms)
	collectWithScorer(lc, ms, 1, 1.0)
	collectWithScorer(lc, ms, 2, 2.0)
	collectWithScorer(lc, ms, 3, 3.0)

	results := c.Results()
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i := 1; i < len(results); i++ {
		if results[i-1].Score < results[i].Score {
			t.Errorf("results not in descending order: %v", results)
		}
	}
}

func TestTopKCollector_CollectMoreThanK(t *testing.T) {
	c := NewTopKCollector(3)
	lc := c.GetLeafCollector(leafCtx(0))
	ms := &mockScorable{}
	lc.SetScorer(ms)
	for i := range 10 {
		collectWithScorer(lc, ms, i, float64(i))
	}

	results := c.Results()
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	expected := []float64{9, 8, 7}
	for i, want := range expected {
		if results[i].Score != want {
			t.Errorf("results[%d].Score = %v, want %v", i, results[i].Score, want)
		}
	}
}

func TestTopKCollector_DescendingOrder(t *testing.T) {
	c := NewTopKCollector(5)
	lc := c.GetLeafCollector(leafCtx(0))
	ms := &mockScorable{}
	lc.SetScorer(ms)
	scores := []float64{3.0, 1.0, 4.0, 1.5, 2.0}
	for i, s := range scores {
		collectWithScorer(lc, ms, i, s)
	}

	results := c.Results()
	for i := 1; i < len(results); i++ {
		if results[i-1].Score < results[i].Score {
			t.Errorf("results not in descending order at index %d: %v", i, results)
			break
		}
	}
}

func TestTopKCollector_EqualScores(t *testing.T) {
	c := NewTopKCollector(3)
	lc := c.GetLeafCollector(leafCtx(0))
	ms := &mockScorable{}
	lc.SetScorer(ms)
	for i := range 5 {
		collectWithScorer(lc, ms, i, 1.0)
	}

	results := c.Results()
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func TestTopKCollector_SingleElement(t *testing.T) {
	c := NewTopKCollector(1)
	lc := c.GetLeafCollector(leafCtx(0))
	ms := &mockScorable{}
	lc.SetScorer(ms)
	collectWithScorer(lc, ms, 1, 5.0)
	collectWithScorer(lc, ms, 2, 10.0)
	collectWithScorer(lc, ms, 3, 1.0)

	results := c.Results()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Score != 10.0 {
		t.Errorf("expected score 10.0, got %v", results[0].Score)
	}
}

func TestTopKCollector_EmptyResults(t *testing.T) {
	c := NewTopKCollector(5)
	results := c.Results()
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestTopKCollector_ReverseInsertOrder(t *testing.T) {
	c := NewTopKCollector(3)
	lc := c.GetLeafCollector(leafCtx(0))
	ms := &mockScorable{}
	lc.SetScorer(ms)
	for i := 9; i >= 0; i-- {
		collectWithScorer(lc, ms, i, float64(i))
	}

	results := c.Results()
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	expected := []float64{9, 8, 7}
	for i, want := range expected {
		if results[i].Score != want {
			t.Errorf("results[%d].Score = %v, want %v", i, results[i].Score, want)
		}
	}
}
