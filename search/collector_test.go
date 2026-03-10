package search

import (
	"testing"
)

func TestTopKCollector_CollectLessThanK(t *testing.T) {
	c := NewTopKCollector(5)
	c.Collect(SearchResult{DocID: 1, Score: 1.0})
	c.Collect(SearchResult{DocID: 2, Score: 2.0})
	c.Collect(SearchResult{DocID: 3, Score: 3.0})

	results := c.Results()
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	// Results should be in descending score order
	for i := 1; i < len(results); i++ {
		if results[i-1].Score < results[i].Score {
			t.Errorf("results not in descending order: %v", results)
		}
	}
}

func TestTopKCollector_CollectMoreThanK(t *testing.T) {
	c := NewTopKCollector(3)
	for i := 0; i < 10; i++ {
		c.Collect(SearchResult{DocID: i, Score: float64(i)})
	}

	results := c.Results()
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	// Should keep top-3: scores 9, 8, 7
	expected := []float64{9, 8, 7}
	for i, want := range expected {
		if results[i].Score != want {
			t.Errorf("results[%d].Score = %v, want %v", i, results[i].Score, want)
		}
	}
}

func TestTopKCollector_DescendingOrder(t *testing.T) {
	c := NewTopKCollector(5)
	scores := []float64{3.0, 1.0, 4.0, 1.5, 2.0}
	for i, s := range scores {
		c.Collect(SearchResult{DocID: i, Score: s})
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
	for i := 0; i < 5; i++ {
		c.Collect(SearchResult{DocID: i, Score: 1.0})
	}

	results := c.Results()
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func TestTopKCollector_SingleElement(t *testing.T) {
	c := NewTopKCollector(1)
	c.Collect(SearchResult{DocID: 1, Score: 5.0})
	c.Collect(SearchResult{DocID: 2, Score: 10.0})
	c.Collect(SearchResult{DocID: 3, Score: 1.0})

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
	// Insert in descending order
	for i := 9; i >= 0; i-- {
		c.Collect(SearchResult{DocID: i, Score: float64(i)})
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
