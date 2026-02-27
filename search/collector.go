package search

import "container/heap"

// TopKCollector collects the top-K documents by score using a min-heap.
type TopKCollector struct {
	k       int
	results minHeap
}

func NewTopKCollector(k int) *TopKCollector {
	return &TopKCollector{
		k:       k,
		results: make(minHeap, 0, k),
	}
}

// Collect adds a document to the collector.
// Only the top-K scoring documents are retained.
func (c *TopKCollector) Collect(result SearchResult) {
	if len(c.results) < c.k {
		heap.Push(&c.results, result)
	} else if result.Score > c.results[0].Score {
		c.results[0] = result
		heap.Fix(&c.results, 0)
	}
}

// Results returns collected documents in descending score order.
func (c *TopKCollector) Results() []SearchResult {
	sorted := make([]SearchResult, len(c.results))
	for i := len(c.results) - 1; i >= 0; i-- {
		sorted[i] = heap.Pop(&c.results).(SearchResult)
	}
	return sorted
}

// min-heap implementation ordered by score (lowest first)
type minHeap []SearchResult

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(SearchResult))
}

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	result := old[n-1]
	*h = old[:n-1]
	return result
}
