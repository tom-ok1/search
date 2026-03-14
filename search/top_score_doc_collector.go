package search

import (
	"container/heap"

	"gosearch/index"
)

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

// GetLeafCollector returns a leaf-level collector that knows the DocBase offset.
func (c *TopKCollector) GetLeafCollector(ctx index.LeafReaderContext) LeafCollector {
	return &topKLeafCollector{parent: c, docBase: ctx.DocBase}
}

// ScoreMode returns ScoreModeComplete because score-based ranking needs scores.
func (c *TopKCollector) ScoreMode() ScoreMode { return ScoreModeComplete }

// Results returns collected documents in descending score order.
func (c *TopKCollector) Results() []SearchResult {
	sorted := make([]SearchResult, len(c.results))
	for i := len(c.results) - 1; i >= 0; i-- {
		sorted[i] = heap.Pop(&c.results).(SearchResult)
	}
	return sorted
}

// topKLeafCollector collects hits for a single segment into the parent TopKCollector.
type topKLeafCollector struct {
	parent  *TopKCollector
	docBase int
}

func (lc *topKLeafCollector) Collect(docID int, score float64) {
	globalDocID := lc.docBase + docID
	result := SearchResult{DocID: globalDocID, Score: score}
	if len(lc.parent.results) < lc.parent.k {
		heap.Push(&lc.parent.results, result)
	} else if score > lc.parent.results[0].Score {
		lc.parent.results[0] = result
		heap.Fix(&lc.parent.results, 0)
	}
}

// min-heap implementation ordered by score (lowest first)
type minHeap []SearchResult

var _ heap.Interface = (*minHeap)(nil)

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
