package search

import (
	"container/heap"

	"gosearch/index"
)

// TopKCollector collects the top-K documents by score using a min-heap.
type TopKCollector struct {
	k         int
	totalHits int
	results   minHeap
}

func NewTopKCollector(k int) *TopKCollector {
	if k < 1 {
		k = 1
	}
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

// TotalHits returns the total number of documents that matched the query.
func (c *TopKCollector) TotalHits() int { return c.totalHits }

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
	scorer  Scorable
}

func (lc *topKLeafCollector) SetScorer(scorer Scorable) {
	lc.scorer = scorer
}

func (lc *topKLeafCollector) CompetitiveIterator() DocIdSetIterator { return nil }

func (lc *topKLeafCollector) Collect(docID int) {
	score := lc.scorer.Score()
	lc.parent.totalHits++
	globalDocID := lc.docBase + docID
	result := SearchResult{DocID: globalDocID, Score: score}
	if len(lc.parent.results) < lc.parent.k {
		heap.Push(&lc.parent.results, result)
	} else if score > lc.parent.results[0].Score ||
		(score == lc.parent.results[0].Score && globalDocID < lc.parent.results[0].DocID) {
		lc.parent.results[0] = result
		heap.Fix(&lc.parent.results, 0)
	}
}

// min-heap implementation ordered by score (lowest first)
type minHeap []SearchResult

var _ heap.Interface = (*minHeap)(nil)

func (h minHeap) Len() int { return len(h) }
func (h minHeap) Less(i, j int) bool {
	if h[i].Score != h[j].Score {
		return h[i].Score < h[j].Score
	}
	// Tie-break: higher DocID is "less" in the min-heap, so lower DocIDs survive eviction.
	// This matches Lucene's convention where lower docIDs win ties.
	return h[i].DocID > h[j].DocID
}
func (h minHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x any) {
	*h = append(*h, x.(SearchResult))
}

func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	result := old[n-1]
	*h = old[:n-1]
	return result
}
