package search

import (
	"container/heap"

	"gosearch/index"
)

// fieldEntry represents a single document in the top-K field-sorted queue.
type fieldEntry struct {
	slot        int
	globalDocID int
	score       float64
}

// TopFieldCollector collects top-K documents sorted by field values.
type TopFieldCollector struct {
	k           int
	comparators []FieldComparator
	reverses    []bool
	heap        *fieldHeap
	totalHits   int
	nextSlot    int
	needsScore  bool
}

func NewTopFieldCollector(k int, sort *Sort) *TopFieldCollector {
	comparators := make([]FieldComparator, len(sort.Fields))
	reverses := make([]bool, len(sort.Fields))
	needsScore := false

	for i, sf := range sort.Fields {
		reverses[i] = sf.Reverse
		switch sf.Type {
		case SortFieldScore:
			comparators[i] = NewScoreFieldComparator(k)
			needsScore = true
		case SortFieldNumeric:
			comparators[i] = NewNumericFieldComparator(sf.Field, k)
		case SortFieldString:
			comparators[i] = NewStringFieldComparator(sf.Field, k)
		default:
			comparators[i] = NewScoreFieldComparator(k)
			needsScore = true
		}
	}

	h := &fieldHeap{
		entries:     make([]*fieldEntry, 0, k),
		comparators: comparators,
		reverses:    reverses,
	}

	return &TopFieldCollector{
		k:           k,
		comparators: comparators,
		reverses:    reverses,
		heap:        h,
		needsScore:  needsScore,
	}
}

// GetLeafCollector returns a leaf-level collector for the given segment.
func (c *TopFieldCollector) GetLeafCollector(ctx index.LeafReaderContext) LeafCollector {
	leafComps := make([]LeafFieldComparator, len(c.comparators))
	for i, comp := range c.comparators {
		leafComps[i] = comp.GetLeafComparator(ctx.Segment)
	}
	return &topFieldLeafCollector{
		parent:          c,
		docBase:         ctx.DocBase,
		leafComparators: leafComps,
	}
}

// ScoreMode returns ScoreModeComplete only if a score-based sort field is present.
func (c *TopFieldCollector) ScoreMode() ScoreMode {
	if c.needsScore {
		return ScoreModeComplete
	}
	return ScoreModeNone
}

// compareWithBottom returns > 0 if the candidate doc is better than the current bottom.
func (c *TopFieldCollector) compareWithBottom(leafComps []LeafFieldComparator, localDocID int) int {
	for i, lc := range leafComps {
		cmp := lc.CompareBottom(localDocID)
		if c.reverses[i] {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// Results returns collected documents in sort order.
func (c *TopFieldCollector) Results() []SearchResult {
	n := c.heap.Len()
	results := make([]SearchResult, n)
	for i := n - 1; i >= 0; i-- {
		entry := heap.Pop(c.heap).(*fieldEntry)
		sortValues := make([]any, len(c.comparators))
		for j, comp := range c.comparators {
			sortValues[j] = comp.Value(entry.slot)
		}
		results[i] = SearchResult{
			DocID:      entry.globalDocID,
			Score:      entry.score,
			SortValues: sortValues,
		}
	}
	return results
}

// fieldHeap is a min-heap of fieldEntry, ordered by comparators.
// The "worst" entry (bottom of top-K) is at index 0.
type fieldHeap struct {
	entries     []*fieldEntry
	comparators []FieldComparator
	reverses    []bool
}

func (h *fieldHeap) Len() int { return len(h.entries) }

func (h *fieldHeap) Less(i, j int) bool {
	si := h.entries[i].slot
	sj := h.entries[j].slot
	for idx, comp := range h.comparators {
		cmp := comp.CompareSlots(si, sj)
		if h.reverses[idx] {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp > 0
		}
	}
	return false
}

func (h *fieldHeap) Swap(i, j int) { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }

func (h *fieldHeap) Push(x any) {
	h.entries = append(h.entries, x.(*fieldEntry))
}

func (h *fieldHeap) Pop() any {
	old := h.entries
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	h.entries = old[:n-1]
	return entry
}

// topFieldLeafCollector collects hits for a single segment into the parent TopFieldCollector.
type topFieldLeafCollector struct {
	parent          *TopFieldCollector
	docBase         int
	leafComparators []LeafFieldComparator
	scorer          Scorable
}

func (lc *topFieldLeafCollector) SetScorer(scorer Scorable) {
	lc.scorer = scorer
}

func (lc *topFieldLeafCollector) Collect(docID int) {
	var score float64
	if lc.parent.needsScore {
		score = lc.scorer.Score()
	}
	globalDocID := lc.docBase + docID
	c := lc.parent
	c.totalHits++

	for _, leafComp := range lc.leafComparators {
		leafComp.SetScorer(score)
	}

	if c.heap.Len() < c.k {
		slot := c.nextSlot
		c.nextSlot++
		for _, leafComp := range lc.leafComparators {
			leafComp.Copy(slot, docID)
		}
		entry := &fieldEntry{
			slot:        slot,
			globalDocID: globalDocID,
			score:       score,
		}
		heap.Push(c.heap, entry)
		if c.heap.Len() == c.k {
			bottom := c.heap.entries[0]
			for _, leafComp := range lc.leafComparators {
				leafComp.SetBottom(bottom.slot)
			}
		}
	} else {
		cmp := c.compareWithBottom(lc.leafComparators, docID)
		if cmp > 0 {
			bottom := c.heap.entries[0]
			slot := bottom.slot
			for _, leafComp := range lc.leafComparators {
				leafComp.Copy(slot, docID)
			}
			bottom.globalDocID = globalDocID
			bottom.score = score
			heap.Fix(c.heap, 0)
			newBottom := c.heap.entries[0]
			for _, leafComp := range lc.leafComparators {
				leafComp.SetBottom(newBottom.slot)
			}
		}
	}
}
