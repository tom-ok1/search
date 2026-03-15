package search

import "container/heap"

// DisjunctionScorer implements OR semantics for multiple scorers.
// Uses a min-heap of scorers ordered by current docID.
// It also implements DocIdSetIterator directly to avoid per-call allocations.
type DisjunctionScorer struct {
	heap         *scorerHeap
	totalCost    int64
	docID        int
	currentScore float64
	toAdvance    []Scorer // reusable buffer
}

// NewDisjunctionScorer creates a DisjunctionScorer from multiple scorers.
// Nil scorers are filtered out. Returns nil if no valid scorers remain.
func NewDisjunctionScorer(scorers []Scorer) Scorer {
	var valid []Scorer
	for _, s := range scorers {
		if s != nil {
			valid = append(valid, s)
		}
	}
	if len(valid) == 0 {
		return nil
	}

	h := &scorerHeap{}
	var totalCost int64
	for _, s := range valid {
		iter := s.Iterator()
		totalCost += iter.Cost()
		if iter.NextDoc() != NoMoreDocs {
			heap.Push(h, s)
		}
	}

	if h.Len() == 0 {
		return nil
	}

	return &DisjunctionScorer{
		heap:      h,
		totalCost: totalCost,
		docID:     -1,
		toAdvance: make([]Scorer, 0, len(valid)),
	}
}

func (s *DisjunctionScorer) Iterator() DocIdSetIterator {
	return s
}

func (s *DisjunctionScorer) DocID() int {
	return s.docID
}

func (s *DisjunctionScorer) Score() float64 {
	return s.currentScore
}

func (s *DisjunctionScorer) NextDoc() int {
	if s.heap.Len() == 0 {
		s.docID = NoMoreDocs
		s.currentScore = 0
		return NoMoreDocs
	}

	top := (*s.heap)[0]
	doc := top.DocID()
	s.docID = doc
	s.currentScore = 0

	// Collect scores from all scorers at this docID, reusing buffer
	s.toAdvance = s.toAdvance[:0]
	for s.heap.Len() > 0 && (*s.heap)[0].DocID() == doc {
		scorer := heap.Pop(s.heap).(Scorer)
		s.currentScore += scorer.Score()
		s.toAdvance = append(s.toAdvance, scorer)
	}

	// Advance them to the next doc (after scoring)
	for _, scorer := range s.toAdvance {
		if scorer.Iterator().NextDoc() != NoMoreDocs {
			heap.Push(s.heap, scorer)
		}
	}

	return doc
}

func (s *DisjunctionScorer) Advance(target int) int {
	for s.heap.Len() > 0 && (*s.heap)[0].DocID() < target {
		scorer := heap.Pop(s.heap).(Scorer)
		if scorer.Iterator().Advance(target) != NoMoreDocs {
			heap.Push(s.heap, scorer)
		}
	}

	if s.heap.Len() == 0 {
		s.docID = NoMoreDocs
		return NoMoreDocs
	}

	return s.NextDoc()
}

func (s *DisjunctionScorer) Cost() int64 {
	return s.totalCost
}

// scorerHeap is a min-heap of scorers ordered by current docID.
type scorerHeap []Scorer

func (h scorerHeap) Len() int           { return len(h) }
func (h scorerHeap) Less(i, j int) bool { return h[i].DocID() < h[j].DocID() }
func (h scorerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *scorerHeap) Push(x any) {
	*h = append(*h, x.(Scorer))
}

func (h *scorerHeap) Pop() any {
	old := *h
	n := len(old)
	scorer := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return scorer
}
