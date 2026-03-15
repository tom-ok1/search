package search

// ConjunctionScorer implements AND semantics for multiple scorers.
// Uses the lead iterator (cheapest by cost) and advances others to match.
// It also implements DocIdSetIterator directly to avoid per-call allocations.
type ConjunctionScorer struct {
	scorers []Scorer
	iters   []DocIdSetIterator // cached iterators, [0] is the lead
	docID   int
}

// NewConjunctionScorer creates a ConjunctionScorer from multiple scorers.
// Returns nil if any scorer is nil (no documents can match).
func NewConjunctionScorer(scorers []Scorer) Scorer {
	if len(scorers) == 0 {
		return nil
	}
	for _, s := range scorers {
		if s == nil {
			return nil
		}
	}

	// Cache iterators once, then sort scorers+iters together by cost
	n := len(scorers)
	sorted := make([]Scorer, n)
	copy(sorted, scorers)
	iters := make([]DocIdSetIterator, n)
	costs := make([]int64, n)
	for i, s := range sorted {
		iters[i] = s.Iterator()
		costs[i] = iters[i].Cost()
	}
	// Bubble sort is fine for the typically small number of clauses
	for i := range n {
		for j := i + 1; j < n; j++ {
			if costs[j] < costs[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
				iters[i], iters[j] = iters[j], iters[i]
				costs[i], costs[j] = costs[j], costs[i]
			}
		}
	}

	return &ConjunctionScorer{
		scorers: sorted,
		iters:   iters,
		docID:   -1,
	}
}

func (s *ConjunctionScorer) Iterator() DocIdSetIterator {
	return s
}

func (s *ConjunctionScorer) DocID() int {
	return s.docID
}

func (s *ConjunctionScorer) Score() float64 {
	total := 0.0
	for _, scorer := range s.scorers {
		total += scorer.Score()
	}
	return total
}

func (s *ConjunctionScorer) NextDoc() int {
	return s.advanceToMatch(s.iters[0].NextDoc())
}

func (s *ConjunctionScorer) Advance(target int) int {
	return s.advanceToMatch(s.iters[0].Advance(target))
}

func (s *ConjunctionScorer) Cost() int64 {
	return s.iters[0].Cost()
}

// advanceToMatch finds the next document that matches all sub-scorers.
func (s *ConjunctionScorer) advanceToMatch(doc int) int {
	for doc != NoMoreDocs {
		allMatch := true
		for i := 1; i < len(s.iters); i++ {
			otherDoc := s.iters[i].DocID()
			if otherDoc < doc {
				otherDoc = s.iters[i].Advance(doc)
			}
			if otherDoc > doc {
				doc = s.iters[0].Advance(otherDoc)
				allMatch = false
				break
			}
		}
		if allMatch {
			s.docID = doc
			return doc
		}
	}
	s.docID = NoMoreDocs
	return NoMoreDocs
}
