package search

// ConjunctionScorer implements AND semantics for multiple scorers.
// Uses the lead iterator (cheapest by cost) and advances others to match.
// It also implements DocIdSetIterator directly to avoid per-call allocations.
//
// Like Lucene's ConjunctionScorer, it separates "required" scorers (used for
// iteration/matching) from "scoring" scorers (used for score computation).
// This allows FILTER clauses to participate in matching without affecting scores.
type ConjunctionScorer struct {
	scorers        []Scorer           // all required scorers, sorted by cost
	scoringScorers []Scorer           // subset used for Score() computation
	iters          []DocIdSetIterator // cached iterators, [0] is the lead
	docID          int
}

// NewConjunctionScorer creates a ConjunctionScorer where all scorers
// contribute to both matching and scoring.
func NewConjunctionScorer(scorers []Scorer) Scorer {
	return NewConjunctionScorerWithScoring(scorers, scorers)
}

// NewConjunctionScorerWithScoring creates a ConjunctionScorer where `required`
// scorers determine matching, but only `scoringScorers` contribute to Score().
// This mirrors Lucene's ConjunctionScorer(required, scoringScorers) constructor.
func NewConjunctionScorerWithScoring(required []Scorer, scoringScorers []Scorer) Scorer {
	if len(required) == 0 {
		return nil
	}
	for _, s := range required {
		if s == nil {
			return nil
		}
	}

	// Cache iterators once, then sort scorers+iters together by cost
	n := len(required)
	sorted := make([]Scorer, n)
	copy(sorted, required)
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

	// Copy scoring scorers so the caller's slice isn't affected
	ss := make([]Scorer, len(scoringScorers))
	copy(ss, scoringScorers)

	return &ConjunctionScorer{
		scorers:        sorted,
		scoringScorers: ss,
		iters:          iters,
		docID:          -1,
	}
}

func (s *ConjunctionScorer) Iterator() DocIdSetIterator {
	return s
}

func (s *ConjunctionScorer) DocID() int {
	return s.docID
}

// Score returns the sum of scores from scoringScorers only.
// Their iterators are already positioned at the current doc because
// scoringScorers is a subset of required, whose iterators are all
// advanced together in advanceToMatch.
func (s *ConjunctionScorer) Score() float64 {
	total := 0.0
	for _, scorer := range s.scoringScorers {
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
