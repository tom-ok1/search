package search

import "gosearch/index"

// Occur represents the type of a boolean clause.
type Occur int

const (
	OccurMust    Occur = iota // AND (contributes to scoring)
	OccurShould               // OR  (contributes to scoring)
	OccurMustNot              // NOT (no scoring)
	OccurFilter               // AND (no scoring)
)

// BooleanClause is a single clause in a BooleanQuery.
type BooleanClause struct {
	Query Query
	Occur Occur
}

// BooleanQuery combines multiple query clauses with boolean logic.
type BooleanQuery struct {
	Clauses []BooleanClause
}

func NewBooleanQuery() *BooleanQuery {
	return &BooleanQuery{}
}

func (q *BooleanQuery) Add(query Query, occur Occur) *BooleanQuery {
	q.Clauses = append(q.Clauses, BooleanClause{Query: query, Occur: occur})
	return q
}

func (q *BooleanQuery) ExtractTerms() []FieldTerm {
	var terms []FieldTerm
	for _, clause := range q.Clauses {
		if clause.Occur != OccurMustNot {
			terms = append(terms, clause.Query.ExtractTerms()...)
		}
	}
	return terms
}

// CreateWeight creates a Weight that recursively creates child Weights for each clause.
func (q *BooleanQuery) CreateWeight(searcher *IndexSearcher, scoreMode ScoreMode) Weight {
	w := &booleanWeight{query: q}
	for _, clause := range q.Clauses {
		childScoreMode := scoreMode
		if clause.Occur == OccurMustNot || clause.Occur == OccurFilter {
			childScoreMode = ScoreModeNone
		}
		cw := clause.Query.CreateWeight(searcher, childScoreMode)
		w.clauseWeights = append(w.clauseWeights, clauseWeightEntry{weight: cw, occur: clause.Occur})
	}
	return w
}

// booleanWeight holds child Weights for each clause.
type booleanWeight struct {
	query         *BooleanQuery
	clauseWeights []clauseWeightEntry
}

type clauseWeightEntry struct {
	weight Weight
	occur  Occur
}

func (w *booleanWeight) Query() Query { return w.query }

func (w *booleanWeight) Scorer(ctx index.LeafReaderContext) Scorer {
	var mustScorers []Scorer
	var filterScorers []Scorer
	var shouldScorers []Scorer
	var mustNotScorers []Scorer

	for _, cw := range w.clauseWeights {
		scorer := cw.weight.Scorer(ctx)

		switch cw.occur {
		case OccurMust:
			if scorer == nil {
				return nil
			}
			mustScorers = append(mustScorers, scorer)
		case OccurFilter:
			if scorer == nil {
				return nil
			}
			filterScorers = append(filterScorers, scorer)
		case OccurShould:
			if scorer != nil {
				shouldScorers = append(shouldScorers, scorer)
			}
		case OccurMustNot:
			if scorer != nil {
				mustNotScorers = append(mustNotScorers, scorer)
			}
		}
	}

	// Build the required scorer by combining MUST (scoring) and FILTER (non-scoring)
	// clauses, following Lucene's BooleanScorerSupplier.req() approach.
	reqScorer := buildRequiredScorer(filterScorers, mustScorers)

	var mainScorer Scorer

	if reqScorer != nil {
		if len(shouldScorers) > 0 {
			// MUST/FILTER + SHOULD: SHOULD boosts the score
			mainScorer = newBoostedScorer(reqScorer, shouldScorers)
		} else {
			mainScorer = reqScorer
		}
	} else if len(shouldScorers) > 0 {
		mainScorer = NewDisjunctionScorer(shouldScorers)
	}

	if mainScorer == nil {
		return nil
	}

	if len(mustNotScorers) > 0 {
		excl := NewDisjunctionScorer(mustNotScorers)
		if excl != nil {
			mainScorer = newExclusionScorer(mainScorer, excl)
		}
	}

	return mainScorer
}

// boostedScorer adds scores from booster scorers to the main scorer.
type boostedScorer struct {
	main         Scorer
	boosters     []Scorer
	boosterIters []DocIdSetIterator
}

func newBoostedScorer(main Scorer, boosters []Scorer) *boostedScorer {
	iters := make([]DocIdSetIterator, len(boosters))
	for i, b := range boosters {
		iters[i] = b.Iterator()
	}
	return &boostedScorer{main: main, boosters: boosters, boosterIters: iters}
}

func (s *boostedScorer) Iterator() DocIdSetIterator {
	return s.main.Iterator()
}

func (s *boostedScorer) DocID() int {
	return s.main.DocID()
}

func (s *boostedScorer) Score() float64 {
	score := s.main.Score()
	doc := s.main.DocID()
	for i, b := range s.boosters {
		if b.DocID() == doc {
			score += b.Score()
		} else if b.DocID() < doc {
			s.boosterIters[i].Advance(doc)
			if b.DocID() == doc {
				score += b.Score()
			}
		}
	}
	return score
}

// exclusionScorer filters out documents that match the excluded scorer.
type exclusionScorer struct {
	main     Scorer
	excluded Scorer
	mainIter DocIdSetIterator
	excIter  DocIdSetIterator
	docID    int
}

func newExclusionScorer(main, excluded Scorer) *exclusionScorer {
	return &exclusionScorer{
		main:     main,
		excluded: excluded,
		mainIter: main.Iterator(),
		excIter:  excluded.Iterator(),
		docID:    -1,
	}
}

func (s *exclusionScorer) Iterator() DocIdSetIterator {
	return s
}

func (s *exclusionScorer) DocID() int {
	return s.docID
}

func (s *exclusionScorer) Score() float64 {
	return s.main.Score()
}

func (s *exclusionScorer) NextDoc() int {
	return s.advanceToNonExcluded(s.mainIter.NextDoc())
}

func (s *exclusionScorer) Advance(target int) int {
	return s.advanceToNonExcluded(s.mainIter.Advance(target))
}

func (s *exclusionScorer) Cost() int64 {
	return s.mainIter.Cost()
}

func (s *exclusionScorer) advanceToNonExcluded(doc int) int {
	for doc != NoMoreDocs {
		excDoc := s.excluded.DocID()
		if excDoc < doc {
			excDoc = s.excIter.Advance(doc)
		}
		if excDoc != doc {
			s.docID = doc
			return doc
		}
		doc = s.mainIter.NextDoc()
	}
	s.docID = NoMoreDocs
	return NoMoreDocs
}

// buildRequiredScorer combines FILTER (non-scoring) and MUST (scoring) scorers
// into a single scorer, following Lucene's BooleanScorerSupplier.req() method.
// All scorers participate in matching, but only MUST scorers contribute to scoring.
func buildRequiredScorer(filterScorers, mustScorers []Scorer) Scorer {
	totalRequired := len(filterScorers) + len(mustScorers)
	if totalRequired == 0 {
		return nil
	}

	// Single clause shortcut
	if totalRequired == 1 {
		if len(mustScorers) == 1 {
			return mustScorers[0]
		}
		// Single FILTER only — wrap to return 0 score
		return newConstantScoreScorer(filterScorers[0], 0.0)
	}

	// Multiple clauses: combine all into a ConjunctionScorer with scoring separation
	allRequired := make([]Scorer, 0, totalRequired)
	allRequired = append(allRequired, filterScorers...)
	allRequired = append(allRequired, mustScorers...)

	if len(mustScorers) == 0 {
		// All filters, no scoring — score is always 0
		return NewConjunctionScorerWithScoring(allRequired, nil)
	}

	return NewConjunctionScorerWithScoring(allRequired, mustScorers)
}

// constantScoreScorer wraps a scorer and returns a constant score.
type constantScoreScorer struct {
	inner Scorer
	score float64
}

func newConstantScoreScorer(inner Scorer, score float64) Scorer {
	return &constantScoreScorer{inner: inner, score: score}
}

func (s *constantScoreScorer) Iterator() DocIdSetIterator { return s.inner.Iterator() }
func (s *constantScoreScorer) DocID() int                 { return s.inner.DocID() }
func (s *constantScoreScorer) Score() float64             { return s.score }
