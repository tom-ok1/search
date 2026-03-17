package search

import "gosearch/index"

// Occur represents the type of a boolean clause.
type Occur int

const (
	OccurMust    Occur = iota // AND
	OccurShould               // OR
	OccurMustNot              // NOT
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

// CreateWeight creates a Weight that recursively creates child Weights for each clause.
func (q *BooleanQuery) CreateWeight(searcher *IndexSearcher, scoreMode ScoreMode) Weight {
	w := &booleanWeight{query: q}
	for _, clause := range q.Clauses {
		childScoreMode := scoreMode
		if clause.Occur == OccurMustNot {
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

	var mainScorer Scorer

	if len(mustScorers) > 0 {
		mainScorer = NewConjunctionScorer(mustScorers)
		if len(shouldScorers) > 0 {
			mainScorer = newBoostedScorer(mainScorer, shouldScorers)
		}
	} else if len(shouldScorers) > 0 {
		mainScorer = NewDisjunctionScorer(shouldScorers)
	} else {
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
