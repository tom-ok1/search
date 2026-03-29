package search

import (
	"gosearch/index"
)

// MatchAllQuery matches every non-deleted document with a constant score of 1.0.
type MatchAllQuery struct{}

func NewMatchAllQuery() *MatchAllQuery {
	return &MatchAllQuery{}
}

func (q *MatchAllQuery) CreateWeight(searcher *IndexSearcher, scoreMode ScoreMode) Weight {
	return &matchAllWeight{query: q}
}

func (q *MatchAllQuery) ExtractTerms() []FieldTerm {
	return nil
}

type matchAllWeight struct {
	query *MatchAllQuery
}

func (w *matchAllWeight) Query() Query {
	return w.query
}

func (w *matchAllWeight) Scorer(ctx index.LeafReaderContext) Scorer {
	maxDoc := ctx.Segment.DocCount()
	liveDocs := ctx.Segment.LiveDocs()
	return &matchAllScorer{
		maxDoc:   maxDoc,
		liveDocs: liveDocs,
		doc:      -1,
	}
}

type matchAllScorer struct {
	maxDoc   int
	liveDocs *index.Bitset
	doc      int
}

func (s *matchAllScorer) Score() float64 {
	return 1.0
}

func (s *matchAllScorer) DocID() int {
	return s.doc
}

func (s *matchAllScorer) Iterator() DocIdSetIterator {
	return s
}

func (s *matchAllScorer) NextDoc() int {
	for {
		s.doc++
		if s.doc >= s.maxDoc {
			s.doc = NoMoreDocs
			return NoMoreDocs
		}
		if s.liveDocs == nil || !s.liveDocs.Get(s.doc) {
			return s.doc
		}
	}
}

func (s *matchAllScorer) Advance(target int) int {
	s.doc = target - 1
	return s.NextDoc()
}

func (s *matchAllScorer) Cost() int64 {
	return int64(s.maxDoc)
}
