package search

import (
	"gosearch/index"
)

// MatchNoneQuery matches no documents. Equivalent to Lucene's MatchNoDocsQuery.
type MatchNoneQuery struct{}

func NewMatchNoneQuery() *MatchNoneQuery {
	return &MatchNoneQuery{}
}

func (q *MatchNoneQuery) CreateWeight(searcher *IndexSearcher, scoreMode ScoreMode) Weight {
	return &matchNoneWeight{query: q}
}

func (q *MatchNoneQuery) ExtractTerms() []FieldTerm {
	return nil
}

type matchNoneWeight struct {
	query *MatchNoneQuery
}

func (w *matchNoneWeight) Query() Query {
	return w.query
}

func (w *matchNoneWeight) Scorer(ctx index.LeafReaderContext) Scorer {
	return nil
}
