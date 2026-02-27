package search

import "gosearch/index"

// TermQuery searches for a single term in a field.
type TermQuery struct {
	Field string
	Term  string
}

func NewTermQuery(field, term string) *TermQuery {
	return &TermQuery{Field: field, Term: term}
}

func (q *TermQuery) Execute(idx *index.InMemoryIndex) []DocScore {
	pl := idx.GetPostings(q.Field, q.Term)
	if pl == nil {
		return nil
	}

	scorer := NewBM25Scorer()
	docCount := idx.DocCount()
	docFreq := len(pl.Postings)
	idf := scorer.IDF(docCount, docFreq)
	avgDocLen := idx.AvgFieldLength(q.Field)

	var results []DocScore
	for _, posting := range pl.Postings {
		docLen := float64(idx.FieldLength(q.Field, posting.DocID))
		score := scorer.Score(float64(posting.Freq), docLen, avgDocLen, idf)
		results = append(results, DocScore{DocID: posting.DocID, Score: score})
	}
	return results
}
