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

func (q *TermQuery) Execute(seg index.SegmentReader) []DocScore {
	docFreq := seg.DocFreq(q.Field, q.Term)
	if docFreq == 0 {
		return nil
	}

	scorer := NewBM25Scorer()
	docCount := seg.DocCount()
	idf := scorer.IDF(docCount, docFreq)

	totalFieldLen := seg.TotalFieldLength(q.Field)
	avgDocLen := 0.0
	if docCount > 0 {
		avgDocLen = float64(totalFieldLen) / float64(docCount)
	}

	var results []DocScore
	iter := seg.PostingsIterator(q.Field, q.Term)
	for iter.Next() {
		docLen := float64(seg.FieldLength(q.Field, iter.DocID()))
		score := scorer.Score(float64(iter.Freq()), docLen, avgDocLen, idf)
		results = append(results, DocScore{DocID: iter.DocID(), Score: score})
	}
	return results
}
