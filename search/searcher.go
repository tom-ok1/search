package search

import "gosearch/index"

// SearchResult represents a single search hit.
type SearchResult struct {
	DocID  int
	Score  float64
	Fields map[string]string
}

// SimpleSearch searches for a single term in the given field and returns matching documents.
// For BM25-scored search, use TermSearch instead.
func SimpleSearch(idx *index.InMemoryIndex, field, term string) []SearchResult {
	pl := idx.GetPostings(field, term)
	if pl == nil {
		return nil
	}

	var results []SearchResult
	for _, posting := range pl.Postings {
		results = append(results, SearchResult{
			DocID:  posting.DocID,
			Score:  1.0,
			Fields: idx.GetStoredFields(posting.DocID),
		})
	}
	return results
}

// TermSearch searches for a single term with BM25 scoring and returns the top-K results.
func TermSearch(idx *index.InMemoryIndex, field, term string, topK int) []SearchResult {
	pl := idx.GetPostings(field, term)
	if pl == nil {
		return nil
	}

	scorer := NewBM25Scorer()

	docCount := idx.DocCount()
	docFreq := len(pl.Postings)
	idf := scorer.IDF(docCount, docFreq)
	avgDocLen := idx.AvgFieldLength(field)

	collector := NewTopKCollector(topK)

	for _, posting := range pl.Postings {
		docLen := float64(idx.FieldLength(field, posting.DocID))
		score := scorer.Score(float64(posting.Freq), docLen, avgDocLen, idf)

		collector.Collect(SearchResult{
			DocID:  posting.DocID,
			Score:  score,
			Fields: idx.GetStoredFields(posting.DocID),
		})
	}

	return collector.Results()
}
