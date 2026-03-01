package search

import "gosearch/index"

// SearchResult represents a single search hit.
type SearchResult struct {
	DocID  int               // global DocID
	Score  float64
	Fields map[string]string // stored fields
}

// IndexSearcher searches across multiple segments.
type IndexSearcher struct {
	reader *index.IndexReader
}

func NewIndexSearcher(reader *index.IndexReader) *IndexSearcher {
	return &IndexSearcher{reader: reader}
}

// Search executes a term query across all segments and returns the top-K results.
func (s *IndexSearcher) Search(field, term string, topK int) []SearchResult {
	scorer := NewBM25Scorer()
	collector := NewTopKCollector(topK)

	totalDocCount := s.reader.TotalDocCount()

	// Compute docFreq across all segments (needed for IDF)
	docFreq := 0
	for _, leaf := range s.reader.Leaves() {
		pl := leaf.Segment.GetPostings(field, term)
		if pl == nil {
			continue
		}
		docFreq += len(pl.Postings)
	}

	if docFreq == 0 {
		return nil
	}

	idf := scorer.IDF(totalDocCount, docFreq)

	// Compute avgDocLen across all segments
	totalLen := 0
	totalDocs := 0
	for _, leaf := range s.reader.Leaves() {
		lengths := leaf.Segment.GetFieldLengths(field)
		if lengths == nil {
			continue
		}
		for _, l := range lengths {
			totalLen += l
			totalDocs++
		}
	}
	avgDocLen := 0.0
	if totalDocs > 0 {
		avgDocLen = float64(totalLen) / float64(totalDocs)
	}

	// Search each segment
	for _, leaf := range s.reader.Leaves() {
		pl := leaf.Segment.GetPostings(field, term)
		if pl == nil {
			continue
		}

		for _, posting := range pl.Postings {
			// Skip deleted documents
			if leaf.Segment.IsDeleted(posting.DocID) {
				continue
			}

			docLen := 0.0
			if lengths := leaf.Segment.GetFieldLengths(field); lengths != nil && posting.DocID < len(lengths) {
				docLen = float64(lengths[posting.DocID])
			}

			score := scorer.Score(float64(posting.Freq), docLen, avgDocLen, idf)
			globalDocID := leaf.DocBase + posting.DocID

			collector.Collect(SearchResult{
				DocID:  globalDocID,
				Score:  score,
				Fields: leaf.Segment.GetStoredFields(posting.DocID),
			})
		}
	}

	return collector.Results()
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
