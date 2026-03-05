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
		docFreq += leaf.Segment.DocFreq(field, term)
	}

	if docFreq == 0 {
		return nil
	}

	idf := scorer.IDF(totalDocCount, docFreq)

	// Compute avgDocLen across all segments
	totalLen := 0
	totalDocs := 0
	for _, leaf := range s.reader.Leaves() {
		totalLen += leaf.Segment.TotalFieldLength(field)
		totalDocs += leaf.Segment.DocCount()
	}
	avgDocLen := 0.0
	if totalDocs > 0 {
		avgDocLen = float64(totalLen) / float64(totalDocs)
	}

	// Search each segment
	for _, leaf := range s.reader.Leaves() {
		iter := leaf.Segment.PostingsIterator(field, term)
		for iter.Next() {
			if leaf.Segment.IsDeleted(iter.DocID()) {
				continue
			}

			docLen := float64(leaf.Segment.FieldLength(field, iter.DocID()))
			score := scorer.Score(float64(iter.Freq()), docLen, avgDocLen, idf)
			globalDocID := leaf.DocBase + iter.DocID()

			stored, _ := leaf.Segment.StoredFields(iter.DocID())
			collector.Collect(SearchResult{
				DocID:  globalDocID,
				Score:  score,
				Fields: stored,
			})
		}
	}

	return collector.Results()
}

// SimpleSearch searches for a single term in the given field and returns matching documents.
func SimpleSearch(seg index.SegmentReader, field, term string) []SearchResult {
	iter := seg.PostingsIterator(field, term)

	var results []SearchResult
	for iter.Next() {
		stored, _ := seg.StoredFields(iter.DocID())
		results = append(results, SearchResult{
			DocID:  iter.DocID(),
			Score:  1.0,
			Fields: stored,
		})
	}
	return results
}

// TermSearch searches for a single term with BM25 scoring and returns the top-K results.
func TermSearch(seg index.SegmentReader, field, term string, topK int) []SearchResult {
	docFreq := seg.DocFreq(field, term)
	if docFreq == 0 {
		return nil
	}

	scorer := NewBM25Scorer()

	docCount := seg.DocCount()
	idf := scorer.IDF(docCount, docFreq)

	totalFieldLen := seg.TotalFieldLength(field)
	avgDocLen := 0.0
	if docCount > 0 {
		avgDocLen = float64(totalFieldLen) / float64(docCount)
	}

	collector := NewTopKCollector(topK)

	iter := seg.PostingsIterator(field, term)
	for iter.Next() {
		docLen := float64(seg.FieldLength(field, iter.DocID()))
		score := scorer.Score(float64(iter.Freq()), docLen, avgDocLen, idf)

		stored, _ := seg.StoredFields(iter.DocID())
		collector.Collect(SearchResult{
			DocID:  iter.DocID(),
			Score:  score,
			Fields: stored,
		})
	}

	return collector.Results()
}
