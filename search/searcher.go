package search

import "gosearch/index"

// SearchResult represents a single search hit.
type SearchResult struct {
	DocID      int // global DocID
	Score      float64
	Fields     map[string]string // stored fields
	SortValues []any             // populated by TopFieldCollector
}

// IndexSearcher searches across multiple segments.
type IndexSearcher struct {
	reader *index.IndexReader
}

func NewIndexSearcher(reader *index.IndexReader) *IndexSearcher {
	return &IndexSearcher{reader: reader}
}

// Search executes a Query across all segments, collecting results into the given Collector.
// StoredFields are populated on the final results after collection is complete.
func (s *IndexSearcher) Search(q Query, c Collector) []SearchResult {
	scoreMode := c.ScoreMode()
	weight := q.CreateWeight(s, scoreMode)

	for _, leaf := range s.reader.Leaves() {
		scorer := weight.Scorer(leaf)
		if scorer == nil {
			continue
		}

		lc := c.GetLeafCollector(leaf)
		lc.SetScorer(scorer)

		iter := scorer.Iterator()
		for iter.NextDoc() != NoMoreDocs {
			if !leaf.Segment.IsDeleted(iter.DocID()) {
				lc.Collect(iter.DocID())
			}
		}
	}

	results := c.Results()
	for i := range results {
		results[i].Fields = s.reader.GetStoredFields(results[i].DocID)
	}
	return results
}

// CollectionStatistics aggregates field-level statistics across all segments.
func (s *IndexSearcher) CollectionStatistics(field string) *CollectionStatistics {
	var docCount, sumTotalTermFreq int64
	for _, leaf := range s.reader.Leaves() {
		seg := leaf.Segment
		docCount += int64(seg.LiveDocCount())
		sumTotalTermFreq += int64(seg.TotalFieldLength(field))
	}
	if docCount == 0 {
		return nil
	}
	return &CollectionStatistics{
		Field:            field,
		MaxDoc:           int64(s.reader.TotalDocCount()),
		DocCount:         docCount,
		SumTotalTermFreq: sumTotalTermFreq,
	}
}

// TermStatistics aggregates term-level statistics across all segments.
func (s *IndexSearcher) TermStatistics(field, term string) *TermStatistics {
	var docFreq int64
	for _, leaf := range s.reader.Leaves() {
		docFreq += int64(leaf.Segment.DocFreq(field, term))
	}
	if docFreq == 0 {
		return nil
	}
	return &TermStatistics{
		Term:    term,
		DocFreq: docFreq,
	}
}
