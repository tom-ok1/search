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
	for _, leaf := range s.reader.Leaves() {
		scorer := q.CreateScorer(leaf, scoreMode)
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
