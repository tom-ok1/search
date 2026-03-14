package search

import "gosearch/index"

// SearchResult represents a single search hit.
type SearchResult struct {
	DocID      int               // global DocID
	Score      float64
	Fields     map[string]string // stored fields
	SortValues []interface{}     // populated by TopFieldCollector
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
	for _, leaf := range s.reader.Leaves() {
		lc := c.GetLeafCollector(leaf)
		for _, ds := range q.Execute(leaf.Segment) {
			if leaf.Segment.IsDeleted(ds.DocID) {
				continue
			}
			lc.Collect(ds.DocID, ds.Score)
		}
	}

	results := c.Results()
	for i := range results {
		results[i].Fields = s.reader.GetStoredFields(results[i].DocID)
	}
	return results
}

