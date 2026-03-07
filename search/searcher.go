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

// Search executes a Query across all segments and returns top-K results.
func (s *IndexSearcher) Search(q Query, topK int) []SearchResult {
	collector := NewTopKCollector(topK)

	for _, leaf := range s.reader.Leaves() {
		results := q.Execute(leaf.Segment)
		for _, ds := range results {
			if leaf.Segment.IsDeleted(ds.DocID) {
				continue
			}
			globalDocID := leaf.DocBase + ds.DocID
			stored, _ := leaf.Segment.StoredFields(ds.DocID)
			collector.Collect(SearchResult{
				DocID:  globalDocID,
				Score:  ds.Score,
				Fields: stored,
			})
		}
	}

	return collector.Results()
}

