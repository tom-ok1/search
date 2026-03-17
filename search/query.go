package search

// Query represents a search query that can create a Weight for execution.
type Query interface {
	// CreateWeight creates a Weight that holds collection-level precomputations.
	// The searcher is used to access collection-wide statistics across all segments.
	CreateWeight(searcher *IndexSearcher, scoreMode ScoreMode) Weight
}
