package search

import "gosearch/index"

// Query represents a search query that can create a Scorer for execution.
type Query interface {
	// CreateScorer creates a Scorer for the given segment context.
	// Returns nil if no documents match in this segment.
	CreateScorer(ctx index.LeafReaderContext, scoreMode ScoreMode) Scorer
}
