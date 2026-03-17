package search

import "gosearch/index"

// Weight holds collection-level precomputations and creates per-segment Scorers.
// This is the intermediate layer between Query (immutable structure) and Scorer (per-segment execution).
type Weight interface {
	// Query returns the original query.
	Query() Query

	// Scorer creates a Scorer for the given segment, using precomputed collection-level statistics.
	// Returns nil if no documents match in this segment.
	Scorer(ctx index.LeafReaderContext) Scorer
}
