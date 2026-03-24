package search

// LeafCollector collects hits within a single segment.
type LeafCollector interface {
	// SetScorer sets the Scorable for lazy score retrieval.
	// Called before any Collect calls for the segment.
	SetScorer(scorer Scorable)

	// Collect collects a matching document.
	// The score can be retrieved from the Scorable set via SetScorer.
	Collect(docID int)

	// CompetitiveIterator returns an iterator over competitive doc IDs
	// for block-level skipping, or nil if not supported.
	CompetitiveIterator() DocIdSetIterator
}
