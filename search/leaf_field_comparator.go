package search

// LeafFieldComparator performs segment-local comparisons and copies.
type LeafFieldComparator interface {
	// SetBottom tells the comparator which slot is the current bottom.
	SetBottom(slot int)
	// CompareBottom compares the bottom (worst) slot with a new candidate doc.
	CompareBottom(docID int) int
	// Copy copies a doc's value into a slot.
	Copy(slot int, docID int)
	// SetScorer sets the current document's score.
	// Must be called before CompareBottom or Copy for score-based comparators.
	SetScorer(score float64)
	// CompetitiveIterator returns an iterator over competitive doc IDs
	// for block-level skipping, or nil if not supported.
	CompetitiveIterator() DocIdSetIterator
}
