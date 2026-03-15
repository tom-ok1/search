package search

// Scorer iterates over matching documents and provides their scores.
// This is GoSearch's equivalent of Lucene's Scorer.
type Scorer interface {
	Scorable

	// Iterator returns the underlying document iterator.
	Iterator() DocIdSetIterator
}

// Scorable provides access to the score of the current document.
// Used by LeafCollector to lazily retrieve scores when needed.
type Scorable interface {
	// Score returns the score for the current document.
	Score() float64

	// DocID returns the current document ID.
	DocID() int
}
