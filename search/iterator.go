package search

// NoMoreDocs is a sentinel value indicating the iterator is exhausted.
const NoMoreDocs = int(^uint(0) >> 1) // MaxInt

// DocIdSetIterator provides iteration over a set of document IDs.
// This is GoSearch's equivalent of Lucene's DocIdSetIterator.
type DocIdSetIterator interface {
	// DocID returns the current document ID, or -1 before iteration starts,
	// or NoMoreDocs if exhausted.
	DocID() int

	// NextDoc advances to the next document and returns its ID.
	// Returns NoMoreDocs when exhausted.
	NextDoc() int

	// Advance moves to the first document at or after target.
	// Returns NoMoreDocs if no such document exists.
	// The behavior is undefined if target <= current DocID.
	Advance(target int) int

	// Cost returns an estimated cost of iterating over all documents.
	// Used to optimize query execution order.
	Cost() int64
}
