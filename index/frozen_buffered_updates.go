// index/frozen_buffered_updates.go
package index

// frozenDeleteTerm is a delete term in a FrozenBufferedUpdates.
type frozenDeleteTerm struct {
	Field string
	Term  string
}

// FrozenBufferedUpdates is an immutable snapshot of delete operations
// for cross-segment application. Created by freezing a BufferedUpdates
// (typically the global buffer in DeleteQueue).
//
// When applied to existing segments, all matching documents are deleted
// regardless of docID (since these segments were fully committed before
// the delete was issued).
//
// Lucene equivalent: org.apache.lucene.index.FrozenBufferedUpdates
type FrozenBufferedUpdates struct {
	deleteTerms []frozenDeleteTerm
}

// newFrozenBufferedUpdates creates an immutable snapshot from a BufferedUpdates.
// The snapshot is independent — subsequent changes to the source don't affect it.
func newFrozenBufferedUpdates(bu *BufferedUpdates) *FrozenBufferedUpdates {
	terms := make([]frozenDeleteTerm, 0, len(bu.deleteTerms))
	for key := range bu.deleteTerms {
		terms = append(terms, frozenDeleteTerm{
			Field: key.Field,
			Term:  key.Term,
		})
	}
	return &FrozenBufferedUpdates{deleteTerms: terms}
}

// any returns true if there are any frozen delete terms.
func (f *FrozenBufferedUpdates) any() bool {
	return len(f.deleteTerms) > 0
}
