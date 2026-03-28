// index/buffered_updates.go
package index

// deleteTermKey is the map key for deduplicating buffered delete terms.
type deleteTermKey struct {
	Field string
	Term  string
}

// bufferedDeleteTerm represents a delete term with its docIDUpto.
type bufferedDeleteTerm struct {
	Field     string
	Term      string
	DocIDUpto int
}

// BufferedUpdates holds buffered deletes for a single DWPT.
// Each delete term maps to a docIDUpto: the delete applies to all documents
// in this segment with docID < docIDUpto that match the term.
//
// Lucene equivalent: org.apache.lucene.index.BufferedUpdates (term deletes only)
type BufferedUpdates struct {
	deleteTerms map[deleteTermKey]int // field+term -> docIDUpto
}

func newBufferedUpdates() *BufferedUpdates {
	return &BufferedUpdates{
		deleteTerms: make(map[deleteTermKey]int),
	}
}

// addTerm records a delete-by-term with the given docIDUpto.
// If the same term already exists with a lower docIDUpto, the higher value wins.
// This matches Lucene's BufferedUpdates.addTerm semantics.
func (bu *BufferedUpdates) addTerm(field, term string, docIDUpto int) {
	key := deleteTermKey{Field: field, Term: term}
	if current, ok := bu.deleteTerms[key]; ok && docIDUpto <= current {
		return
	}
	bu.deleteTerms[key] = docIDUpto
}

// getDocIDUpto returns the docIDUpto for the given term, or -1 if not found.
func (bu *BufferedUpdates) getDocIDUpto(field, term string) int {
	if v, ok := bu.deleteTerms[deleteTermKey{Field: field, Term: term}]; ok {
		return v
	}
	return -1
}

// numTerms returns the number of distinct delete terms.
func (bu *BufferedUpdates) numTerms() int {
	return len(bu.deleteTerms)
}

// any returns true if there are any buffered deletes.
func (bu *BufferedUpdates) any() bool {
	return len(bu.deleteTerms) > 0
}

// clear removes all buffered deletes.
func (bu *BufferedUpdates) clear() {
	bu.deleteTerms = make(map[deleteTermKey]int)
}

// terms returns a snapshot of all delete terms.
func (bu *BufferedUpdates) terms() []bufferedDeleteTerm {
	result := make([]bufferedDeleteTerm, 0, len(bu.deleteTerms))
	for key, docIDUpto := range bu.deleteTerms {
		result = append(result, bufferedDeleteTerm{
			Field:     key.Field,
			Term:      key.Term,
			DocIDUpto: docIDUpto,
		})
	}
	return result
}
