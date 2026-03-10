package index

import "gosearch/fst"

// TermIterator iterates over all terms in a single field of a DiskSegment.
type TermIterator struct {
	fstIter *fst.FSTIterator
	term    string
	ordinal int
}

// TermIterator returns an iterator over all terms for the given field.
// Returns nil if the field does not exist.
func (ds *DiskSegment) TermIterator(field string) *TermIterator {
	termFST := ds.termFSTs[field]
	if termFST == nil {
		return nil
	}
	return &TermIterator{
		fstIter: termFST.Iterator(),
	}
}

// Next advances to the next term. Returns false when exhausted.
func (it *TermIterator) Next() bool {
	if it.fstIter.Next() {
		it.term = string(it.fstIter.Key())
		it.ordinal = int(it.fstIter.Value())
		return true
	}
	return false
}

// Term returns the current term string.
func (it *TermIterator) Term() string {
	return it.term
}

// Ordinal returns the current term's ordinal (FST output).
func (it *TermIterator) Ordinal() int {
	return it.ordinal
}
