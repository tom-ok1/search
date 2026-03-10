package index

import "gosearch/store"

// Posting holds occurrence information for a single term in a single document.
type Posting struct {
	DocID     int   // document ID
	Freq      int   // term frequency in the document
	Positions []int // list of positions where the term occurs
}

// PostingsList is the postings list for a term, sorted by DocID in ascending order.
type PostingsList struct {
	Term     string
	Postings []Posting
}

// PostingsIterator provides lazy, streaming access to postings for a single term.
// This is GoSearch's equivalent of Lucene's PostingsEnum.
type PostingsIterator interface {
	// Next advances to the next posting. Returns false when exhausted.
	Next() bool
	// DocID returns the current document ID. Only valid after Next() returns true.
	DocID() int
	// Freq returns the term frequency in the current document.
	Freq() int
	// Positions returns the term positions in the current document.
	Positions() []int
}

// ---------------------------------------------------------------------------
// SlicePostingsIterator (in-memory)
// ---------------------------------------------------------------------------

// SlicePostingsIterator adapts an existing []Posting slice to PostingsIterator.
type SlicePostingsIterator struct {
	postings []Posting
	idx      int
}

func NewSlicePostingsIterator(postings []Posting) *SlicePostingsIterator {
	return &SlicePostingsIterator{postings: postings, idx: -1}
}

func (it *SlicePostingsIterator) Next() bool {
	it.idx++
	return it.idx < len(it.postings)
}

func (it *SlicePostingsIterator) DocID() int {
	return it.postings[it.idx].DocID
}

func (it *SlicePostingsIterator) Freq() int {
	return it.postings[it.idx].Freq
}

func (it *SlicePostingsIterator) Positions() []int {
	return it.postings[it.idx].Positions
}

// ---------------------------------------------------------------------------
// EmptyPostingsIterator
// ---------------------------------------------------------------------------

// EmptyPostingsIterator is a PostingsIterator that yields no results.
type EmptyPostingsIterator struct{}

func (EmptyPostingsIterator) Next() bool       { return false }
func (EmptyPostingsIterator) DocID() int       { return -1 }
func (EmptyPostingsIterator) Freq() int        { return 0 }
func (EmptyPostingsIterator) Positions() []int { return nil }

// ---------------------------------------------------------------------------
// DiskPostingsIterator (mmap-based)
// ---------------------------------------------------------------------------

// DiskPostingsIterator reads postings from a mmap'd .tdat slice using delta decoding.
type DiskPostingsIterator struct {
	input     *store.MMapIndexInput
	remaining int // remaining postings to read
	prevDocID int // for delta decoding

	docID     int
	freq      int
	positions []int
}

func (it *DiskPostingsIterator) Next() bool {
	if it.remaining <= 0 {
		return false
	}
	it.remaining--

	// Read delta-encoded doc ID
	delta, err := it.input.ReadVInt()
	if err != nil {
		return false
	}
	it.docID = it.prevDocID + delta
	it.prevDocID = it.docID

	// Read frequency
	freq, err := it.input.ReadVInt()
	if err != nil {
		it.remaining = 0
		return false
	}
	it.freq = freq

	// Read positions (delta-encoded)
	posCount, err := it.input.ReadVInt()
	if err != nil {
		it.remaining = 0
		return false
	}
	it.positions = make([]int, posCount)
	prevPos := 0
	for i := 0; i < posCount; i++ {
		posDelta, err := it.input.ReadVInt()
		if err != nil {
			it.remaining = 0
			return false
		}
		it.positions[i] = prevPos + posDelta
		prevPos = it.positions[i]
	}

	return true
}

func (it *DiskPostingsIterator) DocID() int       { return it.docID }
func (it *DiskPostingsIterator) Freq() int        { return it.freq }
func (it *DiskPostingsIterator) Positions() []int { return it.positions }
