package index

import (
	"gosearch/store"
	"sort"
)

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
	// Advance advances to the first posting with DocID >= target.
	// Returns true if such a posting exists.
	Advance(target int) bool
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

func (it *SlicePostingsIterator) Advance(target int) bool {
	start := it.idx + 1
	i := sort.Search(len(it.postings)-start, func(i int) bool {
		return it.postings[start+i].DocID >= target
	})
	it.idx = start + i
	return it.idx < len(it.postings)
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
func (EmptyPostingsIterator) Advance(int) bool { return false }

// ---------------------------------------------------------------------------
// DiskPostingsIterator (mmap-based)
// ---------------------------------------------------------------------------

// DiskPostingsIterator reads postings from a mmap'd .tdat slice using delta decoding.
// Positions are decoded lazily: Next() skips position bytes, and Positions()
// seeks back to decode them on demand.
type DiskPostingsIterator struct {
	input     *store.MMapIndexInput
	remaining int // remaining postings to read
	prevDocID int // for delta decoding

	docID            int
	freq             int
	positions        []int
	posCount         int  // number of position VInts to decode
	posStartOffset   int  // file offset where position data begins
	positionsDecoded bool // true after Positions() has been called for current posting
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

	// Read position count, save offset, skip position VInts
	posCount, err := it.input.ReadVInt()
	if err != nil {
		it.remaining = 0
		return false
	}
	it.posCount = posCount
	it.posStartOffset = it.input.Position()
	it.positions = nil
	it.positionsDecoded = false

	// Skip past position VInts without decoding
	for range posCount {
		if _, err := it.input.ReadVInt(); err != nil {
			it.remaining = 0
			return false
		}
	}

	return true
}

func (it *DiskPostingsIterator) DocID() int { return it.docID }
func (it *DiskPostingsIterator) Freq() int  { return it.freq }
func (it *DiskPostingsIterator) Positions() []int {
	if it.positionsDecoded {
		return it.positions
	}
	it.positionsDecoded = true

	if it.posCount == 0 {
		return nil
	}

	// Save current position, seek back to position data, decode, restore
	savedPos := it.input.Position()
	it.input.Seek(it.posStartOffset)

	it.positions = make([]int, it.posCount)
	prevPos := 0
	for i := range it.posCount {
		posDelta, err := it.input.ReadVInt()
		if err != nil {
			it.input.Seek(savedPos)
			return it.positions[:i]
		}
		it.positions[i] = prevPos + posDelta
		prevPos = it.positions[i]
	}

	it.input.Seek(savedPos)
	return it.positions
}

func (it *DiskPostingsIterator) Advance(target int) bool {
	for it.Next() {
		if it.docID >= target {
			return true
		}
	}
	return false
}
