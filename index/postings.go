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

// DiskPostingsIterator reads postings from mmap'd .tdat and .tpos slices.
// Doc IDs and frequencies are read from .tdat; positions from .tpos.
//
// The slice returned by Positions() is reused across Next() calls.
// Callers that need positions to outlive the next Next() call must copy the slice.
type DiskPostingsIterator struct {
	input    *store.MMapIndexInput // .tdat slice (doc/freq)
	posInput *store.MMapIndexInput // .tpos slice (positions)

	remaining int // remaining postings to read
	prevDocID int // for delta decoding

	docID            int
	freq             int
	positions        []int
	posCount         int  // number of position VInts for current posting
	positionsDecoded bool // true after Positions() has been called for current posting
	posSkipPending   bool // true if previous posting's positions were not consumed
	prevPosCount     int  // position count from previous posting (for skipping)
}

func (it *DiskPostingsIterator) Next() bool {
	if it.remaining <= 0 {
		return false
	}
	it.remaining--

	// Skip unconsumed positions from the previous posting in .tpos
	if it.posInput != nil && it.posSkipPending {
		for range it.prevPosCount {
			if _, err := it.posInput.ReadVInt(); err != nil {
				it.remaining = 0
				return false
			}
		}
	}

	// Read delta-encoded doc ID from .tdat
	delta, err := it.input.ReadVInt()
	if err != nil {
		return false
	}
	it.docID = it.prevDocID + delta
	it.prevDocID = it.docID

	// Read frequency from .tdat
	freq, err := it.input.ReadVInt()
	if err != nil {
		it.remaining = 0
		return false
	}
	it.freq = freq

	// Read position count from .tpos (if available)
	it.posCount = 0
	it.positions = nil
	it.positionsDecoded = false
	it.posSkipPending = false
	if it.posInput != nil {
		posCount, err := it.posInput.ReadVInt()
		if err != nil {
			it.remaining = 0
			return false
		}
		it.posCount = posCount
		it.posSkipPending = true
		it.prevPosCount = posCount
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
	it.posSkipPending = false

	if it.posInput == nil || it.posCount == 0 {
		return nil
	}

	if cap(it.positions) >= it.posCount {
		it.positions = it.positions[:it.posCount]
	} else {
		it.positions = make([]int, it.posCount)
	}
	prevPos := 0
	for i := range it.posCount {
		posDelta, err := it.posInput.ReadVInt()
		if err != nil {
			return it.positions[:i]
		}
		it.positions[i] = prevPos + posDelta
		prevPos = it.positions[i]
	}

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
