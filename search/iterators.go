package search

import "gosearch/index"

// EmptyDocIdSetIterator is an iterator that returns no documents.
type EmptyDocIdSetIterator struct{}

func (EmptyDocIdSetIterator) DocID() int             { return NoMoreDocs }
func (EmptyDocIdSetIterator) NextDoc() int           { return NoMoreDocs }
func (EmptyDocIdSetIterator) Advance(target int) int { return NoMoreDocs }
func (EmptyDocIdSetIterator) Cost() int64            { return 0 }

// PostingsDocIdSetIterator adapts a PostingsIterator to DocIdSetIterator.
type PostingsDocIdSetIterator struct {
	postings index.PostingsIterator
	docID    int
	cost     int64
}

// NewPostingsDocIdSetIterator creates a new PostingsDocIdSetIterator.
// cost is the estimated number of documents (typically DocFreq).
func NewPostingsDocIdSetIterator(postings index.PostingsIterator, cost int64) *PostingsDocIdSetIterator {
	return &PostingsDocIdSetIterator{
		postings: postings,
		docID:    -1,
		cost:     cost,
	}
}

func (it *PostingsDocIdSetIterator) DocID() int {
	return it.docID
}

func (it *PostingsDocIdSetIterator) NextDoc() int {
	if it.postings.Next() {
		it.docID = it.postings.DocID()
		return it.docID
	}
	it.docID = NoMoreDocs
	return NoMoreDocs
}

func (it *PostingsDocIdSetIterator) Advance(target int) int {
	for {
		doc := it.NextDoc()
		if doc == NoMoreDocs || doc >= target {
			return doc
		}
	}
}

func (it *PostingsDocIdSetIterator) Cost() int64 {
	return it.cost
}

// Freq returns the term frequency for the current document.
func (it *PostingsDocIdSetIterator) Freq() int {
	return it.postings.Freq()
}

// Positions returns the term positions for the current document.
func (it *PostingsDocIdSetIterator) Positions() []int {
	return it.postings.Positions()
}
