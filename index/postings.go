package index

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
