package search

import "gosearch/index"

// Query represents a search query.
type Query interface {
	// Execute runs the query and returns matching documents with scores.
	Execute(idx *index.InMemoryIndex) []DocScore
}

// DocScore is a pair of document ID and its relevance score.
type DocScore struct {
	DocID int
	Score float64
}
