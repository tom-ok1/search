package search

import "gosearch/index"

// ScoreMode indicates what scoring information the collector needs.
type ScoreMode int

const (
	ScoreModeComplete ScoreMode = iota // needs accurate scores
	ScoreModeNone                      // does not need scores
)

// Collector produces a LeafCollector for each segment and aggregates results.
type Collector interface {
	GetLeafCollector(ctx index.LeafReaderContext) LeafCollector
	ScoreMode() ScoreMode
	Results() []SearchResult
}

// LeafCollector collects hits within a single segment.
type LeafCollector interface {
	Collect(docID int, score float64)
}
