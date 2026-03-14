package search

// LeafCollector collects hits within a single segment.
type LeafCollector interface {
	Collect(docID int, score float64)
}
