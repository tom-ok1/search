package search

// CollectionStatistics holds collection-level statistics for a field across all segments.
type CollectionStatistics struct {
	Field            string
	MaxDoc           int64
	DocCount         int64
	SumTotalTermFreq int64
	SumDocFreq       int64
}

// TermStatistics holds term-specific statistics aggregated across all segments.
type TermStatistics struct {
	Term          string
	DocFreq       int64
	TotalTermFreq int64
}
