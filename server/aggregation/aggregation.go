package aggregation

import "gosearch/index"

// AggregationResult represents the result of an aggregation.
type AggregationResult struct {
	Name    string
	Type    string
	Value   any            // for metric aggregations (e.g., value_count)
	Buckets []BucketResult // for bucket aggregations (e.g., terms)
}

// BucketResult represents a single bucket in a bucket aggregation.
type BucketResult struct {
	Key      string
	DocCount int64
}

// Aggregator computes aggregated values over matched documents.
// Mirrors Elasticsearch's Aggregator interface.
type Aggregator interface {
	Name() string
	GetLeafAggregator(ctx index.LeafReaderContext) LeafAggregator
	BuildResult() AggregationResult
}

// LeafAggregator collects values from a single segment.
// Mirrors Elasticsearch's LeafBucketCollector.
type LeafAggregator interface {
	Collect(docID int)
}
