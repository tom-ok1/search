package aggregation

import (
	"gosearch/index"
	"gosearch/search"
)

// Collector adapts a slice of Aggregators into a search.Collector so that
// aggregation collection runs in the same document-iteration pass as top-K
// scoring. This mirrors Elasticsearch's BucketCollector integration with
// the search pipeline.
type Collector struct {
	aggs []Aggregator
}

// NewCollector creates a search.Collector that delegates to the given aggregators.
func NewCollector(aggs []Aggregator) *Collector {
	return &Collector{aggs: aggs}
}

func (c *Collector) GetLeafCollector(ctx index.LeafReaderContext) search.LeafCollector {
	leafAggs := make([]LeafAggregator, len(c.aggs))
	for i, agg := range c.aggs {
		leafAggs[i] = agg.GetLeafAggregator(ctx)
	}
	return &aggLeafCollector{leafAggs: leafAggs}
}

func (c *Collector) ScoreMode() search.ScoreMode {
	return search.ScoreModeNone
}

func (c *Collector) Results() []search.SearchResult {
	return nil
}

type aggLeafCollector struct {
	leafAggs []LeafAggregator
}

func (alc *aggLeafCollector) SetScorer(scorer search.Scorable) {}

func (alc *aggLeafCollector) Collect(docID int) {
	for _, la := range alc.leafAggs {
		la.Collect(docID)
	}
}

func (alc *aggLeafCollector) CompetitiveIterator() search.DocIdSetIterator {
	return nil
}
