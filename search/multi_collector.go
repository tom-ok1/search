package search

import "gosearch/index"

// MultiCollector wraps multiple Collectors so they all receive documents
// in a single pass over the posting lists. This mirrors Lucene's
// MultiCollector which is used when both top-K and aggregation collectors
// need to run together.
type MultiCollector struct {
	collectors []Collector
}

// NewMultiCollector creates a collector that delegates to all given collectors.
// If only one collector is provided, it is returned directly.
func NewMultiCollector(collectors ...Collector) Collector {
	if len(collectors) == 1 {
		return collectors[0]
	}
	return &MultiCollector{collectors: collectors}
}

func (mc *MultiCollector) GetLeafCollector(ctx index.LeafReaderContext) LeafCollector {
	leafCollectors := make([]LeafCollector, len(mc.collectors))
	for i, c := range mc.collectors {
		leafCollectors[i] = c.GetLeafCollector(ctx)
	}
	return &multiLeafCollector{leafCollectors: leafCollectors}
}

func (mc *MultiCollector) ScoreMode() ScoreMode {
	for _, c := range mc.collectors {
		if c.ScoreMode() == ScoreModeComplete {
			return ScoreModeComplete
		}
	}
	return ScoreModeNone
}

// Results returns results from the first collector that has them.
func (mc *MultiCollector) Results() []SearchResult {
	for _, c := range mc.collectors {
		if r := c.Results(); len(r) > 0 {
			return r
		}
	}
	return nil
}

type multiLeafCollector struct {
	leafCollectors []LeafCollector
}

func (mlc *multiLeafCollector) SetScorer(scorer Scorable) {
	for _, lc := range mlc.leafCollectors {
		lc.SetScorer(scorer)
	}
}

func (mlc *multiLeafCollector) Collect(docID int) {
	for _, lc := range mlc.leafCollectors {
		lc.Collect(docID)
	}
}

// CompetitiveIterator returns nil because when multiple collectors are active,
// competitive skipping cannot be used — aggregations need all matching docs.
func (mlc *multiLeafCollector) CompetitiveIterator() DocIdSetIterator {
	return nil
}
