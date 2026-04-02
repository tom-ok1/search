package search

import "gosearch/index"

// MultiCollector wraps a primary (top-K) collector and additional collectors
// (e.g. aggregations) so they all receive documents in a single pass over the
// posting lists. Results() always delegates to the primary collector.
// This mirrors Lucene's MultiCollector.
type MultiCollector struct {
	primary    Collector
	collectors []Collector // primary + others
}

// NewMultiCollector creates a collector that feeds documents to the primary
// collector and all additional collectors. Results() returns the primary
// collector's results.
func NewMultiCollector(primary Collector, others ...Collector) *MultiCollector {
	all := make([]Collector, 0, 1+len(others))
	all = append(all, primary)
	all = append(all, others...)
	return &MultiCollector{primary: primary, collectors: all}
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

// Results returns results from the primary (top-K) collector.
func (mc *MultiCollector) Results() []SearchResult {
	return mc.primary.Results()
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
