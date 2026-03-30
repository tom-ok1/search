package aggregation

import (
	"sort"
	"sync"

	"gosearch/index"
)

// TermsAggregator builds buckets of unique values for keyword fields
// using SortedDocValues. Mirrors Elasticsearch's TermsAggregator.
type TermsAggregator struct {
	name   string
	field  string
	size   int
	mu     sync.Mutex
	counts map[string]int64
}

// NewTermsAggregator creates a new TermsAggregator for the given field.
// size limits how many buckets are returned (top-N by count).
func NewTermsAggregator(name, field string, size int) *TermsAggregator {
	return &TermsAggregator{
		name:   name,
		field:  field,
		size:   size,
		counts: make(map[string]int64),
	}
}

// Name returns the aggregation name.
func (a *TermsAggregator) Name() string { return a.name }

// GetLeafAggregator returns a LeafAggregator for the given segment context.
func (a *TermsAggregator) GetLeafAggregator(ctx index.LeafReaderContext) LeafAggregator {
	sdv := ctx.Segment.SortedDocValues(a.field)
	if sdv == nil {
		return &termsNoopLeaf{}
	}
	return &termsSortedLeaf{
		sdv:    sdv,
		counts: &a.counts,
		mu:     &a.mu,
	}
}

// BuildResult returns the aggregation result with buckets sorted by
// count descending, then key ascending (ES behavior), limited to size.
func (a *TermsAggregator) BuildResult() AggregationResult {
	buckets := make([]BucketResult, 0, len(a.counts))
	for key, count := range a.counts {
		buckets = append(buckets, BucketResult{
			Key:      key,
			DocCount: count,
		})
	}

	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].DocCount != buckets[j].DocCount {
			return buckets[i].DocCount > buckets[j].DocCount
		}
		return buckets[i].Key < buckets[j].Key
	})

	if a.size > 0 && len(buckets) > a.size {
		buckets = buckets[:a.size]
	}

	return AggregationResult{
		Name:    a.name,
		Type:    "terms",
		Buckets: buckets,
	}
}

// termsSortedLeaf collects term values from SortedDocValues.
type termsSortedLeaf struct {
	sdv    index.SortedDocValues
	counts *map[string]int64
	mu     *sync.Mutex
}

func (l *termsSortedLeaf) Collect(docID int) {
	ord, err := l.sdv.OrdValue(docID)
	if err != nil || ord < 0 {
		return
	}
	term, err := l.sdv.LookupOrd(ord)
	if err != nil {
		return
	}
	l.mu.Lock()
	(*l.counts)[string(term)]++
	l.mu.Unlock()
}

// termsNoopLeaf is used when no SortedDocValues exist for the field.
type termsNoopLeaf struct{}

func (l *termsNoopLeaf) Collect(docID int) {}
