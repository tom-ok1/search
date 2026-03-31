package aggregation

import (
	"container/heap"
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
	n := len(a.counts)
	if n == 0 {
		return AggregationResult{Name: a.name, Type: "terms"}
	}

	k := n
	if a.size > 0 && a.size < k {
		k = a.size
	}

	var buckets []BucketResult

	// Use a min-heap for top-K selection when k is significantly smaller
	// than n (mirrors ES's BucketPriorityQueue / ObjectArrayPriorityQueue).
	if k < n/2 {
		h := &bucketMinHeap{}
		heap.Init(h)
		for key, count := range a.counts {
			b := BucketResult{Key: key, DocCount: count}
			if h.Len() < k {
				heap.Push(h, b)
			} else if bucketGreater(b, (*h)[0]) {
				(*h)[0] = b
				heap.Fix(h, 0)
			}
		}
		// Pop from min-heap gives ascending order; reverse for descending.
		buckets = make([]BucketResult, h.Len())
		for i := len(buckets) - 1; i >= 0; i-- {
			buckets[i] = heap.Pop(h).(BucketResult)
		}
	} else {
		buckets = make([]BucketResult, 0, n)
		for key, count := range a.counts {
			buckets = append(buckets, BucketResult{Key: key, DocCount: count})
		}
		sort.Slice(buckets, func(i, j int) bool {
			return bucketGreater(buckets[i], buckets[j])
		})
		buckets = buckets[:k]
	}

	return AggregationResult{
		Name:    a.name,
		Type:    "terms",
		Buckets: buckets,
	}
}

// bucketGreater returns true if a should rank before b
// (higher count first, then lexicographically smaller key).
func bucketGreater(a, b BucketResult) bool {
	if a.DocCount != b.DocCount {
		return a.DocCount > b.DocCount
	}
	return a.Key < b.Key
}

// bucketMinHeap is a min-heap of BucketResult ordered so the least-competitive
// bucket (lowest count, highest key) is at index 0, matching ES's
// insertWithOverflow pattern.
type bucketMinHeap []BucketResult

func (h bucketMinHeap) Len() int      { return len(h) }
func (h bucketMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Less: the root should be the element we'd evict first — the one that
// ranks worst. That's the opposite of bucketGreater.
func (h bucketMinHeap) Less(i, j int) bool {
	return bucketGreater(h[j], h[i])
}

func (h *bucketMinHeap) Push(x any) { *h = append(*h, x.(BucketResult)) }
func (h *bucketMinHeap) Pop() any {
	old := *h
	n := len(old)
	v := old[n-1]
	*h = old[:n-1]
	return v
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
