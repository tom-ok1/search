package aggregation

import (
	"sync/atomic"

	"gosearch/index"
)

// ValueCountAggregator counts documents that have a value for a given field.
// It checks SortedDocValues first (keyword/boolean fields), then NumericDocValues.
type ValueCountAggregator struct {
	name  string
	field string
	count atomic.Int64
}

// NewValueCountAggregator creates a new ValueCountAggregator for the given field.
func NewValueCountAggregator(name, field string) *ValueCountAggregator {
	return &ValueCountAggregator{
		name:  name,
		field: field,
	}
}

// Name returns the aggregation name.
func (a *ValueCountAggregator) Name() string { return a.name }

// GetLeafAggregator returns a LeafAggregator for the given segment context.
func (a *ValueCountAggregator) GetLeafAggregator(ctx index.LeafReaderContext) LeafAggregator {
	sdv := ctx.Segment.SortedDocValues(a.field)
	if sdv != nil {
		return &valueCountSortedLeaf{
			sdv:   sdv,
			count: &a.count,
		}
	}
	ndv := ctx.Segment.NumericDocValues(a.field)
	if ndv != nil {
		return &valueCountNumericLeaf{
			ndv:   ndv,
			count: &a.count,
		}
	}
	// No doc values for this field; collect is a no-op.
	return &valueCountNoopLeaf{}
}

// BuildResult returns the aggregation result with the accumulated count.
func (a *ValueCountAggregator) BuildResult() AggregationResult {
	return AggregationResult{
		Name:  a.name,
		Type:  "value_count",
		Value: a.count.Load(),
	}
}

// valueCountSortedLeaf collects from SortedDocValues.
type valueCountSortedLeaf struct {
	sdv   index.SortedDocValues
	count *atomic.Int64
}

func (l *valueCountSortedLeaf) Collect(docID int) {
	ord, err := l.sdv.OrdValue(docID)
	if err == nil && ord >= 0 {
		l.count.Add(1)
	}
}

// valueCountNumericLeaf collects from NumericDocValues.
type valueCountNumericLeaf struct {
	ndv   index.NumericDocValues
	count *atomic.Int64
}

func (l *valueCountNumericLeaf) Collect(docID int) {
	_, err := l.ndv.Get(docID)
	if err == nil {
		l.count.Add(1)
	}
}

// valueCountNoopLeaf is used when no doc values exist for the field.
type valueCountNoopLeaf struct{}

func (l *valueCountNoopLeaf) Collect(docID int) {}
