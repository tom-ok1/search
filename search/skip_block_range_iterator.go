package search

import "gosearch/index"

// SkipBlockRangeIterator iterates over document IDs that belong to blocks
// whose value ranges overlap with a given [minValue, maxValue] range.
// It uses DocValuesSkipper to skip non-competitive blocks efficiently.
type SkipBlockRangeIterator struct {
	skipper  *index.DocValuesSkipper
	minValue int64
	maxValue int64
	doc      int
}

// NewSkipBlockRangeIterator creates a new iterator that only yields documents
// from blocks whose value ranges overlap with [minValue, maxValue].
func NewSkipBlockRangeIterator(skipper *index.DocValuesSkipper, minValue, maxValue int64) *SkipBlockRangeIterator {
	return &SkipBlockRangeIterator{
		skipper:  skipper,
		minValue: minValue,
		maxValue: maxValue,
		doc:      -1,
	}
}

func (it *SkipBlockRangeIterator) DocID() int { return it.doc }

func (it *SkipBlockRangeIterator) NextDoc() int {
	return it.Advance(it.doc + 1)
}

func (it *SkipBlockRangeIterator) Advance(target int) int {
	if target <= it.skipper.MaxDocID() {
		// Within current block — already validated as competitive
		if it.doc > -1 {
			it.doc = target
			return it.doc
		}
	} else {
		// Advance to the block containing target
		if !it.skipper.Advance(target) {
			it.doc = NoMoreDocs
			return NoMoreDocs
		}
	}

	// Skip blocks whose value ranges don't overlap
	if !it.skipper.AdvanceToValue(it.minValue, it.maxValue) {
		it.doc = NoMoreDocs
		return NoMoreDocs
	}

	it.doc = max(target, it.skipper.MinDocID())
	return it.doc
}

func (it *SkipBlockRangeIterator) Cost() int64 {
	return int64(NoMoreDocs)
}
