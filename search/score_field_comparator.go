package search

import (
	"cmp"

	"gosearch/index"
)

// ScoreFieldComparator sorts by BM25 score.
// It implements both FieldComparator and LeafFieldComparator because
// score has no segment-specific state (analogous to Lucene's RelevanceComparator).
type ScoreFieldComparator struct {
	scores       []float64
	bottom       float64
	currentScore float64
}

func NewScoreFieldComparator(numSlots int) *ScoreFieldComparator {
	return &ScoreFieldComparator{
		scores: make([]float64, numSlots),
	}
}

func (c *ScoreFieldComparator) CompareSlots(slot1, slot2 int) int {
	return cmp.Compare(c.scores[slot1], c.scores[slot2])
}

func (c *ScoreFieldComparator) Value(slot int) any {
	return c.scores[slot]
}

func (c *ScoreFieldComparator) GetLeafComparator(seg index.SegmentReader) LeafFieldComparator {
	return c
}

// LeafFieldComparator implementation

func (c *ScoreFieldComparator) SetBottom(slot int) {
	c.bottom = c.scores[slot]
}

func (c *ScoreFieldComparator) CompareBottom(docID int) int {
	return cmp.Compare(c.currentScore, c.bottom)
}

func (c *ScoreFieldComparator) Copy(slot int, docID int) {
	c.scores[slot] = c.currentScore
}

func (c *ScoreFieldComparator) SetScorer(score float64) {
	c.currentScore = score
}
