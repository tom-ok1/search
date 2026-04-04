package bkd

// mergeCollector implements IntersectVisitor to collect all (docID, value) pairs
// from a single leaf. It buffers one leaf's worth of points.
type mergeCollector struct {
	docIDs []int
	values []int64
	count  int
}

func (c *mergeCollector) reset() {
	c.count = 0
}

func (c *mergeCollector) Visit(docID int) {
	// Not used during merge — we always need values.
}

func (c *mergeCollector) VisitValue(docID int, value int64) {
	if c.count >= len(c.docIDs) {
		c.docIDs = append(c.docIDs, docID)
		c.values = append(c.values, value)
	} else {
		c.docIDs[c.count] = docID
		c.values[c.count] = value
	}
	c.count++
}

func (c *mergeCollector) Compare(minValue, maxValue int64) Relation {
	return CellCrossesQuery // Accept all points.
}

// MergeReader iterates over all points in a BKD tree leaf-by-leaf in sorted
// order. It buffers only one leaf at a time, keeping memory usage at O(MaxPointsInLeafNode).
type MergeReader struct {
	tree      PointTree
	collector mergeCollector
	blockPos  int // position within current leaf's collected data
	docID     int
	value     int64
	exhausted bool
}

// NewMergeReader creates a MergeReader positioned before the first point.
// Call Next() to advance to the first point.
func NewMergeReader(tree PointTree) (*MergeReader, error) {
	mr := &MergeReader{
		tree: tree,
		collector: mergeCollector{
			docIDs: make([]int, 0, MaxPointsInLeafNode),
			values: make([]int64, 0, MaxPointsInLeafNode),
		},
	}
	// Navigate to the leftmost (first) leaf.
	moved := false
	for tree.MoveToChild() {
		moved = true
	}
	if !moved && tree.Size() == 0 {
		mr.exhausted = true
		return mr, nil
	}
	// Collect the first leaf.
	tree.VisitDocValues(&mr.collector)
	mr.blockPos = 0
	return mr, nil
}

// Next advances to the next point. Returns false when exhausted.
func (mr *MergeReader) Next() bool {
	if mr.exhausted {
		return false
	}
	for {
		if mr.blockPos < mr.collector.count {
			mr.docID = mr.collector.docIDs[mr.blockPos]
			mr.value = mr.collector.values[mr.blockPos]
			mr.blockPos++
			return true
		}
		// Current leaf exhausted — move to next leaf.
		if !mr.collectNextLeaf() {
			mr.exhausted = true
			return false
		}
		mr.blockPos = 0
	}
}

// collectNextLeaf navigates the PointTree to the next leaf and collects its data.
func (mr *MergeReader) collectNextLeaf() bool {
	mr.collector.reset()
	for {
		if mr.tree.MoveToSibling() {
			// Descend to leftmost leaf of sibling subtree.
			for mr.tree.MoveToChild() {
			}
			mr.tree.VisitDocValues(&mr.collector)
			return true
		}
		if !mr.tree.MoveToParent() {
			return false
		}
	}
}

// DocID returns the current point's document ID.
func (mr *MergeReader) DocID() int { return mr.docID }

// Value returns the current point's value.
func (mr *MergeReader) Value() int64 { return mr.value }
