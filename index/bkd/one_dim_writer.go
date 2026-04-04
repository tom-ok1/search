package bkd

import (
	"encoding/binary"
	"fmt"
	"math"

	"gosearch/store"
)

// leafBlock holds data for one completed leaf.
type leafBlock struct {
	points []point
}

// OneDimensionBKDWriter builds a BKD tree from a pre-sorted stream of points.
// Points must be added in ascending order of (value, docID).
// Memory usage is O(MaxPointsInLeafNode) for the active buffer plus metadata
// for completed leaves.
type OneDimensionBKDWriter struct {
	dir     store.Directory
	segName string
	field   string

	leafBuf []point     // current leaf buffer
	leaves  []leafBlock // completed leaves

	numPoints int
	docSet    map[int]struct{}
}

// NewOneDimensionBKDWriter creates a writer that accepts sorted points.
func NewOneDimensionBKDWriter(dir store.Directory, segName, field string) (*OneDimensionBKDWriter, error) {
	return &OneDimensionBKDWriter{
		dir:     dir,
		segName: segName,
		field:   field,
		leafBuf: make([]point, 0, MaxPointsInLeafNode),
		docSet:  make(map[int]struct{}),
	}, nil
}

// Add appends a point. Points must arrive in sorted order (value asc, docID asc).
func (w *OneDimensionBKDWriter) Add(docID int, value int64) error {
	w.leafBuf = append(w.leafBuf, point{docID: docID, value: value})
	w.docSet[docID] = struct{}{}
	w.numPoints++

	if len(w.leafBuf) == MaxPointsInLeafNode {
		w.flushLeaf()
	}
	return nil
}

// flushLeaf saves the current leaf buffer and resets it.
func (w *OneDimensionBKDWriter) flushLeaf() {
	saved := make([]point, len(w.leafBuf))
	copy(saved, w.leafBuf)
	w.leaves = append(w.leaves, leafBlock{points: saved})
	w.leafBuf = w.leafBuf[:0]
}

// Finish writes the .kd file. After calling Finish, the writer must not be reused.
func (w *OneDimensionBKDWriter) Finish() error {
	// Flush remaining points.
	if len(w.leafBuf) > 0 {
		w.flushLeaf()
	}

	fileName := fmt.Sprintf("%s.%s.kd", w.segName, w.field)
	out, err := w.dir.CreateOutput(fileName)
	if err != nil {
		return err
	}
	defer out.Close()

	numPoints := w.numPoints

	// Empty dataset: write 32 bytes of zeros.
	if numPoints == 0 {
		for range 32 {
			if _, err := out.Write([]byte{0}); err != nil {
				return err
			}
		}
		return nil
	}

	docCount := len(w.docSet)

	// Collect all leaf slices.
	allLeaves := make([][]point, len(w.leaves))
	for i, lb := range w.leaves {
		allLeaves[i] = lb.points
	}

	// Pad to power-of-2 number of leaves (required by the BKD format).
	numLeavesRaw := len(allLeaves)
	numLeaves := nextPowerOf2(numLeavesRaw)
	for len(allLeaves) < numLeaves {
		allLeaves = append(allLeaves, nil)
	}

	numInnerNodes := numLeaves - 1

	globalMinValue := w.leaves[0].points[0].value
	lastLeaf := w.leaves[numLeavesRaw-1].points
	globalMaxValue := lastLeaf[len(lastLeaf)-1].value

	// Compute inner nodes.
	innerNodes := make([]innerNode, numInnerNodes+1)
	computeInnerNodesFromLeaves(1, numInnerNodes, allLeaves, innerNodes)

	// --- Write .kd file ---

	// Header (32 bytes).
	if err := out.WriteUint32(MaxPointsInLeafNode); err != nil {
		return err
	}
	if err := out.WriteUint32(uint32(numLeaves)); err != nil {
		return err
	}
	if err := out.WriteUint32(uint32(numPoints)); err != nil {
		return err
	}
	if err := out.WriteUint32(uint32(docCount)); err != nil {
		return err
	}
	if err := out.WriteUint64(uint64(globalMinValue)); err != nil {
		return err
	}
	if err := out.WriteUint64(uint64(globalMaxValue)); err != nil {
		return err
	}

	// Inner nodes (12 bytes each, heap-ordered 1-indexed).
	for i := 1; i <= numInnerNodes; i++ {
		if err := out.WriteUint64(uint64(innerNodes[i].splitValue)); err != nil {
			return err
		}
		if err := out.WriteUint32(uint32(innerNodes[i].numPoints)); err != nil {
			return err
		}
	}

	// Compute leaf data offsets.
	leafOffsets := make([]uint64, numLeaves)
	var offset uint64
	for i := range numLeaves {
		leafOffsets[i] = offset
		n := len(allLeaves[i])
		offset += uint64(n)*4 + uint64(n)*8
	}

	// Leaf directory (28 bytes each).
	for i := range numLeaves {
		leaf := allLeaves[i]
		if err := out.WriteUint64(leafOffsets[i]); err != nil {
			return err
		}
		if err := out.WriteUint32(uint32(len(leaf))); err != nil {
			return err
		}
		var minVal, maxVal int64
		if len(leaf) > 0 {
			minVal = leaf[0].value
			maxVal = leaf[len(leaf)-1].value
		}
		if err := out.WriteUint64(uint64(minVal)); err != nil {
			return err
		}
		if err := out.WriteUint64(uint64(maxVal)); err != nil {
			return err
		}
	}

	// Leaf data.
	buf := make([]byte, 8)
	for i := range numLeaves {
		leaf := allLeaves[i]
		for _, p := range leaf {
			binary.LittleEndian.PutUint32(buf[:4], uint32(p.docID))
			if _, err := out.Write(buf[:4]); err != nil {
				return err
			}
		}
		for _, p := range leaf {
			binary.LittleEndian.PutUint64(buf, uint64(p.value))
			if _, err := out.Write(buf); err != nil {
				return err
			}
		}
	}

	return nil
}

// computeInnerNodesFromLeaves computes splitValue and numPoints for inner nodes.
func computeInnerNodesFromLeaves(nodeID, numInnerNodes int, leaves [][]point, innerNodes []innerNode) (maxVal int64, total int) {
	if nodeID > numInnerNodes {
		leafIdx := nodeID - numInnerNodes - 1
		leaf := leaves[leafIdx]
		n := len(leaf)
		if n == 0 {
			return math.MinInt64, 0
		}
		return leaf[n-1].value, n
	}

	leftMax, leftCount := computeInnerNodesFromLeaves(nodeID*2, numInnerNodes, leaves, innerNodes)
	rightMax, rightCount := computeInnerNodesFromLeaves(nodeID*2+1, numInnerNodes, leaves, innerNodes)

	innerNodes[nodeID].splitValue = leftMax
	innerNodes[nodeID].numPoints = leftCount + rightCount

	if rightMax > leftMax {
		return rightMax, leftCount + rightCount
	}
	return leftMax, leftCount + rightCount
}
