package bkd

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"gosearch/store"
)

// point represents a single indexed point with a document ID and value.
type point struct {
	docID int
	value int64
}

// BKDWriter buffers points and builds a BKD tree, serializing it to a .kd file.
type BKDWriter struct {
	points []point
}

// NewBKDWriter creates a new BKDWriter.
func NewBKDWriter() *BKDWriter {
	return &BKDWriter{}
}

// Add buffers a point for later tree construction.
func (w *BKDWriter) Add(docID int, value int64) {
	w.points = append(w.points, point{docID: docID, value: value})
}

// Finish builds the BKD tree from buffered points and writes the .kdm and .kdd files.
func (w *BKDWriter) Finish(dir store.Directory, segName, field string) error {
	metaName := fmt.Sprintf("%s.%s.kdm", segName, field)
	dataName := fmt.Sprintf("%s.%s.kdd", segName, field)

	metaOut, err := dir.CreateOutput(metaName)
	if err != nil {
		return err
	}
	defer metaOut.Close()

	dataOut, err := dir.CreateOutput(dataName)
	if err != nil {
		return err
	}
	defer dataOut.Close()

	numPoints := len(w.points)

	// Empty dataset: write 32 bytes of zeros to .kdm, empty .kdd.
	if numPoints == 0 {
		if _, err := metaOut.Write(make([]byte, 32)); err != nil {
			return err
		}
		return nil
	}

	// Sort points by value, then docID.
	sort.Slice(w.points, func(i, j int) bool {
		if w.points[i].value != w.points[j].value {
			return w.points[i].value < w.points[j].value
		}
		return w.points[i].docID < w.points[j].docID
	})

	// Count distinct docs.
	docSet := make(map[int]struct{}, numPoints)
	for _, p := range w.points {
		docSet[p.docID] = struct{}{}
	}
	docCount := len(docSet)

	globalMinValue := w.points[0].value
	globalMaxValue := w.points[numPoints-1].value

	// Calculate number of leaves.
	numLeaves := nextPowerOf2(int(math.Ceil(float64(numPoints) / float64(MaxPointsInLeafNode))))
	numInnerNodes := numLeaves - 1

	// Build leaf buckets via recursive splitting.
	leaves := make([][]point, numLeaves)
	w.buildLeaves(w.points, 1, numInnerNodes, leaves)

	// Compute inner node metadata.
	innerNodes := make([]innerNode, numInnerNodes+1)
	w.computeInnerNodes(1, numInnerNodes, leaves, innerNodes)

	// --- Write .kdd (leaf data) ---
	buf := make([]byte, 8)
	leafOffsets := make([]uint64, numLeaves)
	leafCounts := make([]int, numLeaves)
	var offset uint64
	for i := range numLeaves {
		leafOffsets[i] = offset
		leafCounts[i] = len(leaves[i])
		for _, p := range leaves[i] {
			binary.LittleEndian.PutUint32(buf[:4], uint32(p.docID))
			if _, err := dataOut.Write(buf[:4]); err != nil {
				return err
			}
		}
		for _, p := range leaves[i] {
			binary.LittleEndian.PutUint64(buf, uint64(p.value))
			if _, err := dataOut.Write(buf); err != nil {
				return err
			}
		}
		n := len(leaves[i])
		offset += uint64(n)*4 + uint64(n)*8
	}

	// --- Write .kdm (header + inner nodes + leaf directory) ---

	// Header (32 bytes).
	if err := metaOut.WriteUint32(MaxPointsInLeafNode); err != nil {
		return err
	}
	if err := metaOut.WriteUint32(uint32(numLeaves)); err != nil {
		return err
	}
	if err := metaOut.WriteUint32(uint32(numPoints)); err != nil {
		return err
	}
	if err := metaOut.WriteUint32(uint32(docCount)); err != nil {
		return err
	}
	if err := metaOut.WriteUint64(uint64(globalMinValue)); err != nil {
		return err
	}
	if err := metaOut.WriteUint64(uint64(globalMaxValue)); err != nil {
		return err
	}

	// Inner nodes (12 bytes each, heap-ordered 1-indexed).
	for i := 1; i <= numInnerNodes; i++ {
		if err := metaOut.WriteUint64(uint64(innerNodes[i].splitValue)); err != nil {
			return err
		}
		if err := metaOut.WriteUint32(uint32(innerNodes[i].numPoints)); err != nil {
			return err
		}
	}

	// Leaf directory (12 bytes each: offset u64 + numPts u32).
	for i := range numLeaves {
		if err := metaOut.WriteUint64(leafOffsets[i]); err != nil {
			return err
		}
		if err := metaOut.WriteUint32(uint32(leafCounts[i])); err != nil {
			return err
		}
	}

	return nil
}

// buildLeaves recursively splits sorted points into leaf buckets using
// heap-ordered node IDs.
func (w *BKDWriter) buildLeaves(pts []point, nodeID, numInnerNodes int, leaves [][]point) {
	if nodeID > numInnerNodes {
		// Leaf node.
		leafIdx := nodeID - numInnerNodes - 1
		leaves[leafIdx] = pts
		return
	}
	// Split at midpoint.
	mid := len(pts) / 2
	w.buildLeaves(pts[:mid], nodeID*2, numInnerNodes, leaves)
	w.buildLeaves(pts[mid:], nodeID*2+1, numInnerNodes, leaves)
}

// computeInnerNodes recursively computes splitValue and numPoints for inner nodes.
// splitValue = max value in the left subtree. numPoints = total points in subtree.
func (w *BKDWriter) computeInnerNodes(nodeID, numInnerNodes int, leaves [][]point, innerNodes []innerNode) (maxVal int64, total int) {
	if nodeID > numInnerNodes {
		// Leaf node.
		leafIdx := nodeID - numInnerNodes - 1
		leaf := leaves[leafIdx]
		n := len(leaf)
		if n == 0 {
			return math.MinInt64, 0
		}
		return leaf[n-1].value, n
	}

	leftMax, leftCount := w.computeInnerNodes(nodeID*2, numInnerNodes, leaves, innerNodes)
	rightMax, rightCount := w.computeInnerNodes(nodeID*2+1, numInnerNodes, leaves, innerNodes)

	innerNodes[nodeID].splitValue = leftMax
	innerNodes[nodeID].numPoints = leftCount + rightCount

	if rightMax > leftMax {
		return rightMax, leftCount + rightCount
	}
	return leftMax, leftCount + rightCount
}

type innerNode struct {
	splitValue int64
	numPoints  int
}

// nextPowerOf2 returns the smallest power of 2 >= n. Returns 1 for n <= 1.
func nextPowerOf2(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p *= 2
	}
	return p
}
