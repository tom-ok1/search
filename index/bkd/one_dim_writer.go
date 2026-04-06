package bkd

import (
	"fmt"
	"math"

	"gosearch/store"
)

// leafMeta holds lightweight metadata for a completed leaf.
// The actual leaf data lives in the .kdd file on disk.
type leafMeta struct {
	offset   uint64 // byte offset within .kdd file
	numPts   int
	minValue int64
	maxValue int64
}

// OneDimensionBKDWriter builds a BKD tree from a pre-sorted stream of points.
// Points must be added in ascending order of (value, docID).
// Memory usage is O(MaxPointsInLeafNode) for the active buffer plus O(numLeaves)
// for leaf metadata. Leaf data is written directly to the .kdd file.
type OneDimensionBKDWriter struct {
	dir     store.Directory
	segName string
	field   string

	leafBuf []point    // current leaf buffer — at most MaxPointsInLeafNode
	metas   []leafMeta // completed leaf metadata

	dataOut   store.IndexOutput // .kdd file output
	dataName  string            // .kdd file name within dir
	dataBytes uint64            // bytes written to .kdd so far

	numPoints int
	docSet    map[int]struct{}
}

// NewOneDimensionBKDWriter creates a writer that accepts sorted points.
func NewOneDimensionBKDWriter(dir store.Directory, segName, field string) (*OneDimensionBKDWriter, error) {
	dataName := fmt.Sprintf("%s.%s.kdd", segName, field)
	dataOut, err := dir.CreateOutput(dataName)
	if err != nil {
		return nil, fmt.Errorf("bkd: create data file %s: %w", dataName, err)
	}
	return &OneDimensionBKDWriter{
		dir:      dir,
		segName:  segName,
		field:    field,
		leafBuf:  make([]point, 0, MaxPointsInLeafNode),
		dataOut:  dataOut,
		dataName: dataName,
		docSet:   make(map[int]struct{}),
	}, nil
}

// Abort cleans up the .kdd file without writing the .kdm file.
// Safe to call multiple times or after Finish.
func (w *OneDimensionBKDWriter) Abort() {
	if w.dataOut != nil {
		w.dataOut.Close()
		w.dataOut = nil
	}
	if w.dataName != "" {
		w.dir.DeleteFile(w.dataName)
		w.dataName = ""
	}
}

// Add appends a point. Points must arrive in sorted order (value asc, docID asc).
func (w *OneDimensionBKDWriter) Add(docID int, value int64) error {
	w.leafBuf = append(w.leafBuf, point{docID: docID, value: value})
	w.docSet[docID] = struct{}{}
	w.numPoints++

	if len(w.leafBuf) == MaxPointsInLeafNode {
		return w.flushLeaf()
	}
	return nil
}

// flushLeaf serializes the current leaf buffer to the .kdd file and records metadata.
func (w *OneDimensionBKDWriter) flushLeaf() error {
	leaf := w.leafBuf
	n := len(leaf)

	w.metas = append(w.metas, leafMeta{
		offset:   w.dataBytes,
		numPts:   n,
		minValue: leaf[0].value,
		maxValue: leaf[n-1].value,
	})

	for _, p := range leaf {
		if err := w.dataOut.WriteUint32(uint32(p.docID)); err != nil {
			return err
		}
	}
	for _, p := range leaf {
		if err := w.dataOut.WriteUint64(uint64(p.value)); err != nil {
			return err
		}
	}

	w.dataBytes += uint64(n)*4 + uint64(n)*8
	w.leafBuf = w.leafBuf[:0]
	return nil
}

// Finish writes the .kdm file. After calling Finish, the writer must not be reused.
func (w *OneDimensionBKDWriter) Finish() error {
	// Flush remaining points.
	if len(w.leafBuf) > 0 {
		if err := w.flushLeaf(); err != nil {
			return err
		}
	}

	// Close .kdd file.
	if err := w.dataOut.Close(); err != nil {
		return err
	}
	w.dataOut = nil

	metaName := fmt.Sprintf("%s.%s.kdm", w.segName, w.field)
	metaOut, err := w.dir.CreateOutput(metaName)
	if err != nil {
		return err
	}
	defer metaOut.Close()

	numPoints := w.numPoints

	// Empty dataset: write 32 bytes of zeros to .kdm.
	if numPoints == 0 {
		if _, err := metaOut.Write(make([]byte, 32)); err != nil {
			return err
		}
		return nil
	}

	docCount := len(w.docSet)

	// Pad to power-of-2 number of leaves (required by the BKD format).
	numLeavesRaw := len(w.metas)
	numLeaves := nextPowerOf2(numLeavesRaw)

	// Pad metas with empty entries for the power-of-2 padding.
	allMetas := make([]leafMeta, numLeaves)
	copy(allMetas, w.metas)

	numInnerNodes := numLeaves - 1

	globalMinValue := w.metas[0].minValue
	globalMaxValue := w.metas[numLeavesRaw-1].maxValue

	// Compute inner nodes from metadata only.
	innerNodes := make([]innerNode, numInnerNodes+1)
	computeInnerNodesFromMetas(1, numInnerNodes, allMetas, innerNodes)

	// --- Write .kdm file ---

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

	// Leaf directory (12 bytes each).
	for i := range numLeaves {
		m := allMetas[i]
		if err := metaOut.WriteUint64(m.offset); err != nil {
			return err
		}
		if err := metaOut.WriteUint32(uint32(m.numPts)); err != nil {
			return err
		}
	}

	return nil
}

// computeInnerNodesFromMetas computes splitValue and numPoints for inner nodes
// using only leaf metadata (no point data needed).
func computeInnerNodesFromMetas(nodeID, numInnerNodes int, metas []leafMeta, innerNodes []innerNode) (maxVal int64, total int) {
	if nodeID > numInnerNodes {
		leafIdx := nodeID - numInnerNodes - 1
		if leafIdx >= len(metas) || metas[leafIdx].numPts == 0 {
			return math.MinInt64, 0
		}
		return metas[leafIdx].maxValue, metas[leafIdx].numPts
	}

	leftMax, leftCount := computeInnerNodesFromMetas(nodeID*2, numInnerNodes, metas, innerNodes)
	rightMax, rightCount := computeInnerNodesFromMetas(nodeID*2+1, numInnerNodes, metas, innerNodes)

	innerNodes[nodeID].splitValue = leftMax
	innerNodes[nodeID].numPoints = leftCount + rightCount

	if rightMax > leftMax {
		return rightMax, leftCount + rightCount
	}
	return leftMax, leftCount + rightCount
}
