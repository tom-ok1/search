package bkd

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"gosearch/store"
)

// leafMeta holds lightweight metadata for a completed leaf.
// The actual leaf data lives in a temp file on disk.
type leafMeta struct {
	offset   uint64 // byte offset within the temp file
	numPts   int
	minValue int64
	maxValue int64
}

// OneDimensionBKDWriter builds a BKD tree from a pre-sorted stream of points.
// Points must be added in ascending order of (value, docID).
// Memory usage is O(MaxPointsInLeafNode) for the active buffer plus O(numLeaves)
// for leaf metadata. Leaf data is flushed to a temporary file on disk.
type OneDimensionBKDWriter struct {
	dir     store.Directory
	segName string
	field   string

	leafBuf []point    // current leaf buffer — at most MaxPointsInLeafNode
	metas   []leafMeta // completed leaf metadata

	tmpOut   store.IndexOutput // temp file for leaf data
	tmpName  string            // temp file name within dir
	tmpBytes uint64            // bytes written to temp file so far

	numPoints int
	docSet    map[int]struct{}
}

// NewOneDimensionBKDWriter creates a writer that accepts sorted points.
func NewOneDimensionBKDWriter(dir store.Directory, segName, field string) (*OneDimensionBKDWriter, error) {
	tmpName := fmt.Sprintf("%s.%s.kd.tmp", segName, field)
	tmpOut, err := dir.CreateOutput(tmpName)
	if err != nil {
		return nil, fmt.Errorf("bkd: create temp file %s: %w", tmpName, err)
	}
	return &OneDimensionBKDWriter{
		dir:     dir,
		segName: segName,
		field:   field,
		leafBuf: make([]point, 0, MaxPointsInLeafNode),
		tmpOut:  tmpOut,
		tmpName: tmpName,
		docSet:  make(map[int]struct{}),
	}, nil
}

// Abort cleans up the temp file without writing the .kd file.
// Safe to call multiple times or after Finish.
func (w *OneDimensionBKDWriter) Abort() {
	if w.tmpOut != nil {
		w.tmpOut.Close()
		w.tmpOut = nil
	}
	if w.tmpName != "" {
		w.dir.DeleteFile(w.tmpName)
		w.tmpName = ""
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

// flushLeaf serializes the current leaf buffer to the temp file and records metadata.
func (w *OneDimensionBKDWriter) flushLeaf() error {
	leaf := w.leafBuf
	n := len(leaf)

	w.metas = append(w.metas, leafMeta{
		offset:   w.tmpBytes,
		numPts:   n,
		minValue: leaf[0].value,
		maxValue: leaf[n-1].value,
	})

	buf := make([]byte, 8)
	// Write docIDs (4 bytes each).
	for _, p := range leaf {
		binary.LittleEndian.PutUint32(buf[:4], uint32(p.docID))
		if _, err := w.tmpOut.Write(buf[:4]); err != nil {
			return err
		}
	}
	// Write values (8 bytes each).
	for _, p := range leaf {
		binary.LittleEndian.PutUint64(buf, uint64(p.value))
		if _, err := w.tmpOut.Write(buf); err != nil {
			return err
		}
	}

	w.tmpBytes += uint64(n)*4 + uint64(n)*8
	w.leafBuf = w.leafBuf[:0]
	return nil
}

// Finish writes the .kd file. After calling Finish, the writer must not be reused.
func (w *OneDimensionBKDWriter) Finish() error {
	// Flush remaining points.
	if len(w.leafBuf) > 0 {
		if err := w.flushLeaf(); err != nil {
			return err
		}
	}

	// Close temp file so we can read it back.
	if err := w.tmpOut.Close(); err != nil {
		return err
	}
	w.tmpOut = nil

	fileName := fmt.Sprintf("%s.%s.kd", w.segName, w.field)
	out, err := w.dir.CreateOutput(fileName)
	if err != nil {
		return err
	}
	defer out.Close()

	numPoints := w.numPoints

	// Empty dataset: write 32 bytes of zeros.
	if numPoints == 0 {
		if _, err := out.Write(make([]byte, 32)); err != nil {
			return err
		}
		if err := w.dir.DeleteFile(w.tmpName); err != nil {
			return fmt.Errorf("bkd: delete temp file: %w", err)
		}
		w.tmpName = ""
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

	// Leaf directory (28 bytes each).
	// Offsets in the directory are relative to the start of leaf data in the .kd file,
	// which matches the temp file offsets since the temp file contains only leaf data.
	for i := range numLeaves {
		m := allMetas[i]
		if err := out.WriteUint64(m.offset); err != nil {
			return err
		}
		if err := out.WriteUint32(uint32(m.numPts)); err != nil {
			return err
		}
		if err := out.WriteUint64(uint64(m.minValue)); err != nil {
			return err
		}
		if err := out.WriteUint64(uint64(m.maxValue)); err != nil {
			return err
		}
	}

	// Copy leaf data from temp file to .kd file.
	tmpIn, err := w.dir.OpenInput(w.tmpName)
	if err != nil {
		return fmt.Errorf("bkd: reopen temp file: %w", err)
	}
	defer tmpIn.Close()

	copyBuf := make([]byte, 64*1024) // 64 KB copy buffer
	if _, err := io.CopyBuffer(out, tmpIn, copyBuf); err != nil {
		return fmt.Errorf("bkd: copy leaf data: %w", err)
	}

	// Clean up temp file.
	if err := w.dir.DeleteFile(w.tmpName); err != nil {
		return fmt.Errorf("bkd: delete temp file: %w", err)
	}
	w.tmpName = ""

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
