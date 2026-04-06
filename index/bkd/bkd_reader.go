package bkd

import (
	"fmt"

	"gosearch/store"
)

// leafDirEntry describes a single leaf in the BKD tree's on-disk layout.
type leafDirEntry struct {
	offset uint64 // byte offset into .kdd file
	numPts int
}

// BKDReader reads a .kdm/.kdd file pair produced by BKDWriter and provides a PointTree
// implementation for tree navigation.
type BKDReader struct {
	meta          *store.MMapIndexInput // .kdm file
	data          *store.MMapIndexInput // .kdd file
	numLeaves     int
	numPoints     int
	docCount      int
	numInnerNodes int
	globalMin     int64
	globalMax     int64
	innerNodes    []innerNode    // 1-indexed, size numInnerNodes+1
	leafDir       []leafDirEntry // size numLeaves
}

// OpenBKDReaderFromPath opens a BKD index from a directory path string.
func OpenBKDReaderFromPath(dirPath, segName, field string) (*BKDReader, error) {
	dir, err := store.NewFSDirectory(dirPath)
	if err != nil {
		return nil, fmt.Errorf("bkd: open directory %s: %w", dirPath, err)
	}
	return OpenBKDReader(dir, segName, field)
}

// OpenBKDReader opens a BKD index from a directory.
func OpenBKDReader(dir store.Directory, segName, field string) (*BKDReader, error) {
	metaName := fmt.Sprintf("%s.%s.kdm", segName, field)
	dataName := fmt.Sprintf("%s.%s.kdd", segName, field)

	meta, err := store.OpenMMap(dir.FilePath(metaName))
	if err != nil {
		return nil, fmt.Errorf("bkd: open %s: %w", metaName, err)
	}

	data, err := store.OpenMMap(dir.FilePath(dataName))
	if err != nil {
		meta.Close()
		return nil, fmt.Errorf("bkd: open %s: %w", dataName, err)
	}

	r := &BKDReader{meta: meta, data: data}

	// Read header (32 bytes) from .kdm.
	meta.Seek(0)

	_, err = meta.ReadUint32() // maxPointsInLeaf (unused for reading)
	if err != nil {
		r.Close()
		return nil, err
	}

	numLeaves, err := meta.ReadUint32()
	if err != nil {
		r.Close()
		return nil, err
	}
	r.numLeaves = int(numLeaves)

	numPoints, err := meta.ReadUint32()
	if err != nil {
		r.Close()
		return nil, err
	}
	r.numPoints = int(numPoints)

	docCount, err := meta.ReadUint32()
	if err != nil {
		r.Close()
		return nil, err
	}
	r.docCount = int(docCount)

	globalMin, err := meta.ReadUint64()
	if err != nil {
		r.Close()
		return nil, err
	}
	r.globalMin = int64(globalMin)

	globalMax, err := meta.ReadUint64()
	if err != nil {
		r.Close()
		return nil, err
	}
	r.globalMax = int64(globalMax)

	r.numInnerNodes = max(r.numLeaves-1, 0)

	// Read inner nodes (12 bytes each, 1-indexed) from .kdm.
	r.innerNodes = make([]innerNode, r.numInnerNodes+1)
	for i := 1; i <= r.numInnerNodes; i++ {
		sv, err := meta.ReadUint64()
		if err != nil {
			r.Close()
			return nil, err
		}
		np, err := meta.ReadUint32()
		if err != nil {
			r.Close()
			return nil, err
		}
		r.innerNodes[i] = innerNode{splitValue: int64(sv), numPoints: int(np)}
	}

	// Read leaf directory (12 bytes each) from .kdm.
	r.leafDir = make([]leafDirEntry, r.numLeaves)
	for i := range r.numLeaves {
		offset, err := meta.ReadUint64()
		if err != nil {
			r.Close()
			return nil, err
		}
		numPts, err := meta.ReadUint32()
		if err != nil {
			r.Close()
			return nil, err
		}
		r.leafDir[i] = leafDirEntry{
			offset: offset,
			numPts: int(numPts),
		}
	}

	return r, nil
}

// NumPoints returns the total number of indexed points.
func (r *BKDReader) NumPoints() int { return r.numPoints }

// DocCount returns the number of distinct documents.
func (r *BKDReader) DocCount() int { return r.docCount }

// MinValue returns the global minimum value.
func (r *BKDReader) MinValue() int64 { return r.globalMin }

// MaxValue returns the global maximum value.
func (r *BKDReader) MaxValue() int64 { return r.globalMax }

// PointTree returns a PointTree cursor positioned at the root of the BKD tree.
func (r *BKDReader) PointTree() PointTree {
	if r.numLeaves == 0 {
		return &emptyPointTree{}
	}
	return &bkdPointTree{
		reader:   r,
		nodeID:   1,
		level:    0,
		minValue: r.globalMin,
		maxValue: r.globalMax,
	}
}

// Close releases the memory-mapped files.
func (r *BKDReader) Close() error {
	err1 := r.meta.Close()
	err2 := r.data.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// bkdPointTree implements PointTree for navigating a BKD tree.
type bkdPointTree struct {
	reader      *BKDReader
	nodeID      int // 1-based heap index
	level       int
	nodeStack   []int // parent nodeIDs for MoveToParent
	minValue    int64
	maxValue    int64
	boundsStack []boundsEntry // saved min/max for MoveToParent / MoveToSibling
}

type boundsEntry struct {
	minValue int64
	maxValue int64
}

func (t *bkdPointTree) isLeaf() bool {
	return t.nodeID > t.reader.numInnerNodes
}

func (t *bkdPointTree) leafIndex() int {
	return t.nodeID - t.reader.numInnerNodes - 1
}

// MoveToChild moves to the left child. Returns false if at a leaf.
func (t *bkdPointTree) MoveToChild() bool {
	if t.isLeaf() {
		return false
	}
	t.nodeStack = append(t.nodeStack, t.nodeID)
	t.boundsStack = append(t.boundsStack, boundsEntry{minValue: t.minValue, maxValue: t.maxValue})
	// Left child: max narrows to parent's splitValue.
	t.maxValue = t.reader.innerNodes[t.nodeID].splitValue
	t.nodeID = t.nodeID * 2
	t.level++
	return true
}

// MoveToSibling moves to the right sibling. Returns false if already a right child.
func (t *bkdPointTree) MoveToSibling() bool {
	if t.nodeID%2 == 1 {
		// Already a right child (odd nodeID).
		return false
	}
	parentNodeID := t.nodeStack[len(t.nodeStack)-1]
	saved := t.boundsStack[len(t.boundsStack)-1]
	// Right child: min narrows to parent's splitValue, max restores to parent's original.
	t.minValue = t.reader.innerNodes[parentNodeID].splitValue
	t.maxValue = saved.maxValue
	t.nodeID++
	return true
}

// MoveToParent moves to the parent. Returns false if at root.
func (t *bkdPointTree) MoveToParent() bool {
	if len(t.nodeStack) == 0 {
		return false
	}
	saved := t.boundsStack[len(t.boundsStack)-1]
	t.boundsStack = t.boundsStack[:len(t.boundsStack)-1]
	t.minValue = saved.minValue
	t.maxValue = saved.maxValue
	t.nodeID = t.nodeStack[len(t.nodeStack)-1]
	t.nodeStack = t.nodeStack[:len(t.nodeStack)-1]
	t.level--
	return true
}

// MinValue returns the minimum value in the current cell (O(1)).
func (t *bkdPointTree) MinValue() int64 {
	return t.minValue
}

// MaxValue returns the maximum value in the current cell (O(1)).
func (t *bkdPointTree) MaxValue() int64 {
	return t.maxValue
}

// Size returns the number of points in the current cell.
func (t *bkdPointTree) Size() int {
	if t.isLeaf() {
		return t.reader.leafDir[t.leafIndex()].numPts
	}
	return t.reader.innerNodes[t.nodeID].numPoints
}

// VisitDocIDs visits all document IDs in the current cell without values.
func (t *bkdPointTree) VisitDocIDs(v IntersectVisitor) {
	if t.isLeaf() {
		t.visitLeafDocIDs(t.leafIndex(), v)
		return
	}
	// Recurse into all leaves in subtree.
	t.visitSubtreeDocIDs(t.nodeID, v)
}

func (t *bkdPointTree) visitSubtreeDocIDs(nodeID int, v IntersectVisitor) {
	// if it's a leaf
	if nodeID > t.reader.numInnerNodes {
		leafIdx := nodeID - t.reader.numInnerNodes - 1
		t.visitLeafDocIDs(leafIdx, v)
		return
	}
	t.visitSubtreeDocIDs(nodeID*2, v)
	t.visitSubtreeDocIDs(nodeID*2+1, v)
}

func (t *bkdPointTree) visitLeafDocIDs(leafIdx int, v IntersectVisitor) {
	leaf := t.reader.leafDir[leafIdx]
	baseOffset := int(leaf.offset)
	for j := range leaf.numPts {
		docID, _ := t.reader.data.ReadUint32At(baseOffset + j*4)
		v.Visit(int(docID))
	}
}

// VisitDocValues visits all document ID/value pairs in the current cell.
func (t *bkdPointTree) VisitDocValues(v IntersectVisitor) {
	if t.isLeaf() {
		t.visitLeafDocValues(t.leafIndex(), v)
		return
	}
	// For inner nodes, recurse into all leaves.
	t.visitSubtreeDocValues(t.nodeID, v)
}

func (t *bkdPointTree) visitSubtreeDocValues(nodeID int, v IntersectVisitor) {
	if nodeID > t.reader.numInnerNodes {
		leafIdx := nodeID - t.reader.numInnerNodes - 1
		t.visitLeafDocValues(leafIdx, v)
		return
	}
	t.visitSubtreeDocValues(nodeID*2, v)
	t.visitSubtreeDocValues(nodeID*2+1, v)
}

func (t *bkdPointTree) visitLeafDocValues(leafIdx int, v IntersectVisitor) {
	leaf := t.reader.leafDir[leafIdx]
	baseOffset := int(leaf.offset)
	for j := range leaf.numPts {
		docID, _ := t.reader.data.ReadUint32At(baseOffset + j*4)
		value, _ := t.reader.data.ReadInt64At(baseOffset + leaf.numPts*4 + j*8)
		v.VisitValue(int(docID), value)
	}
}

// emptyPointTree is a PointTree for fields with zero points.
type emptyPointTree struct{}

func (e *emptyPointTree) MoveToChild() bool                 { return false }
func (e *emptyPointTree) MoveToSibling() bool               { return false }
func (e *emptyPointTree) MoveToParent() bool                { return false }
func (e *emptyPointTree) MinValue() int64                   { return 0 }
func (e *emptyPointTree) MaxValue() int64                   { return 0 }
func (e *emptyPointTree) Size() int                         { return 0 }
func (e *emptyPointTree) VisitDocIDs(v IntersectVisitor)    {}
func (e *emptyPointTree) VisitDocValues(v IntersectVisitor) {}
