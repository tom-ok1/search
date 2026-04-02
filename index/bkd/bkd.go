// Package bkd implements a BKD tree for indexing numeric point values.
package bkd

// MaxPointsInLeafNode is the maximum number of points stored in a single leaf node.
const MaxPointsInLeafNode = 512

// Relation describes the spatial relationship between a cell and a query range.
type Relation int

const (
	// CellOutsideQuery means the cell is entirely outside the query range.
	CellOutsideQuery Relation = iota
	// CellInsideQuery means the cell is entirely inside the query range.
	CellInsideQuery
	// CellCrossesQuery means the cell partially overlaps the query range.
	CellCrossesQuery
)

// IntersectVisitor visits documents during a BKD tree intersection.
type IntersectVisitor interface {
	// Visit is called for a document when its containing cell is entirely inside
	// the query range, so no per-value check is needed.
	Visit(docID int)
	// VisitValue is called for a document when its containing cell crosses the
	// query range, so the visitor must check the value.
	VisitValue(docID int, value int64)
	// Compare returns the relation between the query range and the cell defined
	// by [minValue, maxValue].
	Compare(minValue, maxValue int64) Relation
}

// PointTree represents a navigable BKD tree structure.
type PointTree interface {
	// MoveToChild moves to the left child. Returns false if at a leaf.
	MoveToChild() bool
	// MoveToSibling moves to the right sibling. Returns false if no sibling.
	MoveToSibling() bool
	// MoveToParent moves to the parent. Returns false if at root.
	MoveToParent() bool
	// MinValue returns the minimum value in the current cell.
	MinValue() int64
	// MaxValue returns the maximum value in the current cell.
	MaxValue() int64
	// Size returns the number of documents in the current cell.
	Size() int
	// VisitDocIDs visits all document IDs in the current cell without values.
	VisitDocIDs(v IntersectVisitor)
	// VisitDocValues visits all document ID/value pairs in the current cell.
	VisitDocValues(v IntersectVisitor)
}

// Intersect performs a recursive traversal of the BKD tree, calling the visitor
// for documents that match the query range.
func Intersect(tree PointTree, visitor IntersectVisitor) {
	rel := visitor.Compare(tree.MinValue(), tree.MaxValue())

	switch rel {
	case CellOutsideQuery:
		return
	case CellInsideQuery:
		tree.VisitDocIDs(visitor)
		return
	case CellCrossesQuery:
		if tree.MoveToChild() {
			// Recurse into left child.
			Intersect(tree, visitor)

			if tree.MoveToSibling() {
				// Recurse into right child.
				Intersect(tree, visitor)
			}

			tree.MoveToParent()
		} else {
			// Leaf node: visit individual values for filtering.
			tree.VisitDocValues(visitor)
		}
	}
}
