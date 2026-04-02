package bkd

import "testing"

// mockLeaf is a PointTree representing a single leaf node with documents.
type mockLeaf struct {
	minVal int64
	maxVal int64
	docs   []struct {
		docID int
		value int64
	}
}

func (m *mockLeaf) MoveToChild() bool   { return false }
func (m *mockLeaf) MoveToSibling() bool { return false }
func (m *mockLeaf) MoveToParent() bool  { return false }
func (m *mockLeaf) MinValue() int64     { return m.minVal }
func (m *mockLeaf) MaxValue() int64     { return m.maxVal }
func (m *mockLeaf) Size() int           { return len(m.docs) }

func (m *mockLeaf) VisitDocIDs(v IntersectVisitor) {
	for _, d := range m.docs {
		v.Visit(d.docID)
	}
}

func (m *mockLeaf) VisitDocValues(v IntersectVisitor) {
	for _, d := range m.docs {
		v.VisitValue(d.docID, d.value)
	}
}

// rangeVisitor is an IntersectVisitor that collects documents within [min, max].
type rangeVisitor struct {
	min, max   int64
	visited    []int // docIDs from Visit (cell inside)
	visitedVal []int // docIDs from VisitValue (cell crosses, value matched)
}

func (r *rangeVisitor) Visit(docID int) {
	r.visited = append(r.visited, docID)
}

func (r *rangeVisitor) VisitValue(docID int, value int64) {
	if value >= r.min && value <= r.max {
		r.visitedVal = append(r.visitedVal, docID)
	}
}

func (r *rangeVisitor) Compare(minValue, maxValue int64) Relation {
	if minValue >= r.min && maxValue <= r.max {
		return CellInsideQuery
	}
	if minValue > r.max || maxValue < r.min {
		return CellOutsideQuery
	}
	return CellCrossesQuery
}

func TestIntersect_CellInside(t *testing.T) {
	// Visitor range [0,100] fully contains leaf [10,50] → all docs visited via Visit()
	tree := &mockLeaf{
		minVal: 10,
		maxVal: 50,
		docs: []struct {
			docID int
			value int64
		}{
			{1, 15},
			{2, 30},
			{3, 45},
		},
	}
	visitor := &rangeVisitor{min: 0, max: 100}

	Intersect(tree, visitor)

	if len(visitor.visited) != 3 {
		t.Fatalf("expected 3 docs visited via Visit(), got %d", len(visitor.visited))
	}
	if len(visitor.visitedVal) != 0 {
		t.Fatalf("expected 0 docs visited via VisitValue(), got %d", len(visitor.visitedVal))
	}
}

func TestIntersect_CellOutside(t *testing.T) {
	// Visitor range [100,200] does not overlap leaf [10,50] → 0 docs
	tree := &mockLeaf{
		minVal: 10,
		maxVal: 50,
		docs: []struct {
			docID int
			value int64
		}{
			{1, 15},
			{2, 30},
			{3, 45},
		},
	}
	visitor := &rangeVisitor{min: 100, max: 200}

	Intersect(tree, visitor)

	if len(visitor.visited) != 0 {
		t.Fatalf("expected 0 docs visited via Visit(), got %d", len(visitor.visited))
	}
	if len(visitor.visitedVal) != 0 {
		t.Fatalf("expected 0 docs visited via VisitValue(), got %d", len(visitor.visitedVal))
	}
}

func TestIntersect_CellCrosses(t *testing.T) {
	// Visitor range [20,40] partially overlaps leaf [10,50] → only doc with value 30 visited via VisitValue()
	tree := &mockLeaf{
		minVal: 10,
		maxVal: 50,
		docs: []struct {
			docID int
			value int64
		}{
			{1, 15},
			{2, 30},
			{3, 45},
		},
	}
	visitor := &rangeVisitor{min: 20, max: 40}

	Intersect(tree, visitor)

	if len(visitor.visited) != 0 {
		t.Fatalf("expected 0 docs visited via Visit(), got %d", len(visitor.visited))
	}
	if len(visitor.visitedVal) != 1 {
		t.Fatalf("expected 1 doc visited via VisitValue(), got %d", len(visitor.visitedVal))
	}
	if visitor.visitedVal[0] != 2 {
		t.Fatalf("expected docID 2, got %d", visitor.visitedVal[0])
	}
}
