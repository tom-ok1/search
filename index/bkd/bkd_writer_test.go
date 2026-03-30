package bkd

import (
	"sort"
	"testing"

	"gosearch/store"
)

// collectVisitor collects document IDs that match a range query.
type collectVisitor struct {
	min, max int64
	docs     []int
}

func (v *collectVisitor) Visit(docID int) {
	v.docs = append(v.docs, docID)
}

func (v *collectVisitor) VisitValue(docID int, value int64) {
	if value >= v.min && value <= v.max {
		v.docs = append(v.docs, docID)
	}
}

func (v *collectVisitor) Compare(minValue, maxValue int64) Relation {
	if minValue >= v.min && maxValue <= v.max {
		return CellInsideQuery
	}
	if minValue > v.max || maxValue < v.min {
		return CellOutsideQuery
	}
	return CellCrossesQuery
}

func mustDir(t *testing.T) store.Directory {
	t.Helper()
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSDirectory: %v", err)
	}
	return dir
}

func TestBKDWriter_SmallDataset(t *testing.T) {
	dir := mustDir(t)
	w := NewBKDWriter()
	w.Add(0, 10)
	w.Add(1, 30)
	w.Add(2, 20)
	w.Add(3, 50)
	w.Add(4, 40)

	if err := w.Finish(dir, "seg0", "price"); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	if !dir.FileExists("seg0.price.kd") {
		t.Fatal("expected seg0.price.kd to exist")
	}
}

func TestBKDWriter_LargeDataset(t *testing.T) {
	dir := mustDir(t)
	w := NewBKDWriter()
	for i := 2000; i > 0; i-- {
		w.Add(i, int64(i))
	}

	if err := w.Finish(dir, "seg1", "score"); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	if !dir.FileExists("seg1.score.kd") {
		t.Fatal("expected seg1.score.kd to exist")
	}
}

func TestBKDWriter_EmptyDataset(t *testing.T) {
	dir := mustDir(t)
	w := NewBKDWriter()

	if err := w.Finish(dir, "seg2", "empty"); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	if !dir.FileExists("seg2.empty.kd") {
		t.Fatal("expected seg2.empty.kd to exist")
	}
}

func TestBKDWriter_DuplicateValues(t *testing.T) {
	dir := mustDir(t)
	w := NewBKDWriter()
	for i := range 100 {
		w.Add(i, 42)
	}

	if err := w.Finish(dir, "seg3", "dup"); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	if !dir.FileExists("seg3.dup.kd") {
		t.Fatal("expected seg3.dup.kd to exist")
	}
}

// --- Roundtrip tests ---

func TestBKDWriter_Roundtrip_SingleLeaf(t *testing.T) {
	dir := mustDir(t)
	w := NewBKDWriter()
	w.Add(0, 10)
	w.Add(1, 30)
	w.Add(2, 20)

	if err := w.Finish(dir, "seg0", "rt"); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenBKDReader(dir, "seg0", "rt")
	if err != nil {
		t.Fatalf("OpenBKDReader: %v", err)
	}
	defer r.Close()

	if r.NumPoints() != 3 {
		t.Fatalf("NumPoints: got %d, want 3", r.NumPoints())
	}
	if r.MinValue() != 10 {
		t.Fatalf("MinValue: got %d, want 10", r.MinValue())
	}
	if r.MaxValue() != 30 {
		t.Fatalf("MaxValue: got %d, want 30", r.MaxValue())
	}

	// Range [10, 50] should match all 3 docs.
	v := &collectVisitor{min: 10, max: 50}
	tree := r.PointTree()
	Intersect(tree, v)
	if len(v.docs) != 3 {
		t.Fatalf("range [10,50]: got %d docs, want 3", len(v.docs))
	}
}

func TestBKDWriter_Roundtrip_MultiLeaf(t *testing.T) {
	dir := mustDir(t)
	w := NewBKDWriter()
	for i := 2000; i >= 1; i-- {
		w.Add(i, int64(i))
	}

	if err := w.Finish(dir, "seg1", "rt"); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenBKDReader(dir, "seg1", "rt")
	if err != nil {
		t.Fatalf("OpenBKDReader: %v", err)
	}
	defer r.Close()

	// Range [500, 510] should match 11 docs.
	v1 := &collectVisitor{min: 500, max: 510}
	Intersect(r.PointTree(), v1)
	if len(v1.docs) != 11 {
		t.Fatalf("range [500,510]: got %d docs, want 11", len(v1.docs))
	}
	// Verify the correct doc IDs are present.
	sort.Ints(v1.docs)
	for i, want := range []int{500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510} {
		if v1.docs[i] != want {
			t.Fatalf("range [500,510]: docs[%d] = %d, want %d", i, v1.docs[i], want)
		}
	}

	// Range [1, 2000] should match all docs.
	v2 := &collectVisitor{min: 1, max: 2000}
	Intersect(r.PointTree(), v2)
	if len(v2.docs) != 2000 {
		t.Fatalf("range [1,2000]: got %d docs, want 2000", len(v2.docs))
	}

	// Range [5000, 6000] should match 0 docs.
	v3 := &collectVisitor{min: 5000, max: 6000}
	Intersect(r.PointTree(), v3)
	if len(v3.docs) != 0 {
		t.Fatalf("range [5000,6000]: got %d docs, want 0", len(v3.docs))
	}
}

func TestBKDWriter_Roundtrip_Duplicates(t *testing.T) {
	dir := mustDir(t)
	w := NewBKDWriter()
	for i := range 100 {
		w.Add(i, 42)
	}

	if err := w.Finish(dir, "seg3", "rt"); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenBKDReader(dir, "seg3", "rt")
	if err != nil {
		t.Fatalf("OpenBKDReader: %v", err)
	}
	defer r.Close()

	// Range [42, 42] should match all 100 docs.
	v1 := &collectVisitor{min: 42, max: 42}
	Intersect(r.PointTree(), v1)
	if len(v1.docs) != 100 {
		t.Fatalf("range [42,42]: got %d docs, want 100", len(v1.docs))
	}

	// Range [43, 100] should match 0 docs.
	v2 := &collectVisitor{min: 43, max: 100}
	Intersect(r.PointTree(), v2)
	if len(v2.docs) != 0 {
		t.Fatalf("range [43,100]: got %d docs, want 0", len(v2.docs))
	}
}

func TestBKDWriter_Roundtrip_Empty(t *testing.T) {
	dir := mustDir(t)
	w := NewBKDWriter()

	if err := w.Finish(dir, "seg2", "rt"); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenBKDReader(dir, "seg2", "rt")
	if err != nil {
		t.Fatalf("OpenBKDReader: %v", err)
	}
	defer r.Close()

	if r.NumPoints() != 0 {
		t.Fatalf("NumPoints: got %d, want 0", r.NumPoints())
	}

	tree := r.PointTree()
	if tree.Size() != 0 {
		t.Fatalf("Size: got %d, want 0", tree.Size())
	}
	if tree.MoveToChild() {
		t.Fatal("MoveToChild should return false for empty tree")
	}
}
