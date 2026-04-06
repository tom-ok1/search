package bkd

import (
	"sort"
	"testing"
)

func TestOneDimensionBKDWriter_SmallDataset(t *testing.T) {
	dir := mustDir(t)
	odw, err := NewOneDimensionBKDWriter(dir, "seg0", "price")
	if err != nil {
		t.Fatalf("NewOneDimensionBKDWriter: %v", err)
	}

	points := []point{{0, 10}, {2, 20}, {1, 30}, {4, 40}, {3, 50}}
	for _, p := range points {
		if err := odw.Add(p.docID, p.value); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	if err := odw.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := openBKDReaderForTest(dir, "seg0", "price")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	if r.NumPoints() != 5 {
		t.Fatalf("NumPoints = %d, want 5", r.NumPoints())
	}
	if r.MinValue() != 10 {
		t.Fatalf("MinValue = %d, want 10", r.MinValue())
	}
	if r.MaxValue() != 50 {
		t.Fatalf("MaxValue = %d, want 50", r.MaxValue())
	}

	v := &collectVisitor{min: 10, max: 50}
	Intersect(r.PointTree(), v)
	if len(v.docs) != 5 {
		t.Fatalf("range [10,50]: got %d docs, want 5", len(v.docs))
	}
}

func TestOneDimensionBKDWriter_LargeDataset(t *testing.T) {
	dir := mustDir(t)
	odw, err := NewOneDimensionBKDWriter(dir, "seg0", "score")
	if err != nil {
		t.Fatalf("NewOneDimensionBKDWriter: %v", err)
	}

	n := 5000
	for i := range n {
		if err := odw.Add(i, int64(i)); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := odw.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := openBKDReaderForTest(dir, "seg0", "score")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	if r.NumPoints() != n {
		t.Fatalf("NumPoints = %d, want %d", r.NumPoints(), n)
	}

	v := &collectVisitor{min: 100, max: 199}
	Intersect(r.PointTree(), v)
	sort.Ints(v.docs)
	if len(v.docs) != 100 {
		t.Fatalf("range [100,199]: got %d docs, want 100", len(v.docs))
	}
	for i, d := range v.docs {
		if d != 100+i {
			t.Fatalf("docs[%d] = %d, want %d", i, d, 100+i)
		}
	}
}

func TestOneDimensionBKDWriter_Empty(t *testing.T) {
	dir := mustDir(t)
	odw, err := NewOneDimensionBKDWriter(dir, "seg0", "empty")
	if err != nil {
		t.Fatalf("NewOneDimensionBKDWriter: %v", err)
	}
	if err := odw.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := openBKDReaderForTest(dir, "seg0", "empty")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	if r.NumPoints() != 0 {
		t.Fatalf("NumPoints = %d, want 0", r.NumPoints())
	}
}

func TestComputeInnerNodesFromMetas(t *testing.T) {
	// 4 leaves → padded to 4 (already power of 2) → 3 inner nodes
	metas := []leafMeta{
		{numPts: 2, maxValue: 10},
		{numPts: 3, maxValue: 30},
		{numPts: 1, maxValue: 50},
		{numPts: 4, maxValue: 80},
	}
	numInnerNodes := 3
	allMetas := make([]leafMeta, 4)
	copy(allMetas, metas)

	innerNodes := make([]innerNode, numInnerNodes+1)
	computeInnerNodesFromMetas(1, numInnerNodes, allMetas, innerNodes)

	// Node 1 (root): splitValue = max of left subtree (leaves 0,1) = 30
	if innerNodes[1].splitValue != 30 {
		t.Fatalf("root splitValue = %d, want 30", innerNodes[1].splitValue)
	}
	if innerNodes[1].numPoints != 10 {
		t.Fatalf("root numPoints = %d, want 10", innerNodes[1].numPoints)
	}
	// Node 2 (left inner): splitValue = max of leaf 0 = 10
	if innerNodes[2].splitValue != 10 {
		t.Fatalf("left inner splitValue = %d, want 10", innerNodes[2].splitValue)
	}
	if innerNodes[2].numPoints != 5 {
		t.Fatalf("left inner numPoints = %d, want 5", innerNodes[2].numPoints)
	}
	// Node 3 (right inner): splitValue = max of leaf 2 = 50
	if innerNodes[3].splitValue != 50 {
		t.Fatalf("right inner splitValue = %d, want 50", innerNodes[3].splitValue)
	}
	if innerNodes[3].numPoints != 5 {
		t.Fatalf("right inner numPoints = %d, want 5", innerNodes[3].numPoints)
	}
}

func TestOneDimensionBKDWriter_Duplicates(t *testing.T) {
	dir := mustDir(t)
	odw, err := NewOneDimensionBKDWriter(dir, "seg0", "dup")
	if err != nil {
		t.Fatalf("NewOneDimensionBKDWriter: %v", err)
	}
	for i := range 100 {
		if err := odw.Add(i, 42); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := odw.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := openBKDReaderForTest(dir, "seg0", "dup")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	v := &collectVisitor{min: 42, max: 42}
	Intersect(r.PointTree(), v)
	if len(v.docs) != 100 {
		t.Fatalf("range [42,42]: got %d docs, want 100", len(v.docs))
	}
}

func TestOneDimensionBKDWriter_TempFileCleanup(t *testing.T) {
	dir := mustDir(t)
	odw, err := NewOneDimensionBKDWriter(dir, "seg0", "price")
	if err != nil {
		t.Fatalf("NewOneDimensionBKDWriter: %v", err)
	}

	// Temp file should exist after construction.
	if !dir.FileExists("seg0.price.kd.tmp") {
		t.Fatal("expected temp file to exist after construction")
	}

	for i := range 1000 {
		if err := odw.Add(i, int64(i)); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := odw.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Temp file should be cleaned up after Finish.
	if dir.FileExists("seg0.price.kd.tmp") {
		t.Fatal("expected temp file to be deleted after Finish")
	}
	// Final .kd file should exist.
	if !dir.FileExists("seg0.price.kd") {
		t.Fatal("expected .kd file to exist")
	}
}

func TestOneDimensionBKDWriter_EmptyTempFileCleanup(t *testing.T) {
	dir := mustDir(t)
	odw, err := NewOneDimensionBKDWriter(dir, "seg0", "empty")
	if err != nil {
		t.Fatalf("NewOneDimensionBKDWriter: %v", err)
	}

	if err := odw.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	if dir.FileExists("seg0.empty.kd.tmp") {
		t.Fatal("expected temp file to be deleted after empty Finish")
	}
}

func TestOneDimensionBKDWriter_LargeMultiLeaf(t *testing.T) {
	dir := mustDir(t)
	odw, err := NewOneDimensionBKDWriter(dir, "seg0", "big")
	if err != nil {
		t.Fatalf("NewOneDimensionBKDWriter: %v", err)
	}

	n := 50000
	for i := range n {
		if err := odw.Add(i, int64(i*10)); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := odw.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := openBKDReaderForTest(dir, "seg0", "big")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	if r.NumPoints() != n {
		t.Fatalf("NumPoints = %d, want %d", r.NumPoints(), n)
	}
	if r.MinValue() != 0 {
		t.Fatalf("MinValue = %d, want 0", r.MinValue())
	}
	if r.MaxValue() != int64((n-1)*10) {
		t.Fatalf("MaxValue = %d, want %d", r.MaxValue(), (n-1)*10)
	}

	// Spot-check a range query.
	v := &collectVisitor{min: 1000, max: 1090}
	Intersect(r.PointTree(), v)
	sort.Ints(v.docs)
	if len(v.docs) != 10 {
		t.Fatalf("range [1000,1090]: got %d docs, want 10", len(v.docs))
	}
	for i, d := range v.docs {
		want := 100 + i
		if d != want {
			t.Fatalf("docs[%d] = %d, want %d", i, d, want)
		}
	}
}
