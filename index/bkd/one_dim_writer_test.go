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
