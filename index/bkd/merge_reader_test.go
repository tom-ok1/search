package bkd

import (
	"testing"
)

func TestMergeReader_IteratesAllPoints(t *testing.T) {
	dir := mustDir(t)
	w := NewBKDWriter()
	for i := range 10 {
		w.Add(i, int64(i*10))
	}
	if err := w.Finish(dir, "seg0", "f"); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	r, err := openBKDReaderForTest(dir, "seg0", "f")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	mr, err := NewMergeReader(r.PointTree())
	if err != nil {
		t.Fatalf("NewMergeReader: %v", err)
	}

	var got []point
	for mr.Next() {
		got = append(got, point{docID: mr.DocID(), value: mr.Value()})
	}
	if len(got) != 10 {
		t.Fatalf("got %d points, want 10", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].value < got[i-1].value {
			t.Fatalf("out of order at %d: %d < %d", i, got[i].value, got[i-1].value)
		}
	}
	for i, p := range got {
		if p.value != int64(i*10) {
			t.Errorf("got[%d].value = %d, want %d", i, p.value, i*10)
		}
	}
}

func TestMergeReader_LargeMultiLeaf(t *testing.T) {
	dir := mustDir(t)
	w := NewBKDWriter()
	n := 2000
	for i := n; i >= 1; i-- {
		w.Add(i, int64(i))
	}
	if err := w.Finish(dir, "seg0", "f"); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	r, err := openBKDReaderForTest(dir, "seg0", "f")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	mr, err := NewMergeReader(r.PointTree())
	if err != nil {
		t.Fatalf("NewMergeReader: %v", err)
	}

	count := 0
	var prev int64 = -1
	for mr.Next() {
		if mr.Value() < prev {
			t.Fatalf("out of order at %d: %d < %d", count, mr.Value(), prev)
		}
		prev = mr.Value()
		count++
	}
	if count != n {
		t.Fatalf("got %d points, want %d", count, n)
	}
}

func TestMergeReader_Empty(t *testing.T) {
	tree := &emptyPointTree{}
	mr, err := NewMergeReader(tree)
	if err != nil {
		t.Fatalf("NewMergeReader: %v", err)
	}
	if mr.Next() {
		t.Fatal("expected no points from empty tree")
	}
}
