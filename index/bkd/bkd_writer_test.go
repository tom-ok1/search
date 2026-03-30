package bkd

import (
	"testing"

	"gosearch/store"
)

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
