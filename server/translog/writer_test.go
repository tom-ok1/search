package translog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTranslogWriter_WriteAndSync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "translog-1.tlog")

	w, err := NewTranslogWriter(path)
	if err != nil {
		t.Fatalf("NewTranslogWriter: %v", err)
	}

	if err := w.Add(&IndexOperation{ID: "1", Source: []byte(`{"a":1}`), Version: 1}); err != nil {
		t.Fatalf("Add index op: %v", err)
	}
	if err := w.Add(&DeleteOperation{ID: "2", Version: 2}); err != nil {
		t.Fatalf("Add delete op: %v", err)
	}

	if got := w.Operations(); got != 2 {
		t.Fatalf("Operations() = %d, want 2", got)
	}

	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify file exists and is non-empty.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("translog file is empty after writing two operations")
	}
}

func TestTranslogWriter_ReadBack(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "translog-2.tlog")

	w, err := NewTranslogWriter(path)
	if err != nil {
		t.Fatalf("NewTranslogWriter: %v", err)
	}

	if err := w.Add(&IndexOperation{ID: "doc1", Source: []byte(`{"field":"value"}`), Version: 5}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := w.Add(&DeleteOperation{ID: "doc2", Version: 10}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read back and verify with ReadOperation.
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	op1, err := ReadOperation(f)
	if err != nil {
		t.Fatalf("ReadOperation 1: %v", err)
	}
	idx, ok := op1.(*IndexOperation)
	if !ok {
		t.Fatalf("expected *IndexOperation, got %T", op1)
	}
	if idx.ID != "doc1" || idx.Version != 5 || string(idx.Source) != `{"field":"value"}` {
		t.Fatalf("unexpected IndexOperation: %+v", idx)
	}

	op2, err := ReadOperation(f)
	if err != nil {
		t.Fatalf("ReadOperation 2: %v", err)
	}
	del, ok := op2.(*DeleteOperation)
	if !ok {
		t.Fatalf("expected *DeleteOperation, got %T", op2)
	}
	if del.ID != "doc2" || del.Version != 10 {
		t.Fatalf("unexpected DeleteOperation: %+v", del)
	}
}
