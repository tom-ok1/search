package translog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTranslogReader_ReadAll(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.tlog")

	// Write 3 ops with TranslogWriter
	w, err := NewTranslogWriter(path)
	if err != nil {
		t.Fatalf("NewTranslogWriter: %v", err)
	}

	ops := []Operation{
		&IndexOperation{ID: "1", Source: []byte(`{"title":"doc1"}`), Version: 1},
		&DeleteOperation{ID: "2", Version: 2},
		&IndexOperation{ID: "3", Source: []byte(`{"title":"doc3"}`), Version: 3},
	}
	for _, op := range ops {
		if err := w.Add(op); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	// Open with TranslogReader
	r, err := NewTranslogReader(path)
	if err != nil {
		t.Fatalf("NewTranslogReader: %v", err)
	}
	defer r.Close()

	result, err := r.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(result) != 3 {
		t.Fatalf("expected 3 ops, got %d", len(result))
	}

	// Verify op 0: IndexOperation
	idx0, ok := result[0].(*IndexOperation)
	if !ok {
		t.Fatalf("op[0]: expected *IndexOperation, got %T", result[0])
	}
	if idx0.ID != "1" || string(idx0.Source) != `{"title":"doc1"}` || idx0.Version != 1 {
		t.Errorf("op[0]: unexpected fields: %+v", idx0)
	}

	// Verify op 1: DeleteOperation
	del1, ok := result[1].(*DeleteOperation)
	if !ok {
		t.Fatalf("op[1]: expected *DeleteOperation, got %T", result[1])
	}
	if del1.ID != "2" || del1.Version != 2 {
		t.Errorf("op[1]: unexpected fields: %+v", del1)
	}

	// Verify op 2: IndexOperation
	idx2, ok := result[2].(*IndexOperation)
	if !ok {
		t.Fatalf("op[2]: expected *IndexOperation, got %T", result[2])
	}
	if idx2.ID != "3" || string(idx2.Source) != `{"title":"doc3"}` || idx2.Version != 3 {
		t.Errorf("op[2]: unexpected fields: %+v", idx2)
	}
}

func TestTranslogReader_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.tlog")

	// Create empty file
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	f.Close()

	r, err := NewTranslogReader(path)
	if err != nil {
		t.Fatalf("NewTranslogReader: %v", err)
	}
	defer r.Close()

	ops, err := r.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("expected 0 ops, got %d", len(ops))
	}
}
