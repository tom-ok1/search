package translog

import (
	"path/filepath"
	"testing"
)

func TestTranslogReader_Snapshot(t *testing.T) {
	dir := t.TempDir()
	tlogPath := filepath.Join(dir, "translog-1.tlog")
	ckpPath := filepath.Join(dir, "translog.ckp")
	genCkpPath := filepath.Join(dir, "translog-1.ckp")

	header := testHeader()
	cp := EmptyCheckpoint(1, 1)
	cp.Offset = HeaderSizeInBytes(&header)

	w, err := NewTranslogWriter(tlogPath, ckpPath, 1, header, *cp)
	if err != nil {
		t.Fatalf("NewTranslogWriter: %v", err)
	}

	if _, err := w.Add(&IndexOperation{ID: "1", Source: []byte(`{"a":1}`), SequenceNo: 0, PrimTerm: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := w.Add(&IndexOperation{ID: "2", Source: []byte(`{"a":2}`), SequenceNo: 1, PrimTerm: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	reader, err := w.CloseIntoReader(genCkpPath)
	if err != nil {
		t.Fatalf("CloseIntoReader: %v", err)
	}
	defer reader.Close()

	snap := reader.Snapshot()
	defer snap.Close()

	if snap.TotalOperations() != 2 {
		t.Fatalf("expected 2 ops, got %d", snap.TotalOperations())
	}

	op1, err := snap.Next()
	if err != nil {
		t.Fatalf("Next 1: %v", err)
	}
	if op1.(*IndexOperation).ID != "1" {
		t.Errorf("expected ID 1, got %s", op1.(*IndexOperation).ID)
	}

	op2, err := snap.Next()
	if err != nil {
		t.Fatalf("Next 2: %v", err)
	}
	if op2.(*IndexOperation).ID != "2" {
		t.Errorf("expected ID 2, got %s", op2.(*IndexOperation).ID)
	}

	op3, err := snap.Next()
	if err != nil {
		t.Fatalf("Next 3: %v", err)
	}
	if op3 != nil {
		t.Errorf("expected nil at end, got %v", op3)
	}
}
