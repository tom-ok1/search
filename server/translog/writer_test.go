package translog

import (
	"os"
	"path/filepath"
	"testing"
)

func testHeader() TranslogHeader {
	return TranslogHeader{TranslogUUID: "test-uuid", PrimaryTerm: 1}
}

func TestTranslogWriter_WriteAndSync(t *testing.T) {
	dir := t.TempDir()
	tlogPath := filepath.Join(dir, "translog-1.tlog")
	ckpPath := filepath.Join(dir, "translog.ckp")

	header := testHeader()
	cp := EmptyCheckpoint(1, 1)
	cp.Offset = HeaderSizeInBytes(&header)

	w, err := NewTranslogWriter(tlogPath, ckpPath, 1, header, *cp)
	if err != nil {
		t.Fatalf("NewTranslogWriter: %v", err)
	}

	if _, err := w.Add(&IndexOperation{ID: "1", Source: []byte(`{"a":1}`), SequenceNo: 0, PrimTerm: 1}); err != nil {
		t.Fatalf("Add index op: %v", err)
	}
	if _, err := w.Add(&DeleteOperation{ID: "2", SequenceNo: 1, PrimTerm: 1}); err != nil {
		t.Fatalf("Add delete op: %v", err)
	}

	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify file exists and is non-empty.
	info, err := os.Stat(tlogPath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("translog file is empty after writing two operations")
	}

	// Verify checkpoint was written.
	ckpData, err := os.ReadFile(ckpPath)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	ckp, err := UnmarshalCheckpoint(ckpData)
	if err != nil {
		t.Fatalf("unmarshal checkpoint: %v", err)
	}
	if ckp.NumOps != 2 {
		t.Errorf("checkpoint NumOps = %d, want 2", ckp.NumOps)
	}
	if ckp.MinSeqNo != 0 {
		t.Errorf("checkpoint MinSeqNo = %d, want 0", ckp.MinSeqNo)
	}
	if ckp.MaxSeqNo != 1 {
		t.Errorf("checkpoint MaxSeqNo = %d, want 1", ckp.MaxSeqNo)
	}
}

func TestTranslogWriter_ReadBack(t *testing.T) {
	dir := t.TempDir()
	tlogPath := filepath.Join(dir, "translog-1.tlog")
	ckpPath := filepath.Join(dir, "translog.ckp")

	header := testHeader()
	cp := EmptyCheckpoint(1, 1)
	cp.Offset = HeaderSizeInBytes(&header)

	w, err := NewTranslogWriter(tlogPath, ckpPath, 1, header, *cp)
	if err != nil {
		t.Fatalf("NewTranslogWriter: %v", err)
	}

	if _, err := w.Add(&IndexOperation{ID: "doc1", Source: []byte(`{"field":"value"}`), SequenceNo: 0, PrimTerm: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := w.Add(&DeleteOperation{ID: "doc2", SequenceNo: 1, PrimTerm: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read back and verify.
	f, err := os.Open(tlogPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	// Skip header.
	_, _, err = ReadHeader(f)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	op1, err := ReadOperation(f)
	if err != nil {
		t.Fatalf("ReadOperation 1: %v", err)
	}
	idx, ok := op1.(*IndexOperation)
	if !ok {
		t.Fatalf("expected *IndexOperation, got %T", op1)
	}
	if idx.ID != "doc1" || idx.SequenceNo != 0 || string(idx.Source) != `{"field":"value"}` {
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
	if del.ID != "doc2" || del.SequenceNo != 1 {
		t.Fatalf("unexpected DeleteOperation: %+v", del)
	}
}

func TestTranslogWriter_CloseIntoReader(t *testing.T) {
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

	if _, err := w.Add(&IndexOperation{ID: "doc1", Source: []byte(`{"a":1}`), SequenceNo: 0, PrimTerm: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	reader, err := w.CloseIntoReader(genCkpPath)
	if err != nil {
		t.Fatalf("CloseIntoReader: %v", err)
	}
	defer reader.Close()

	if reader.generation != 1 {
		t.Errorf("expected generation 1, got %d", reader.generation)
	}
	if reader.checkpoint.NumOps != 1 {
		t.Errorf("expected 1 op, got %d", reader.checkpoint.NumOps)
	}

	// Verify per-generation checkpoint was written.
	if _, err := os.Stat(genCkpPath); err != nil {
		t.Fatalf("generation checkpoint file not found: %v", err)
	}
}
