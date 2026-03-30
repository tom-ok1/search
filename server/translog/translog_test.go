package translog

import (
	"path/filepath"
	"testing"
)

func TestTranslog_WriteAndRecover(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tlog")

	tl, err := NewTranslog(dir)
	if err != nil {
		t.Fatalf("NewTranslog: %v", err)
	}

	op1 := &IndexOperation{ID: "1", Source: []byte(`{"title":"hello"}`), Version: 1}
	op2 := &DeleteOperation{ID: "2", Version: 2}

	if err := tl.Add(op1); err != nil {
		t.Fatalf("Add op1: %v", err)
	}
	if err := tl.Add(op2); err != nil {
		t.Fatalf("Add op2: %v", err)
	}
	if err := tl.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := tl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and recover
	tl2, err := NewTranslog(dir)
	if err != nil {
		t.Fatalf("NewTranslog reopen: %v", err)
	}
	defer tl2.Close()

	ops, err := tl2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(ops) != 2 {
		t.Fatalf("expected 2 ops, got %d", len(ops))
	}

	idx, ok := ops[0].(*IndexOperation)
	if !ok {
		t.Fatalf("expected IndexOperation, got %T", ops[0])
	}
	if idx.ID != "1" || idx.Version != 1 {
		t.Errorf("op0: got ID=%s Version=%d, want ID=1 Version=1", idx.ID, idx.Version)
	}

	del, ok := ops[1].(*DeleteOperation)
	if !ok {
		t.Fatalf("expected DeleteOperation, got %T", ops[1])
	}
	if del.ID != "2" || del.Version != 2 {
		t.Errorf("op1: got ID=%s Version=%d, want ID=2 Version=2", del.ID, del.Version)
	}
}

func TestTranslog_TrimOnCommit(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tlog")

	tl, err := NewTranslog(dir)
	if err != nil {
		t.Fatalf("NewTranslog: %v", err)
	}

	op := &IndexOperation{ID: "1", Source: []byte(`{"x":1}`), Version: 1}
	if err := tl.Add(op); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := tl.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Trim simulates what happens on commit
	if err := tl.TrimToEmpty(); err != nil {
		t.Fatalf("TrimToEmpty: %v", err)
	}
	if err := tl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and recover should return 0 ops
	tl2, err := NewTranslog(dir)
	if err != nil {
		t.Fatalf("NewTranslog reopen: %v", err)
	}
	defer tl2.Close()

	ops, err := tl2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if len(ops) != 0 {
		t.Fatalf("expected 0 ops after trim, got %d", len(ops))
	}
}
