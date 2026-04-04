package translog

import (
	"path/filepath"
	"testing"
)

func testConfig(dir string) *TranslogConfig {
	return &TranslogConfig{Dir: dir}
}

func TestTranslog_WriteAndRecover(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tlog")

	tl, err := NewTranslog(testConfig(dir), "", 1, NoOpsPerformed, 1)
	if err != nil {
		t.Fatalf("NewTranslog: %v", err)
	}

	op1 := &IndexOperation{ID: "1", Source: []byte(`{"title":"hello"}`), SequenceNo: 0, PrimTerm: 1}
	op2 := &DeleteOperation{ID: "2", SequenceNo: 1, PrimTerm: 1}

	if _, err := tl.Add(op1); err != nil {
		t.Fatalf("Add op1: %v", err)
	}
	if _, err := tl.Add(op2); err != nil {
		t.Fatalf("Add op2: %v", err)
	}
	if err := tl.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := tl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and recover via snapshot.
	tl2, err := NewTranslog(testConfig(dir), "", 1, NoOpsPerformed, 1)
	if err != nil {
		t.Fatalf("NewTranslog reopen: %v", err)
	}
	defer tl2.Close()

	snap, err := tl2.NewSnapshot(0, 100)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Close()

	// Collect all ops.
	var ops []Operation
	for {
		op, err := snap.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if op == nil {
			break
		}
		ops = append(ops, op)
	}

	if len(ops) != 2 {
		t.Fatalf("expected 2 ops, got %d", len(ops))
	}

	idx, ok := ops[0].(*IndexOperation)
	if !ok {
		t.Fatalf("expected IndexOperation, got %T", ops[0])
	}
	if idx.ID != "1" || idx.SequenceNo != 0 {
		t.Errorf("op0: got ID=%s SequenceNo=%d, want ID=1 SequenceNo=0", idx.ID, idx.SequenceNo)
	}

	del, ok := ops[1].(*DeleteOperation)
	if !ok {
		t.Fatalf("expected DeleteOperation, got %T", ops[1])
	}
	if del.ID != "2" || del.SequenceNo != 1 {
		t.Errorf("op1: got ID=%s SequenceNo=%d, want ID=2 SequenceNo=1", del.ID, del.SequenceNo)
	}
}

func TestTranslog_RollGeneration(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tlog")

	tl, err := NewTranslog(testConfig(dir), "", 1, NoOpsPerformed, 1)
	if err != nil {
		t.Fatalf("NewTranslog: %v", err)
	}
	defer tl.Close()

	// Write to gen 1.
	if _, err := tl.Add(&IndexOperation{ID: "1", Source: []byte(`{"a":1}`), SequenceNo: 0, PrimTerm: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if tl.CurrentGeneration() != 1 {
		t.Fatalf("expected gen 1, got %d", tl.CurrentGeneration())
	}

	// Roll.
	if err := tl.RollGeneration(); err != nil {
		t.Fatalf("RollGeneration: %v", err)
	}

	if tl.CurrentGeneration() != 2 {
		t.Fatalf("expected gen 2, got %d", tl.CurrentGeneration())
	}

	// Write to gen 2.
	if _, err := tl.Add(&IndexOperation{ID: "2", Source: []byte(`{"a":2}`), SequenceNo: 1, PrimTerm: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Snapshot should see both ops.
	snap, err := tl.NewSnapshot(0, 100)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Close()

	count := 0
	for {
		op, err := snap.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if op == nil {
			break
		}
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 ops across generations, got %d", count)
	}
}

func TestTranslog_TrimUnreferencedReaders(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tlog")

	tl, err := NewTranslog(testConfig(dir), "", 1, NoOpsPerformed, 1)
	if err != nil {
		t.Fatalf("NewTranslog: %v", err)
	}
	defer tl.Close()

	// Write to gen 1 and roll.
	if _, err := tl.Add(&IndexOperation{ID: "1", Source: []byte(`{"x":1}`), SequenceNo: 0, PrimTerm: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := tl.RollGeneration(); err != nil {
		t.Fatalf("RollGeneration: %v", err)
	}

	// Set min required to current gen, trimming gen 1.
	tl.SetMinRequiredGeneration(tl.CurrentGeneration())
	if err := tl.TrimUnreferencedReaders(); err != nil {
		t.Fatalf("TrimUnreferencedReaders: %v", err)
	}

	// Snapshot should see 0 ops (gen 1 was trimmed).
	snap, err := tl.NewSnapshot(0, 100)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Close()

	count := 0
	for {
		op, err := snap.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if op == nil {
			break
		}
		count++
	}
	if count != 0 {
		t.Fatalf("expected 0 ops after trim, got %d", count)
	}
}
