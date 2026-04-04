package translog

import (
	"path/filepath"
	"testing"
)

// newReaderWithOps creates a TranslogReader containing the given operations.
func newReaderWithOps(t *testing.T, ops []Operation, cp *Checkpoint) *TranslogReader {
	t.Helper()
	dir := t.TempDir()
	tlogPath := filepath.Join(dir, "translog-1.tlog")
	ckpPath := filepath.Join(dir, "translog.ckp")
	genCkpPath := filepath.Join(dir, "translog-1.ckp")

	header := testHeader()
	if cp == nil {
		cp = EmptyCheckpoint(1, 1)
	}
	cp.Offset = HeaderSizeInBytes(&header)

	w, err := NewTranslogWriter(tlogPath, ckpPath, 1, header, *cp)
	if err != nil {
		t.Fatalf("NewTranslogWriter: %v", err)
	}
	for _, op := range ops {
		if _, err := w.Add(op); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	reader, err := w.CloseIntoReader(genCkpPath)
	if err != nil {
		t.Fatalf("CloseIntoReader: %v", err)
	}
	return reader
}

// collectOps drains a Snapshot and returns all operations.
func collectOps(t *testing.T, snap Snapshot) []Operation {
	t.Helper()
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
	return ops
}

func TestTranslogSnapshot_Empty(t *testing.T) {
	reader := newReaderWithOps(t, nil, nil)
	defer reader.Close()

	snap := reader.Snapshot()
	defer snap.Close()

	if snap.TotalOperations() != 0 {
		t.Fatalf("expected 0 ops, got %d", snap.TotalOperations())
	}

	op, err := snap.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if op != nil {
		t.Errorf("expected nil, got %v", op)
	}
}

func TestTranslogSnapshot_TrimmedAboveSeqNo(t *testing.T) {
	ops := []Operation{
		&IndexOperation{ID: "1", Source: []byte(`{}`), SequenceNo: 0, PrimTerm: 1},
		&IndexOperation{ID: "2", Source: []byte(`{}`), SequenceNo: 1, PrimTerm: 1},
		&IndexOperation{ID: "3", Source: []byte(`{}`), SequenceNo: 2, PrimTerm: 1},
		&IndexOperation{ID: "4", Source: []byte(`{}`), SequenceNo: 3, PrimTerm: 1},
	}

	cp := EmptyCheckpoint(1, 1)
	cp.TrimmedAboveSeqNo = 1 // only seqNo 0 and 1 should be returned

	reader := newReaderWithOps(t, ops, cp)
	defer reader.Close()

	snap := reader.Snapshot()
	defer snap.Close()

	got := collectOps(t, snap)
	if len(got) != 2 {
		t.Fatalf("expected 2 ops (trimmed above seqNo 1), got %d", len(got))
	}
	if got[0].SeqNo() != 0 {
		t.Errorf("first op seqNo = %d, want 0", got[0].SeqNo())
	}
	if got[1].SeqNo() != 1 {
		t.Errorf("second op seqNo = %d, want 1", got[1].SeqNo())
	}
}

func TestTranslogSnapshot_TrimmedAboveSeqNo_Unassigned(t *testing.T) {
	ops := []Operation{
		&IndexOperation{ID: "1", Source: []byte(`{}`), SequenceNo: 0, PrimTerm: 1},
		&IndexOperation{ID: "2", Source: []byte(`{}`), SequenceNo: 1, PrimTerm: 1},
	}

	// SeqNoUnassigned means no trimming — all ops returned.
	reader := newReaderWithOps(t, ops, nil)
	defer reader.Close()

	snap := reader.Snapshot()
	defer snap.Close()

	got := collectOps(t, snap)
	if len(got) != 2 {
		t.Fatalf("expected 2 ops (no trimming), got %d", len(got))
	}
}

func TestMultiSnapshot_Deduplication(t *testing.T) {
	// Older generation: seqNo 0, 1
	olderOps := []Operation{
		&IndexOperation{ID: "a", Source: []byte(`{"v":1}`), SequenceNo: 0, PrimTerm: 1},
		&IndexOperation{ID: "b", Source: []byte(`{"v":1}`), SequenceNo: 1, PrimTerm: 1},
	}
	olderReader := newReaderWithOps(t, olderOps, nil)
	defer olderReader.Close()

	// Newer generation: seqNo 1 (dup), 2
	newerOps := []Operation{
		&IndexOperation{ID: "b-updated", Source: []byte(`{"v":2}`), SequenceNo: 1, PrimTerm: 1},
		&IndexOperation{ID: "c", Source: []byte(`{"v":1}`), SequenceNo: 2, PrimTerm: 1},
	}
	newerReader := newReaderWithOps(t, newerOps, nil)
	defer newerReader.Close()

	// Oldest first in the slice.
	ms := NewMultiSnapshot([]*TranslogSnapshot{
		olderReader.Snapshot(),
		newerReader.Snapshot(),
	})
	defer ms.Close()

	got := collectOps(t, ms)

	// Expect 3 unique ops: seqNo 1 from newer wins, seqNo 0 from older, seqNo 2 from newer.
	if len(got) != 3 {
		t.Fatalf("expected 3 unique ops, got %d", len(got))
	}

	// Newer generation is iterated first, so seqNo 1 and 2 come first.
	seqNos := make(map[int64]bool)
	for _, op := range got {
		seqNos[op.SeqNo()] = true
	}
	for _, want := range []int64{0, 1, 2} {
		if !seqNos[want] {
			t.Errorf("missing seqNo %d in result", want)
		}
	}

	// Verify dedup: seqNo 1 should be from newer generation (ID "b-updated").
	for _, op := range got {
		if op.SeqNo() == 1 {
			idx := op.(*IndexOperation)
			if idx.ID != "b-updated" {
				t.Errorf("seqNo 1 should be from newer generation, got ID %q", idx.ID)
			}
		}
	}
}

func TestMultiSnapshot_Empty(t *testing.T) {
	ms := NewMultiSnapshot(nil)
	defer ms.Close()

	if ms.TotalOperations() != 0 {
		t.Fatalf("expected 0, got %d", ms.TotalOperations())
	}

	op, err := ms.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if op != nil {
		t.Errorf("expected nil, got %v", op)
	}
}

func TestMultiSnapshot_SingleGeneration(t *testing.T) {
	ops := []Operation{
		&IndexOperation{ID: "1", Source: []byte(`{}`), SequenceNo: 0, PrimTerm: 1},
		&DeleteOperation{ID: "2", SequenceNo: 1, PrimTerm: 1},
	}
	reader := newReaderWithOps(t, ops, nil)
	defer reader.Close()

	ms := NewMultiSnapshot([]*TranslogSnapshot{reader.Snapshot()})
	defer ms.Close()

	if ms.TotalOperations() != 2 {
		t.Fatalf("expected 2, got %d", ms.TotalOperations())
	}

	got := collectOps(t, ms)
	if len(got) != 2 {
		t.Fatalf("expected 2 ops, got %d", len(got))
	}
}

func TestSeqNoFilterSnapshot_Range(t *testing.T) {
	ops := []Operation{
		&IndexOperation{ID: "1", Source: []byte(`{}`), SequenceNo: 0, PrimTerm: 1},
		&IndexOperation{ID: "2", Source: []byte(`{}`), SequenceNo: 1, PrimTerm: 1},
		&IndexOperation{ID: "3", Source: []byte(`{}`), SequenceNo: 2, PrimTerm: 1},
		&IndexOperation{ID: "4", Source: []byte(`{}`), SequenceNo: 3, PrimTerm: 1},
		&IndexOperation{ID: "5", Source: []byte(`{}`), SequenceNo: 4, PrimTerm: 1},
	}
	reader := newReaderWithOps(t, ops, nil)
	defer reader.Close()

	snap := reader.Snapshot()
	filtered := NewSeqNoFilterSnapshot(snap, 1, 3)
	defer filtered.Close()

	got := collectOps(t, filtered)
	if len(got) != 3 {
		t.Fatalf("expected 3 ops in range [1,3], got %d", len(got))
	}
	for i, want := range []int64{1, 2, 3} {
		if got[i].SeqNo() != want {
			t.Errorf("op[%d] seqNo = %d, want %d", i, got[i].SeqNo(), want)
		}
	}
}

func TestSeqNoFilterSnapshot_AllFiltered(t *testing.T) {
	ops := []Operation{
		&IndexOperation{ID: "1", Source: []byte(`{}`), SequenceNo: 0, PrimTerm: 1},
		&IndexOperation{ID: "2", Source: []byte(`{}`), SequenceNo: 1, PrimTerm: 1},
	}
	reader := newReaderWithOps(t, ops, nil)
	defer reader.Close()

	snap := reader.Snapshot()
	filtered := NewSeqNoFilterSnapshot(snap, 10, 20)
	defer filtered.Close()

	got := collectOps(t, filtered)
	if len(got) != 0 {
		t.Fatalf("expected 0 ops, got %d", len(got))
	}
}

func TestSeqNoFilterSnapshot_TotalOperations(t *testing.T) {
	ops := []Operation{
		&IndexOperation{ID: "1", Source: []byte(`{}`), SequenceNo: 0, PrimTerm: 1},
		&IndexOperation{ID: "2", Source: []byte(`{}`), SequenceNo: 1, PrimTerm: 1},
		&IndexOperation{ID: "3", Source: []byte(`{}`), SequenceNo: 2, PrimTerm: 1},
	}
	reader := newReaderWithOps(t, ops, nil)
	defer reader.Close()

	snap := reader.Snapshot()
	filtered := NewSeqNoFilterSnapshot(snap, 1, 1) // only seqNo 1

	// TotalOperations starts at inner total (3).
	// After draining, filtered count should adjust.
	got := collectOps(t, filtered)
	if len(got) != 1 {
		t.Fatalf("expected 1 op, got %d", len(got))
	}
	// After full iteration, TotalOperations should reflect actual count.
	if filtered.TotalOperations() != 1 {
		t.Errorf("TotalOperations = %d, want 1", filtered.TotalOperations())
	}
}

func TestSeqNoFilterSnapshot_NoFiltering(t *testing.T) {
	ops := []Operation{
		&IndexOperation{ID: "1", Source: []byte(`{}`), SequenceNo: 0, PrimTerm: 1},
		&IndexOperation{ID: "2", Source: []byte(`{}`), SequenceNo: 1, PrimTerm: 1},
	}
	reader := newReaderWithOps(t, ops, nil)
	defer reader.Close()

	snap := reader.Snapshot()
	filtered := NewSeqNoFilterSnapshot(snap, 0, 100)
	defer filtered.Close()

	got := collectOps(t, filtered)
	if len(got) != 2 {
		t.Fatalf("expected 2 ops (no filtering), got %d", len(got))
	}
}

func TestMultiSnapshot_ThreeGenerations(t *testing.T) {
	// gen1: seqNo 0
	gen1 := newReaderWithOps(t, []Operation{
		&IndexOperation{ID: "a", Source: []byte(`{}`), SequenceNo: 0, PrimTerm: 1},
	}, nil)
	defer gen1.Close()

	// gen2: seqNo 1, 2
	gen2 := newReaderWithOps(t, []Operation{
		&IndexOperation{ID: "b", Source: []byte(`{}`), SequenceNo: 1, PrimTerm: 1},
		&IndexOperation{ID: "c", Source: []byte(`{}`), SequenceNo: 2, PrimTerm: 1},
	}, nil)
	defer gen2.Close()

	// gen3: seqNo 2 (dup), 3
	gen3 := newReaderWithOps(t, []Operation{
		&DeleteOperation{ID: "c", SequenceNo: 2, PrimTerm: 1},
		&IndexOperation{ID: "d", Source: []byte(`{}`), SequenceNo: 3, PrimTerm: 1},
	}, nil)
	defer gen3.Close()

	ms := NewMultiSnapshot([]*TranslogSnapshot{
		gen1.Snapshot(),
		gen2.Snapshot(),
		gen3.Snapshot(),
	})
	defer ms.Close()

	got := collectOps(t, ms)
	if len(got) != 4 {
		t.Fatalf("expected 4 unique ops, got %d", len(got))
	}

	// seqNo 2 should be the delete from gen3 (newer wins).
	for _, op := range got {
		if op.SeqNo() == 2 {
			if op.OpType() != OpTypeDelete {
				t.Errorf("seqNo 2 should be delete (from gen3), got opType %d", op.OpType())
			}
		}
	}
}
