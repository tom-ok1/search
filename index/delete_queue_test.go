// index/delete_queue_test.go
package index

import "testing"

func TestDeleteQueueAddAndUpdateSlice(t *testing.T) {
	dq := newDeleteQueue()
	slice := dq.newSlice()

	if !slice.isEmpty() {
		t.Fatal("expected empty slice initially")
	}

	dq.addDelete("title", "java")

	hasUpdates := dq.updateSlice(slice)
	if !hasUpdates {
		t.Fatal("expected slice to have updates")
	}

	bu := newBufferedUpdates()
	slice.apply(bu, 5)
	if bu.numTerms() != 1 {
		t.Fatalf("numTerms: got %d, want 1", bu.numTerms())
	}
	if bu.getDocIDUpto("title", "java") != 5 {
		t.Fatalf("docIDUpto: got %d, want 5", bu.getDocIDUpto("title", "java"))
	}

	if !slice.isEmpty() {
		t.Fatal("expected empty slice after apply")
	}
}

func TestDeleteQueueMultipleDeletes(t *testing.T) {
	dq := newDeleteQueue()
	slice := dq.newSlice()

	dq.addDelete("title", "java")
	dq.addDelete("body", "rust")
	dq.addDelete("title", "python")

	dq.updateSlice(slice)
	bu := newBufferedUpdates()
	slice.apply(bu, 10)

	if bu.numTerms() != 3 {
		t.Fatalf("numTerms: got %d, want 3", bu.numTerms())
	}
	if bu.getDocIDUpto("title", "java") != 10 {
		t.Error("wrong docIDUpto for title:java")
	}
	if bu.getDocIDUpto("body", "rust") != 10 {
		t.Error("wrong docIDUpto for body:rust")
	}
	if bu.getDocIDUpto("title", "python") != 10 {
		t.Error("wrong docIDUpto for title:python")
	}
}

func TestDeleteQueueTwoSlicesIndependent(t *testing.T) {
	dq := newDeleteQueue()

	sliceA := dq.newSlice()
	dq.addDelete("f", "t1")

	sliceB := dq.newSlice()
	dq.addDelete("f", "t2")

	dq.updateSlice(sliceA)
	dq.updateSlice(sliceB)

	buA := newBufferedUpdates()
	sliceA.apply(buA, 10)
	if buA.numTerms() != 2 {
		t.Fatalf("sliceA numTerms: got %d, want 2", buA.numTerms())
	}

	buB := newBufferedUpdates()
	sliceB.apply(buB, 10)
	if buB.numTerms() != 1 {
		t.Fatalf("sliceB numTerms: got %d, want 1", buB.numTerms())
	}
	if buB.getDocIDUpto("f", "t2") != 10 {
		t.Error("sliceB missing f:t2")
	}
}

func TestDeleteQueueUpdateSliceNoNewDeletes(t *testing.T) {
	dq := newDeleteQueue()
	slice := dq.newSlice()

	hasUpdates := dq.updateSlice(slice)
	if hasUpdates {
		t.Fatal("expected no updates when queue is empty")
	}
}

func TestDeleteQueueGlobalBuffer(t *testing.T) {
	dq := newDeleteQueue()

	dq.addDelete("title", "java")
	dq.addDelete("body", "rust")

	frozen := dq.freezeGlobalBuffer(nil)
	if frozen == nil {
		t.Fatal("expected non-nil FrozenBufferedUpdates")
	}
	if len(frozen.deleteTerms) != 2 {
		t.Fatalf("frozen terms: got %d, want 2", len(frozen.deleteTerms))
	}

	frozen2 := dq.freezeGlobalBuffer(nil)
	if frozen2 != nil {
		t.Fatal("expected nil when no new deletes since last freeze")
	}
}

func TestDeleteQueueFreezeAdvancesCallerSlice(t *testing.T) {
	dq := newDeleteQueue()
	slice := dq.newSlice()

	dq.addDelete("f", "t1")

	// freezeGlobalBuffer advances the caller's sliceTail (not sliceHead)
	// so the caller can subsequently apply remaining deletes with correct docIDUpto.
	frozen := dq.freezeGlobalBuffer(slice)
	if frozen == nil {
		t.Fatal("expected non-nil frozen updates")
	}

	// The slice should NOT be empty — freeze only set sliceTail, not sliceHead.
	// The caller must apply() to consume the remaining deletes.
	if slice.isEmpty() {
		t.Fatal("expected non-empty slice after freeze (only tail advanced)")
	}

	bu := newBufferedUpdates()
	slice.apply(bu, 10)
	if !bu.any() {
		t.Fatal("expected terms from applying the advanced slice")
	}
	if bu.getDocIDUpto("f", "t1") != 10 {
		t.Fatalf("docIDUpto: got %d, want 10", bu.getDocIDUpto("f", "t1"))
	}

	// After apply, slice should be empty
	if !slice.isEmpty() {
		t.Fatal("expected empty slice after apply")
	}
}

func TestDeleteQueueAnyChanges(t *testing.T) {
	dq := newDeleteQueue()
	if dq.anyChanges() {
		t.Fatal("expected no changes initially")
	}

	dq.addDelete("f", "t")
	if !dq.anyChanges() {
		t.Fatal("expected changes after addDelete")
	}

	dq.freezeGlobalBuffer(nil)
	if dq.anyChanges() {
		t.Fatal("expected no changes after freeze consumed all")
	}
}
