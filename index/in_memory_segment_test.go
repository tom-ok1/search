package index

import (
	"testing"
)

func TestInMemorySegmentMarkDeleted(t *testing.T) {
	seg := newInMemorySegment("_test")
	seg.docCount = 5

	if seg.deletedDocs[2] {
		t.Error("doc 2 should not be deleted initially")
	}

	seg.MarkDeleted(2)
	if !seg.deletedDocs[2] {
		t.Error("doc 2 should be deleted after MarkDeleted")
	}

	// Marking again is idempotent
	seg.MarkDeleted(2)
	if !seg.deletedDocs[2] {
		t.Error("doc 2 should still be deleted")
	}
}

func TestInMemorySegmentInitialState(t *testing.T) {
	seg := newInMemorySegment("_seg0")

	if seg.name != "_seg0" {
		t.Errorf("name: got %q, want %q", seg.name, "_seg0")
	}
	if seg.docCount != 0 {
		t.Errorf("docCount: got %d, want 0", seg.docCount)
	}
	if len(seg.fields) != 0 {
		t.Errorf("fields: got %d, want 0", len(seg.fields))
	}
	if len(seg.storedFields) != 0 {
		t.Errorf("storedFields: got %d, want 0", len(seg.storedFields))
	}
	if len(seg.deletedDocs) != 0 {
		t.Errorf("deletedDocs: got %d, want 0", len(seg.deletedDocs))
	}
}
