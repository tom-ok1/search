package index

import (
	"testing"
)

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
}
