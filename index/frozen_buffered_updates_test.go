// index/frozen_buffered_updates_test.go
package index

import "testing"

func TestFrozenBufferedUpdatesFromBufferedUpdates(t *testing.T) {
	bu := newBufferedUpdates()
	bu.addTerm("title", "java", 5)
	bu.addTerm("body", "rust", 10)

	frozen := newFrozenBufferedUpdates(bu)

	if len(frozen.deleteTerms) != 2 {
		t.Fatalf("deleteTerms: got %d, want 2", len(frozen.deleteTerms))
	}

	found := make(map[deleteTermKey]bool)
	for _, dt := range frozen.deleteTerms {
		found[deleteTermKey{Field: dt.Field, Term: dt.Term}] = true
	}
	if !found[deleteTermKey{Field: "title", Term: "java"}] {
		t.Error("missing title:java")
	}
	if !found[deleteTermKey{Field: "body", Term: "rust"}] {
		t.Error("missing body:rust")
	}
}

func TestFrozenBufferedUpdatesAny(t *testing.T) {
	bu := newBufferedUpdates()
	frozen := newFrozenBufferedUpdates(bu)
	if frozen.any() {
		t.Fatal("expected no terms")
	}

	bu.addTerm("f", "t", 1)
	frozen = newFrozenBufferedUpdates(bu)
	if !frozen.any() {
		t.Fatal("expected terms")
	}
}

func TestFrozenBufferedUpdatesImmutable(t *testing.T) {
	bu := newBufferedUpdates()
	bu.addTerm("f", "t", 1)
	frozen := newFrozenBufferedUpdates(bu)

	bu.addTerm("g", "u", 2)
	bu.clear()

	if len(frozen.deleteTerms) != 1 {
		t.Fatalf("frozen should be unaffected by source changes: got %d terms", len(frozen.deleteTerms))
	}
}
