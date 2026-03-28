// index/buffered_updates_test.go
package index

import "testing"

func TestBufferedUpdatesAddTerm(t *testing.T) {
	bu := newBufferedUpdates()

	bu.addTerm("title", "java", 5)
	if bu.numTerms() != 1 {
		t.Fatalf("numTerms: got %d, want 1", bu.numTerms())
	}

	// Same term with higher docIDUpto wins
	bu.addTerm("title", "java", 10)
	if bu.numTerms() != 1 {
		t.Fatalf("numTerms: got %d, want 1", bu.numTerms())
	}
	got := bu.getDocIDUpto("title", "java")
	if got != 10 {
		t.Fatalf("docIDUpto: got %d, want 10", got)
	}

	// Same term with lower docIDUpto is ignored
	bu.addTerm("title", "java", 3)
	got = bu.getDocIDUpto("title", "java")
	if got != 10 {
		t.Fatalf("docIDUpto should remain 10, got %d", got)
	}

	// Different term
	bu.addTerm("body", "rust", 7)
	if bu.numTerms() != 2 {
		t.Fatalf("numTerms: got %d, want 2", bu.numTerms())
	}
}

func TestBufferedUpdatesAny(t *testing.T) {
	bu := newBufferedUpdates()
	if bu.any() {
		t.Fatal("expected no updates initially")
	}
	bu.addTerm("f", "t", 1)
	if !bu.any() {
		t.Fatal("expected updates after addTerm")
	}
}

func TestBufferedUpdatesClear(t *testing.T) {
	bu := newBufferedUpdates()
	bu.addTerm("f", "t", 1)
	bu.addTerm("g", "u", 2)
	bu.clear()
	if bu.any() {
		t.Fatal("expected no updates after clear")
	}
	if bu.numTerms() != 0 {
		t.Fatalf("numTerms: got %d, want 0", bu.numTerms())
	}
}

func TestBufferedUpdatesTerms(t *testing.T) {
	bu := newBufferedUpdates()
	bu.addTerm("title", "java", 5)
	bu.addTerm("body", "rust", 3)

	terms := bu.terms()
	if len(terms) != 2 {
		t.Fatalf("terms length: got %d, want 2", len(terms))
	}

	found := make(map[deleteTermKey]int)
	for _, dt := range terms {
		found[deleteTermKey{Field: dt.Field, Term: dt.Term}] = dt.DocIDUpto
	}
	if found[deleteTermKey{Field: "title", Term: "java"}] != 5 {
		t.Error("missing or wrong docIDUpto for title:java")
	}
	if found[deleteTermKey{Field: "body", Term: "rust"}] != 3 {
		t.Error("missing or wrong docIDUpto for body:rust")
	}
}
