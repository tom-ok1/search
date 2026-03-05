package index

import "testing"

func TestSlicePostingsIterator(t *testing.T) {
	postings := []Posting{
		{DocID: 0, Freq: 2, Positions: []int{1, 4}},
		{DocID: 3, Freq: 1, Positions: []int{0}},
		{DocID: 7, Freq: 3, Positions: []int{2, 5, 8}},
	}

	iter := NewSlicePostingsIterator(postings)

	for i, expected := range postings {
		if !iter.Next() {
			t.Fatalf("expected Next() to return true at index %d", i)
		}
		if iter.DocID() != expected.DocID {
			t.Errorf("DocID: got %d, want %d", iter.DocID(), expected.DocID)
		}
		if iter.Freq() != expected.Freq {
			t.Errorf("Freq: got %d, want %d", iter.Freq(), expected.Freq)
		}
		positions := iter.Positions()
		if len(positions) != len(expected.Positions) {
			t.Errorf("Positions length: got %d, want %d", len(positions), len(expected.Positions))
		}
		for j, pos := range positions {
			if pos != expected.Positions[j] {
				t.Errorf("Position[%d]: got %d, want %d", j, pos, expected.Positions[j])
			}
		}
	}

	if iter.Next() {
		t.Error("expected Next() to return false after exhaustion")
	}
}

func TestSlicePostingsIteratorEmpty(t *testing.T) {
	iter := NewSlicePostingsIterator(nil)
	if iter.Next() {
		t.Error("expected Next() to return false for empty iterator")
	}
}

func TestEmptyPostingsIterator(t *testing.T) {
	iter := EmptyPostingsIterator{}
	if iter.Next() {
		t.Error("expected Next() to return false")
	}
	if iter.DocID() != -1 {
		t.Errorf("DocID: got %d, want -1", iter.DocID())
	}
	if iter.Freq() != 0 {
		t.Errorf("Freq: got %d, want 0", iter.Freq())
	}
	if iter.Positions() != nil {
		t.Error("Positions: expected nil")
	}
}

func TestSlicePostingsIteratorSinglePosting(t *testing.T) {
	postings := []Posting{
		{DocID: 42, Freq: 1, Positions: []int{7}},
	}
	iter := NewSlicePostingsIterator(postings)

	if !iter.Next() {
		t.Fatal("expected Next() to return true")
	}
	if iter.DocID() != 42 {
		t.Errorf("DocID: got %d, want 42", iter.DocID())
	}
	if iter.Freq() != 1 {
		t.Errorf("Freq: got %d, want 1", iter.Freq())
	}
	if len(iter.Positions()) != 1 || iter.Positions()[0] != 7 {
		t.Errorf("Positions: got %v, want [7]", iter.Positions())
	}
	if iter.Next() {
		t.Error("expected Next() to return false after single posting")
	}
}

func TestSlicePostingsIteratorNoPositions(t *testing.T) {
	postings := []Posting{
		{DocID: 0, Freq: 1, Positions: nil},
	}
	iter := NewSlicePostingsIterator(postings)
	if !iter.Next() {
		t.Fatal("expected Next() to return true")
	}
	if iter.Positions() != nil {
		t.Errorf("Positions: got %v, want nil", iter.Positions())
	}
}

func TestSlicePostingsIteratorMultipleCallsAfterExhaustion(t *testing.T) {
	postings := []Posting{
		{DocID: 0, Freq: 1, Positions: []int{0}},
	}
	iter := NewSlicePostingsIterator(postings)
	iter.Next() // consume the only posting

	// Multiple calls after exhaustion should all return false
	for range 3 {
		if iter.Next() {
			t.Error("expected Next() to return false after exhaustion")
		}
	}
}
