package index

import "testing"

func TestLiveVersionMap_PutAndGet(t *testing.T) {
	m := NewLiveVersionMap()

	// Get on empty map returns not found.
	if _, ok := m.Get("doc1"); ok {
		t.Fatal("expected not found on empty map")
	}

	// Put and verify Get returns it.
	source := []byte(`{"title":"hello"}`)
	m.Put("doc1", VersionValue{SeqNo: 1, PrimaryTerm: 1, Source: source, Deleted: false})

	vv, ok := m.Get("doc1")
	if !ok {
		t.Fatal("expected doc1 to be found")
	}
	if vv.SeqNo != 1 {
		t.Fatalf("expected SeqNo 1, got %d", vv.SeqNo)
	}
	if string(vv.Source) != string(source) {
		t.Fatalf("expected source %q, got %q", source, vv.Source)
	}
	if vv.Deleted {
		t.Fatal("expected Deleted to be false")
	}

	// Overwrite with SeqNo 2 and verify.
	source2 := []byte(`{"title":"updated"}`)
	m.Put("doc1", VersionValue{SeqNo: 2, PrimaryTerm: 1, Source: source2, Deleted: false})

	vv, ok = m.Get("doc1")
	if !ok {
		t.Fatal("expected doc1 to be found after update")
	}
	if vv.SeqNo != 2 {
		t.Fatalf("expected SeqNo 2, got %d", vv.SeqNo)
	}
	if string(vv.Source) != string(source2) {
		t.Fatalf("expected source %q, got %q", source2, vv.Source)
	}
}

func TestLiveVersionMap_Delete(t *testing.T) {
	m := NewLiveVersionMap()

	// Put entry.
	m.Put("doc1", VersionValue{SeqNo: 1, PrimaryTerm: 1, Source: []byte(`{"x":1}`), Deleted: false})

	// Put deleted tombstone.
	m.Put("doc1", VersionValue{SeqNo: 2, PrimaryTerm: 1, Source: nil, Deleted: true})

	vv, ok := m.Get("doc1")
	if !ok {
		t.Fatal("expected doc1 to be found (tombstone)")
	}
	if vv.SeqNo != 2 {
		t.Fatalf("expected SeqNo 2, got %d", vv.SeqNo)
	}
	if !vv.Deleted {
		t.Fatal("expected Deleted to be true")
	}
	if vv.Source != nil {
		t.Fatalf("expected nil source for tombstone, got %q", vv.Source)
	}
}

func TestLiveVersionMap_SeqNoAndPrimaryTerm(t *testing.T) {
	m := NewLiveVersionMap()

	m.Put("doc1", VersionValue{
		Source:      []byte(`{"title":"hello"}`),
		Deleted:     false,
		SeqNo:       42,
		PrimaryTerm: 1,
	})

	vv, ok := m.Get("doc1")
	if !ok {
		t.Fatal("expected doc1 to be found")
	}
	if vv.SeqNo != 42 {
		t.Fatalf("expected SeqNo 42, got %d", vv.SeqNo)
	}
	if vv.PrimaryTerm != 1 {
		t.Fatalf("expected PrimaryTerm 1, got %d", vv.PrimaryTerm)
	}
}

func TestLiveVersionMap_Clear(t *testing.T) {
	m := NewLiveVersionMap()

	m.Put("doc1", VersionValue{SeqNo: 1, PrimaryTerm: 1, Source: []byte(`{"a":1}`), Deleted: false})
	m.Put("doc2", VersionValue{SeqNo: 3, PrimaryTerm: 1, Source: []byte(`{"b":2}`), Deleted: false})

	m.Clear()

	if _, ok := m.Get("doc1"); ok {
		t.Fatal("expected doc1 not found after Clear")
	}
	if _, ok := m.Get("doc2"); ok {
		t.Fatal("expected doc2 not found after Clear")
	}
}
