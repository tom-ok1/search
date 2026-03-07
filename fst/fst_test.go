package fst

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBasicLookup(t *testing.T) {
	b := NewBuilder()
	if err := b.Add([]byte("cat"), 0); err != nil {
		t.Fatal(err)
	}
	if err := b.Add([]byte("dog"), 1); err != nil {
		t.Fatal(err)
	}
	if err := b.Add([]byte("fox"), 2); err != nil {
		t.Fatal(err)
	}

	f, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		key    string
		want   uint64
		exists bool
	}{
		{"cat", 0, true},
		{"dog", 1, true},
		{"fox", 2, true},
		{"ca", 0, false},
		{"cats", 0, false},
		{"do", 0, false},
		{"", 0, false},
		{"zzz", 0, false},
	}

	for _, tt := range tests {
		got, ok := f.Get([]byte(tt.key))
		if ok != tt.exists {
			t.Errorf("Get(%q): exists=%v, want %v", tt.key, ok, tt.exists)
		}
		if ok && got != tt.want {
			t.Errorf("Get(%q)=%d, want %d", tt.key, got, tt.want)
		}
	}
}

func TestSharedPrefix(t *testing.T) {
	b := NewBuilder()
	words := []string{"bar", "bars", "baz", "foo", "foobar"}
	for i, w := range words {
		if err := b.Add([]byte(w), uint64(i*10)); err != nil {
			t.Fatal(err)
		}
	}

	f, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}

	for i, w := range words {
		got, ok := f.Get([]byte(w))
		if !ok {
			t.Errorf("Get(%q): not found", w)
			continue
		}
		if got != uint64(i*10) {
			t.Errorf("Get(%q)=%d, want %d", w, got, i*10)
		}
	}

	// Non-existent keys
	for _, w := range []string{"b", "ba", "baz2", "fo", "foob", "foobars"} {
		_, ok := f.Get([]byte(w))
		if ok {
			t.Errorf("Get(%q): should not exist", w)
		}
	}
}

func TestSingleKey(t *testing.T) {
	b := NewBuilder()
	if err := b.Add([]byte("hello"), 42); err != nil {
		t.Fatal(err)
	}

	f, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}

	got, ok := f.Get([]byte("hello"))
	if !ok || got != 42 {
		t.Errorf("Get(hello)=(%d, %v), want (42, true)", got, ok)
	}

	_, ok = f.Get([]byte("hell"))
	if ok {
		t.Error("Get(hell): should not exist")
	}
}

func TestRoundtrip(t *testing.T) {
	b := NewBuilder()
	keys := []string{"alpha", "beta", "gamma", "delta"}
	// Sort for insertion order
	sorted := []string{"alpha", "beta", "delta", "gamma"}
	for i, k := range sorted {
		if err := b.Add([]byte(k), uint64(i)); err != nil {
			t.Fatal(err)
		}
	}

	f, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}

	// Serialize
	var buf bytes.Buffer
	if _, err := f.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}

	// Deserialize
	f2, err := ReadFST(&buf)
	if err != nil {
		t.Fatal(err)
	}

	// Verify
	for i, k := range sorted {
		got, ok := f2.Get([]byte(k))
		if !ok {
			t.Errorf("after roundtrip: Get(%q) not found", k)
			continue
		}
		if got != uint64(i) {
			t.Errorf("after roundtrip: Get(%q)=%d, want %d", k, got, i)
		}
	}

	// Also test FSTFromBytes
	var buf2 bytes.Buffer
	f.WriteTo(&buf2) //nolint:errcheck
	f3, err := FSTFromBytes(buf2.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	for i, k := range sorted {
		got, ok := f3.Get([]byte(k))
		if !ok || got != uint64(i) {
			t.Errorf("FSTFromBytes: Get(%q)=(%d, %v), want (%d, true)", k, got, ok, i)
		}
	}
	_ = keys
}

func TestUnsortedKeyError(t *testing.T) {
	b := NewBuilder()
	b.Add([]byte("b"), 0)
	err := b.Add([]byte("a"), 1)
	if err == nil {
		t.Error("expected error for unsorted keys")
	}
}

func TestEmptyFSTError(t *testing.T) {
	b := NewBuilder()
	_, err := b.Finish()
	if err == nil {
		t.Error("expected error for empty FST")
	}
}

func TestManyKeys(t *testing.T) {
	b := NewBuilder()
	n := 1000
	// Generate sorted keys: "term0000", "term0001", ...
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("term%04d", i)
	}

	for i, k := range keys {
		if err := b.Add([]byte(k), uint64(i)); err != nil {
			t.Fatalf("Add(%q): %v", k, err)
		}
	}

	f, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}

	// Verify all keys
	for i, k := range keys {
		got, ok := f.Get([]byte(k))
		if !ok {
			t.Errorf("Get(%q): not found", k)
			continue
		}
		if got != uint64(i) {
			t.Errorf("Get(%q)=%d, want %d", k, got, i)
		}
	}

	// Verify non-existent keys
	_, ok := f.Get([]byte("term9999"))
	if ok {
		t.Error("Get(term9999): should not exist")
	}
}
