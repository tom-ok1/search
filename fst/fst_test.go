package fst

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"gosearch/store"
)

// buildFST is a test helper that finishes the builder and parses the result into an FST.
func buildFST(t *testing.T, b *Builder) *FST {
	t.Helper()
	if err := b.Finish(); err != nil {
		t.Fatal(err)
	}
	data := b.w.(*bytes.Buffer).Bytes()
	path := filepath.Join(t.TempDir(), "fst.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	input, err := store.OpenMMap(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { input.Close() })
	f, err := FSTFromInput(input)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// newTestBuilder creates a Builder backed by a bytes.Buffer for testing.
func newTestBuilder() *Builder {
	return NewBuilderWithWriter(&bytes.Buffer{})
}

func TestBasicLookup(t *testing.T) {
	b := newTestBuilder()
	if err := b.Add([]byte("cat"), 0); err != nil {
		t.Fatal(err)
	}
	if err := b.Add([]byte("dog"), 1); err != nil {
		t.Fatal(err)
	}
	if err := b.Add([]byte("fox"), 2); err != nil {
		t.Fatal(err)
	}

	f := buildFST(t, b)

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
	b := newTestBuilder()
	words := []string{"bar", "bars", "baz", "foo", "foobar"}
	for i, w := range words {
		if err := b.Add([]byte(w), uint64(i*10)); err != nil {
			t.Fatal(err)
		}
	}

	f := buildFST(t, b)

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
	b := newTestBuilder()
	if err := b.Add([]byte("hello"), 42); err != nil {
		t.Fatal(err)
	}

	f := buildFST(t, b)

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
	b := newTestBuilder()
	sorted := []string{"alpha", "beta", "delta", "gamma"}
	for i, k := range sorted {
		if err := b.Add([]byte(k), uint64(i)); err != nil {
			t.Fatal(err)
		}
	}

	f := buildFST(t, b)

	// Verify
	for i, k := range sorted {
		got, ok := f.Get([]byte(k))
		if !ok {
			t.Errorf("after roundtrip: Get(%q) not found", k)
			continue
		}
		if got != uint64(i) {
			t.Errorf("after roundtrip: Get(%q)=%d, want %d", k, got, i)
		}
	}
}

func TestUnsortedKeyError(t *testing.T) {
	b := newTestBuilder()
	b.Add([]byte("b"), 0)
	err := b.Add([]byte("a"), 1)
	if err == nil {
		t.Error("expected error for unsorted keys")
	}
}

func TestEmptyFSTError(t *testing.T) {
	b := newTestBuilder()
	err := b.Finish()
	if err == nil {
		t.Error("expected error for empty FST")
	}
}

func TestManyKeys(t *testing.T) {
	b := newTestBuilder()
	n := 1000
	// Generate sorted keys: "term0000", "term0001", ...
	keys := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("term%04d", i)
	}

	for i, k := range keys {
		if err := b.Add([]byte(k), uint64(i)); err != nil {
			t.Fatalf("Add(%q): %v", k, err)
		}
	}

	f := buildFST(t, b)

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
