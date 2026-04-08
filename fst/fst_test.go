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

func TestJapaneseLookup(t *testing.T) {
	b := newTestBuilder()
	// Japanese keys in lexicographic byte order (UTF-8)
	keys := []string{"名古屋", "大阪", "東京"}
	// Sort by raw bytes to ensure correct insertion order
	for i, k := range keys {
		if i > 0 && k <= keys[i-1] {
			t.Fatalf("test keys not in byte order: %q <= %q", k, keys[i-1])
		}
	}
	for i, k := range keys {
		if err := b.Add([]byte(k), uint64(i)); err != nil {
			t.Fatalf("Add(%q): %v", k, err)
		}
	}

	f := buildFST(t, b)

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

	// Non-existent Japanese keys
	for _, k := range []string{"京都", "福岡", "札幌"} {
		_, ok := f.Get([]byte(k))
		if ok {
			t.Errorf("Get(%q): should not exist", k)
		}
	}
}

func TestJapaneseSharedPrefix(t *testing.T) {
	b := newTestBuilder()
	// Keys sharing the prefix "東京" (bytes: E6 9D B1 E4 BA AC)
	// "東京" < "東京タワー" < "東京都" in byte order
	keys := []string{"東京", "東京タワー", "東京都"}
	for i, k := range keys {
		if i > 0 && k <= keys[i-1] {
			t.Fatalf("test keys not in byte order: %q <= %q", k, keys[i-1])
		}
	}
	for i, k := range keys {
		if err := b.Add([]byte(k), uint64(i*10)); err != nil {
			t.Fatalf("Add(%q): %v", k, err)
		}
	}

	f := buildFST(t, b)

	for i, k := range keys {
		got, ok := f.Get([]byte(k))
		if !ok {
			t.Errorf("Get(%q): not found", k)
			continue
		}
		if got != uint64(i*10) {
			t.Errorf("Get(%q)=%d, want %d", k, got, i*10)
		}
	}

	// Partial byte sequences should not match
	_, ok := f.Get([]byte("東"))
	if ok {
		t.Error("Get(東): should not exist (partial key)")
	}
}

func TestPrefixKeys(t *testing.T) {
	b := newTestBuilder()
	// "app" is a prefix of "apple"
	keys := []string{"app", "apple", "application"}
	for i, k := range keys {
		if err := b.Add([]byte(k), uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	f := buildFST(t, b)

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
	// "appl" is not a key
	_, ok := f.Get([]byte("appl"))
	if ok {
		t.Error("Get(appl): should not exist")
	}
}

func TestSingleByteKeys(t *testing.T) {
	b := newTestBuilder()
	// Single byte keys in sorted order
	keys := []string{"a", "b", "c", "z"}
	for i, k := range keys {
		if err := b.Add([]byte(k), uint64(i*100)); err != nil {
			t.Fatal(err)
		}
	}
	f := buildFST(t, b)

	for i, k := range keys {
		got, ok := f.Get([]byte(k))
		if !ok {
			t.Errorf("Get(%q): not found", k)
			continue
		}
		if got != uint64(i*100) {
			t.Errorf("Get(%q)=%d, want %d", k, got, i*100)
		}
	}
	_, ok := f.Get([]byte("d"))
	if ok {
		t.Error("Get(d): should not exist")
	}
}

func TestKeysWithNullBytes(t *testing.T) {
	b := newTestBuilder()
	// Keys containing null bytes (0x00)
	key1 := []byte{0x00, 0x01}
	key2 := []byte{0x00, 0x02}
	key3 := []byte{0x01, 0x00}

	if err := b.Add(key1, 10); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(key2, 20); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(key3, 30); err != nil {
		t.Fatal(err)
	}
	f := buildFST(t, b)

	got, ok := f.Get(key1)
	if !ok || got != 10 {
		t.Errorf("Get(0x00 0x01)=(%d, %v), want (10, true)", got, ok)
	}
	got, ok = f.Get(key2)
	if !ok || got != 20 {
		t.Errorf("Get(0x00 0x02)=(%d, %v), want (20, true)", got, ok)
	}
	got, ok = f.Get(key3)
	if !ok || got != 30 {
		t.Errorf("Get(0x01 0x00)=(%d, %v), want (30, true)", got, ok)
	}
}

func TestKeysDifferingOnlyInLastByte(t *testing.T) {
	b := newTestBuilder()
	keys := []string{"prefixa", "prefixb", "prefixc", "prefixz"}
	for i, k := range keys {
		if err := b.Add([]byte(k), uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	f := buildFST(t, b)

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
	// "prefix" alone should not exist
	_, ok := f.Get([]byte("prefix"))
	if ok {
		t.Error("Get(prefix): should not exist")
	}
}

func TestSpecialCharKeys(t *testing.T) {
	b := newTestBuilder()
	// Keys with special characters, sorted by byte order
	keys := []string{"#tag", "@user", "hello_world", "node.js", "state-of-the-art", "user@example.com"}
	for i, k := range keys {
		if err := b.Add([]byte(k), uint64(i)); err != nil {
			t.Fatalf("Add(%q): %v", k, err)
		}
	}
	f := buildFST(t, b)

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
}

func TestEmojiKeys(t *testing.T) {
	b := newTestBuilder()
	// Emoji are multi-byte UTF-8 sequences, sorted by byte order
	keys := []string{"hello", "hello🔍", "world🔎"}
	for i, k := range keys {
		if err := b.Add([]byte(k), uint64(i)); err != nil {
			t.Fatalf("Add(%q): %v", k, err)
		}
	}
	f := buildFST(t, b)

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
	// Partial emoji bytes should not match
	_, ok := f.Get([]byte("hello\xf0"))
	if ok {
		t.Error("Get(hello + partial emoji): should not exist")
	}
}

func TestCJKExtensionBKeys(t *testing.T) {
	b := newTestBuilder()
	// 𠮷 is CJK Extension B (4 bytes in UTF-8: F0 A0 AE B7)
	// Sort by byte order
	keys := []string{"𠮷", "𠮷野家"}
	for i, k := range keys {
		if err := b.Add([]byte(k), uint64(i*10)); err != nil {
			t.Fatalf("Add(%q): %v", k, err)
		}
	}
	f := buildFST(t, b)

	for i, k := range keys {
		got, ok := f.Get([]byte(k))
		if !ok {
			t.Errorf("Get(%q): not found", k)
			continue
		}
		if got != uint64(i*10) {
			t.Errorf("Get(%q)=%d, want %d", k, got, i*10)
		}
	}
}

func TestBackslashAndQuoteKeys(t *testing.T) {
	b := newTestBuilder()
	keys := []string{"\"quoted\"", "path\\to\\file"}
	for i, k := range keys {
		if err := b.Add([]byte(k), uint64(i)); err != nil {
			t.Fatalf("Add(%q): %v", k, err)
		}
	}
	f := buildFST(t, b)

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

func TestBuilderReset(t *testing.T) {
	// Build first FST
	buf1 := &bytes.Buffer{}
	b := NewBuilderWithWriter(buf1)
	b.Add([]byte("alpha"), 1)
	b.Add([]byte("beta"), 2)
	if err := b.Finish(); err != nil {
		t.Fatal(err)
	}

	// Load first FST
	path1 := filepath.Join(t.TempDir(), "fst1.bin")
	if err := os.WriteFile(path1, buf1.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	input1, err := store.OpenMMap(path1)
	if err != nil {
		t.Fatal(err)
	}
	defer input1.Close()
	fst1, err := FSTFromInput(input1)
	if err != nil {
		t.Fatal(err)
	}

	// Reset and build second FST
	buf2 := &bytes.Buffer{}
	b.Reset(buf2)
	b.Add([]byte("delta"), 3)
	b.Add([]byte("gamma"), 4)
	if err := b.Finish(); err != nil {
		t.Fatal(err)
	}

	// Load second FST
	path2 := filepath.Join(t.TempDir(), "fst2.bin")
	if err := os.WriteFile(path2, buf2.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	input2, err := store.OpenMMap(path2)
	if err != nil {
		t.Fatal(err)
	}
	defer input2.Close()
	fst2, err := FSTFromInput(input2)
	if err != nil {
		t.Fatal(err)
	}

	// Verify first FST
	if v, ok := fst1.Get([]byte("alpha")); !ok || v != 1 {
		t.Errorf("fst1.Get(alpha) = %d, %v; want 1, true", v, ok)
	}
	if v, ok := fst1.Get([]byte("beta")); !ok || v != 2 {
		t.Errorf("fst1.Get(beta) = %d, %v; want 2, true", v, ok)
	}
	if _, ok := fst1.Get([]byte("gamma")); ok {
		t.Error("fst1 should not contain gamma")
	}

	// Verify second FST
	if v, ok := fst2.Get([]byte("delta")); !ok || v != 3 {
		t.Errorf("fst2.Get(delta) = %d, %v; want 3, true", v, ok)
	}
	if v, ok := fst2.Get([]byte("gamma")); !ok || v != 4 {
		t.Errorf("fst2.Get(gamma) = %d, %v; want 4, true", v, ok)
	}
	if _, ok := fst2.Get([]byte("alpha")); ok {
		t.Error("fst2 should not contain alpha")
	}
}
