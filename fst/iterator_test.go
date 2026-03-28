package fst

import (
	"fmt"
	"testing"
)

func TestFSTIterator(t *testing.T) {
	// Build an FST with sorted keys and distinct outputs
	keys := []string{"ant", "app", "apple", "bat", "car"}
	outputs := []uint64{1, 2, 3, 4, 5}

	builder := newTestBuilder()
	for i, key := range keys {
		if err := builder.Add([]byte(key), outputs[i]); err != nil {
			t.Fatalf("Add(%q): %v", key, err)
		}
	}
	f := buildFST(t, builder)

	// Iterate and collect all entries
	iter := f.Iterator()
	var gotKeys []string
	var gotOutputs []uint64
	for iter.Next() {
		gotKeys = append(gotKeys, string(iter.Key()))
		gotOutputs = append(gotOutputs, iter.Value())
	}

	// Verify count
	if len(gotKeys) != len(keys) {
		t.Fatalf("got %d keys, want %d", len(gotKeys), len(keys))
	}

	// Verify order and values
	for i := range keys {
		if gotKeys[i] != keys[i] {
			t.Errorf("key[%d] = %q, want %q", i, gotKeys[i], keys[i])
		}
		if gotOutputs[i] != outputs[i] {
			t.Errorf("output[%d] = %d, want %d", i, gotOutputs[i], outputs[i])
		}
	}
}

func TestFSTIteratorSingleKey(t *testing.T) {
	builder := newTestBuilder()
	if err := builder.Add([]byte("hello"), 42); err != nil {
		t.Fatal(err)
	}
	f := buildFST(t, builder)

	iter := f.Iterator()
	if !iter.Next() {
		t.Fatal("expected at least one entry")
	}
	if got := string(iter.Key()); got != "hello" {
		t.Errorf("key = %q, want %q", got, "hello")
	}
	if got := iter.Value(); got != 42 {
		t.Errorf("value = %d, want %d", got, 42)
	}
	if iter.Next() {
		t.Error("expected no more entries")
	}
}

func TestFSTIteratorSequentialOutputs(t *testing.T) {
	// This mirrors the typical usage: ordinal-based outputs (0, 1, 2, ...)
	keys := []string{"a", "b", "c", "d", "e"}

	builder := newTestBuilder()
	for i, key := range keys {
		if err := builder.Add([]byte(key), uint64(i)); err != nil {
			t.Fatalf("Add(%q): %v", key, err)
		}
	}
	f := buildFST(t, builder)

	iter := f.Iterator()
	i := 0
	for iter.Next() {
		if got := string(iter.Key()); got != keys[i] {
			t.Errorf("key[%d] = %q, want %q", i, got, keys[i])
		}
		if got := iter.Value(); got != uint64(i) {
			t.Errorf("output[%d] = %d, want %d", i, got, i)
		}
		i++
	}
	if i != len(keys) {
		t.Errorf("iterated %d keys, want %d", i, len(keys))
	}
}

func TestFSTIteratorSharedPrefixes(t *testing.T) {
	// Keys with heavy prefix sharing
	keys := []string{"test", "testa", "testb", "testing", "tests"}
	outputs := []uint64{10, 20, 30, 40, 50}

	builder := newTestBuilder()
	for i, key := range keys {
		if err := builder.Add([]byte(key), outputs[i]); err != nil {
			t.Fatal(err)
		}
	}
	f := buildFST(t, builder)

	iter := f.Iterator()
	var gotKeys []string
	var gotOutputs []uint64
	for iter.Next() {
		gotKeys = append(gotKeys, string(iter.Key()))
		gotOutputs = append(gotOutputs, iter.Value())
	}

	if len(gotKeys) != len(keys) {
		t.Fatalf("got %d keys, want %d\ngotKeys: %v", len(gotKeys), len(keys), gotKeys)
	}

	for i := range keys {
		if gotKeys[i] != keys[i] {
			t.Errorf("key[%d] = %q, want %q", i, gotKeys[i], keys[i])
		}
		if gotOutputs[i] != outputs[i] {
			t.Errorf("output[%d] = %d, want %d", i, gotOutputs[i], outputs[i])
		}
	}
}

func TestFSTIteratorConsistentWithGet(t *testing.T) {
	sortedKeys := []string{"alpha", "beta", "delta", "gamma"}
	outputs := []uint64{100, 200, 300, 400}

	builder := newTestBuilder()
	for i, key := range sortedKeys {
		if err := builder.Add([]byte(key), outputs[i]); err != nil {
			t.Fatal(err)
		}
	}
	f := buildFST(t, builder)

	// Verify iterator results match Get() lookups
	iter := f.Iterator()
	for iter.Next() {
		key := iter.Key()
		iterVal := iter.Value()
		getVal, found := f.Get(key)
		if !found {
			t.Errorf("Get(%q) not found, but iterator yielded it", key)
			continue
		}
		if getVal != iterVal {
			t.Errorf("Get(%q) = %d, iterator = %d", key, getVal, iterVal)
		}
	}

	// Also verify Get for known keys
	for i, key := range sortedKeys {
		val, found := f.Get([]byte(key))
		if !found {
			t.Errorf("Get(%q) not found", key)
		}
		if val != outputs[i] {
			t.Errorf("Get(%q) = %d, want %d", key, val, outputs[i])
		}
	}

}

func TestFSTIteratorJapanese(t *testing.T) {
	// Japanese keys in byte-sorted order
	keys := []string{"名古屋", "大阪", "東京"}
	outputs := []uint64{10, 20, 30}

	builder := newTestBuilder()
	for i, key := range keys {
		if err := builder.Add([]byte(key), outputs[i]); err != nil {
			t.Fatalf("Add(%q): %v", key, err)
		}
	}
	f := buildFST(t, builder)

	iter := f.Iterator()
	var gotKeys []string
	var gotOutputs []uint64
	for iter.Next() {
		gotKeys = append(gotKeys, string(iter.Key()))
		gotOutputs = append(gotOutputs, iter.Value())
	}

	if len(gotKeys) != len(keys) {
		t.Fatalf("got %d keys, want %d", len(gotKeys), len(keys))
	}
	for i := range keys {
		if gotKeys[i] != keys[i] {
			t.Errorf("key[%d] = %q, want %q", i, gotKeys[i], keys[i])
		}
		if gotOutputs[i] != outputs[i] {
			t.Errorf("output[%d] = %d, want %d", i, gotOutputs[i], outputs[i])
		}
	}
}

func TestFSTIteratorJapaneseSharedPrefixes(t *testing.T) {
	keys := []string{"東京", "東京タワー", "東京都"}
	outputs := []uint64{1, 2, 3}

	builder := newTestBuilder()
	for i, key := range keys {
		if err := builder.Add([]byte(key), outputs[i]); err != nil {
			t.Fatal(err)
		}
	}
	f := buildFST(t, builder)

	iter := f.Iterator()
	var gotKeys []string
	var gotOutputs []uint64
	for iter.Next() {
		gotKeys = append(gotKeys, string(iter.Key()))
		gotOutputs = append(gotOutputs, iter.Value())
	}

	if len(gotKeys) != len(keys) {
		t.Fatalf("got %d keys, want %d\ngotKeys: %v", len(gotKeys), len(keys), gotKeys)
	}
	for i := range keys {
		if gotKeys[i] != keys[i] {
			t.Errorf("key[%d] = %q, want %q", i, gotKeys[i], keys[i])
		}
		if gotOutputs[i] != outputs[i] {
			t.Errorf("output[%d] = %d, want %d", i, gotOutputs[i], outputs[i])
		}
	}
}

func TestFSTIteratorPrefixKeys(t *testing.T) {
	// Keys where one is a prefix of another
	keys := []string{"app", "apple", "application"}
	outputs := []uint64{1, 2, 3}

	builder := newTestBuilder()
	for i, key := range keys {
		if err := builder.Add([]byte(key), outputs[i]); err != nil {
			t.Fatal(err)
		}
	}
	f := buildFST(t, builder)

	iter := f.Iterator()
	var gotKeys []string
	var gotOutputs []uint64
	for iter.Next() {
		gotKeys = append(gotKeys, string(iter.Key()))
		gotOutputs = append(gotOutputs, iter.Value())
	}

	if len(gotKeys) != len(keys) {
		t.Fatalf("got %d keys, want %d\ngotKeys: %v", len(gotKeys), len(keys), gotKeys)
	}
	for i := range keys {
		if gotKeys[i] != keys[i] {
			t.Errorf("key[%d] = %q, want %q", i, gotKeys[i], keys[i])
		}
		if gotOutputs[i] != outputs[i] {
			t.Errorf("output[%d] = %d, want %d", i, gotOutputs[i], outputs[i])
		}
	}
}

func TestFSTIteratorSingleByteKeys(t *testing.T) {
	keys := []string{"a", "b", "z"}
	outputs := []uint64{10, 20, 30}

	builder := newTestBuilder()
	for i, key := range keys {
		if err := builder.Add([]byte(key), outputs[i]); err != nil {
			t.Fatal(err)
		}
	}
	f := buildFST(t, builder)

	iter := f.Iterator()
	var gotKeys []string
	for iter.Next() {
		gotKeys = append(gotKeys, string(iter.Key()))
	}
	if len(gotKeys) != len(keys) {
		t.Fatalf("got %d keys, want %d", len(gotKeys), len(keys))
	}
	for i := range keys {
		if gotKeys[i] != keys[i] {
			t.Errorf("key[%d] = %q, want %q", i, gotKeys[i], keys[i])
		}
	}
}

func TestFSTIteratorNullByteKeys(t *testing.T) {
	builder := newTestBuilder()
	key1 := []byte{0x00, 0x01}
	key2 := []byte{0x00, 0x02}
	builder.Add(key1, 1)
	builder.Add(key2, 2)
	f := buildFST(t, builder)

	iter := f.Iterator()
	var gotKeys [][]byte
	var gotOutputs []uint64
	for iter.Next() {
		k := make([]byte, len(iter.Key()))
		copy(k, iter.Key())
		gotKeys = append(gotKeys, k)
		gotOutputs = append(gotOutputs, iter.Value())
	}
	if len(gotKeys) != 2 {
		t.Fatalf("got %d keys, want 2", len(gotKeys))
	}
	if string(gotKeys[0]) != string(key1) || gotOutputs[0] != 1 {
		t.Errorf("key[0] = %v/%d, want %v/1", gotKeys[0], gotOutputs[0], key1)
	}
	if string(gotKeys[1]) != string(key2) || gotOutputs[1] != 2 {
		t.Errorf("key[1] = %v/%d, want %v/2", gotKeys[1], gotOutputs[1], key2)
	}
}

func TestFSTIteratorSpecialCharKeys(t *testing.T) {
	keys := []string{"#tag", "@user", "node.js", "state-of-the-art"}
	outputs := []uint64{1, 2, 3, 4}

	builder := newTestBuilder()
	for i, key := range keys {
		if err := builder.Add([]byte(key), outputs[i]); err != nil {
			t.Fatal(err)
		}
	}
	f := buildFST(t, builder)

	iter := f.Iterator()
	var gotKeys []string
	for iter.Next() {
		gotKeys = append(gotKeys, string(iter.Key()))
	}
	if len(gotKeys) != len(keys) {
		t.Fatalf("got %d keys, want %d", len(gotKeys), len(keys))
	}
	for i := range keys {
		if gotKeys[i] != keys[i] {
			t.Errorf("key[%d] = %q, want %q", i, gotKeys[i], keys[i])
		}
	}
}

func TestFSTIteratorEmojiKeys(t *testing.T) {
	keys := []string{"hello", "hello🔍", "world🔎"}
	outputs := []uint64{1, 2, 3}

	builder := newTestBuilder()
	for i, key := range keys {
		if err := builder.Add([]byte(key), outputs[i]); err != nil {
			t.Fatal(err)
		}
	}
	f := buildFST(t, builder)

	iter := f.Iterator()
	var gotKeys []string
	var gotOutputs []uint64
	for iter.Next() {
		gotKeys = append(gotKeys, string(iter.Key()))
		gotOutputs = append(gotOutputs, iter.Value())
	}
	if len(gotKeys) != len(keys) {
		t.Fatalf("got %d keys, want %d", len(gotKeys), len(keys))
	}
	for i := range keys {
		if gotKeys[i] != keys[i] {
			t.Errorf("key[%d] = %q, want %q", i, gotKeys[i], keys[i])
		}
		if gotOutputs[i] != outputs[i] {
			t.Errorf("output[%d] = %d, want %d", i, gotOutputs[i], outputs[i])
		}
	}
}

func TestFSTIteratorLargeDataset(t *testing.T) {
	// Build FST with many keys to stress-test the iterator
	n := 1000
	builder := newTestBuilder()
	for i := range n {
		key := fmt.Sprintf("key_%05d", i)
		if err := builder.Add([]byte(key), uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	f := buildFST(t, builder)

	iter := f.Iterator()
	count := 0
	for iter.Next() {
		expected := fmt.Sprintf("key_%05d", count)
		if got := string(iter.Key()); got != expected {
			t.Errorf("key[%d] = %q, want %q", count, got, expected)
		}
		if got := iter.Value(); got != uint64(count) {
			t.Errorf("output[%d] = %d, want %d", count, got, count)
		}
		count++
	}
	if count != n {
		t.Errorf("iterated %d keys, want %d", count, n)
	}
}
