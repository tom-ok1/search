package index

import (
	"testing"
)

func TestMurmur3Hash(t *testing.T) {
	// Reference values from Lucene's StringHelper.murmurhash3_x86_32 with seed=0.
	// These match Elasticsearch's Murmur3HashFunction.hash().
	tests := []struct {
		input string
		want  int32
	}{
		{"", 0},
		{"test", -1167338989},
		{"hello", 613153351},
		{"doc1", -657533388},
		{"doc2", -1895630836},
		{"abc", -1277324294},
		{"elasticsearch", 1171729715},
		{"0", -764297089},
		{"1", -1810453357},
		{"12345", 329585043},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := Murmur3Hash([]byte(tt.input), 0)
			if got != tt.want {
				t.Errorf("Murmur3Hash(%q, 0) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestMurmur3Hash_Deterministic(t *testing.T) {
	data := []byte("consistency-check")
	h1 := Murmur3Hash(data, 0)
	h2 := Murmur3Hash(data, 0)
	if h1 != h2 {
		t.Fatalf("non-deterministic: %d != %d", h1, h2)
	}
}

func TestMurmur3Hash_DifferentSeeds(t *testing.T) {
	data := []byte("test")
	h0 := Murmur3Hash(data, 0)
	h1 := Murmur3Hash(data, 42)
	if h0 == h1 {
		t.Fatal("different seeds should produce different hashes")
	}
}
