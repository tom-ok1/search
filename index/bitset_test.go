package index

import "testing"

func TestNewBitset(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{"size 0", 0},
		{"size 1", 1},
		{"size 8 (byte boundary)", 8},
		{"size 9 (cross byte boundary)", 9},
		{"size 16", 16},
		{"size 100", 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bs := NewBitset(tt.size)
			if bs.Size() != tt.size {
				t.Errorf("Size: got %d, want %d", bs.Size(), tt.size)
			}
			if bs.Count() != 0 {
				t.Errorf("Count: got %d, want 0 for new bitset", bs.Count())
			}
			expectedBytes := (tt.size + 7) / 8
			if len(bs.Bytes()) != expectedBytes {
				t.Errorf("Bytes length: got %d, want %d", len(bs.Bytes()), expectedBytes)
			}
		})
	}
}

func TestBitsetSetAndGet(t *testing.T) {
	bs := NewBitset(16)

	// All bits should be unset initially
	for i := 0; i < 16; i++ {
		if bs.Get(i) {
			t.Errorf("bit %d should be unset initially", i)
		}
	}

	// Set specific bits
	bs.Set(0)
	bs.Set(3)
	bs.Set(7)
	bs.Set(8)
	bs.Set(15)

	for i := 0; i < 16; i++ {
		expected := i == 0 || i == 3 || i == 7 || i == 8 || i == 15
		if bs.Get(i) != expected {
			t.Errorf("bit %d: got %v, want %v", i, bs.Get(i), expected)
		}
	}
}

func TestBitsetSetIdempotent(t *testing.T) {
	bs := NewBitset(8)
	bs.Set(3)
	bs.Set(3) // setting same bit again
	if bs.Count() != 1 {
		t.Errorf("Count after double-set: got %d, want 1", bs.Count())
	}
}

func TestBitsetCount(t *testing.T) {
	bs := NewBitset(32)
	if bs.Count() != 0 {
		t.Errorf("empty bitset Count: got %d, want 0", bs.Count())
	}

	bs.Set(0)
	bs.Set(7)
	bs.Set(8)
	bs.Set(15)
	bs.Set(31)
	if bs.Count() != 5 {
		t.Errorf("Count: got %d, want 5", bs.Count())
	}
}

func TestBitsetClone(t *testing.T) {
	bs := NewBitset(16)
	bs.Set(2)
	bs.Set(9)

	clone := bs.Clone()

	// Clone should have same state
	if clone.Size() != bs.Size() {
		t.Errorf("clone Size: got %d, want %d", clone.Size(), bs.Size())
	}
	if clone.Count() != bs.Count() {
		t.Errorf("clone Count: got %d, want %d", clone.Count(), bs.Count())
	}
	if !clone.Get(2) || !clone.Get(9) {
		t.Error("clone missing set bits")
	}

	// Mutations to clone should not affect original
	clone.Set(5)
	if bs.Get(5) {
		t.Error("mutation of clone affected original")
	}

	// Mutations to original should not affect clone
	bs.Set(11)
	if clone.Get(11) {
		t.Error("mutation of original affected clone")
	}
}

func TestBitsetFromBytes(t *testing.T) {
	// Build from raw bytes: byte 0 = 0b00000101 (bits 0 and 2 set)
	data := []byte{0x05, 0x00}
	bs := BitsetFromBytes(data, 16)

	if bs.Size() != 16 {
		t.Errorf("Size: got %d, want 16", bs.Size())
	}
	if !bs.Get(0) {
		t.Error("bit 0 should be set")
	}
	if bs.Get(1) {
		t.Error("bit 1 should not be set")
	}
	if !bs.Get(2) {
		t.Error("bit 2 should be set")
	}
	if bs.Count() != 2 {
		t.Errorf("Count: got %d, want 2", bs.Count())
	}

	// Ensure data is copied (not aliased)
	data[0] = 0xFF
	if bs.Get(1) {
		t.Error("BitsetFromBytes should copy data, not alias it")
	}
}

func TestBitsetBytesRoundtrip(t *testing.T) {
	bs := NewBitset(24)
	bs.Set(0)
	bs.Set(7)
	bs.Set(8)
	bs.Set(23)

	raw := bs.Bytes()
	reconstructed := BitsetFromBytes(raw, 24)

	for i := 0; i < 24; i++ {
		if bs.Get(i) != reconstructed.Get(i) {
			t.Errorf("bit %d: original=%v, reconstructed=%v", i, bs.Get(i), reconstructed.Get(i))
		}
	}
}

func TestBitsetSizeNotMultipleOf8(t *testing.T) {
	// Size 5 needs 1 byte
	bs := NewBitset(5)
	bs.Set(0)
	bs.Set(4)
	if bs.Count() != 2 {
		t.Errorf("Count: got %d, want 2", bs.Count())
	}
	if len(bs.Bytes()) != 1 {
		t.Errorf("Bytes length: got %d, want 1", len(bs.Bytes()))
	}
}

func TestBitsetAllBitsSet(t *testing.T) {
	size := 16
	bs := NewBitset(size)
	for i := 0; i < size; i++ {
		bs.Set(i)
	}
	if bs.Count() != size {
		t.Errorf("Count with all bits set: got %d, want %d", bs.Count(), size)
	}
}
