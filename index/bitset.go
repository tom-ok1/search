package index

import (
	"fmt"
	"math/bits"
)

// Bitset is a fixed-size bit array, equivalent to Lucene's FixedBitSet.
// Bit layout matches the .del file format: byte[i/8] & (1 << (i%8)).
type Bitset struct {
	bits []byte
	size int
}

// NewBitset creates a bitset of the given size with all bits clear.
func NewBitset(size int) *Bitset {
	return &Bitset{
		bits: make([]byte, (size+7)/8),
		size: size,
	}
}

// BitsetFromBytes creates a Bitset from existing byte data.
func BitsetFromBytes(data []byte, size int) *Bitset {
	b := make([]byte, len(data))
	copy(b, data)
	return &Bitset{bits: b, size: size}
}

// Set sets the bit at position i. Panics if i is out of range.
func (b *Bitset) Set(i int) {
	if i < 0 || i >= b.size {
		panic(fmt.Sprintf("bitset: index %d out of range [0, %d)", i, b.size))
	}
	b.bits[i/8] |= 1 << uint(i%8)
}

// Get reports whether bit i is set. Returns false if i is out of range.
func (b *Bitset) Get(i int) bool {
	if i < 0 || i >= b.size {
		return false
	}
	return b.bits[i/8]&(1<<uint(i%8)) != 0
}

// Clone returns a deep copy of the bitset.
func (b *Bitset) Clone() *Bitset {
	clone := make([]byte, len(b.bits))
	copy(clone, b.bits)
	return &Bitset{bits: clone, size: b.size}
}

// Count returns the number of set bits (population count).
func (b *Bitset) Count() int {
	n := 0
	for _, v := range b.bits {
		n += bits.OnesCount8(v)
	}
	return n
}

// Size returns the number of bits in the bitset.
func (b *Bitset) Size() int {
	return b.size
}

// Bytes returns the raw byte slice for persistence.
func (b *Bitset) Bytes() []byte {
	return b.bits
}
