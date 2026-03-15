package index

import (
	"fmt"
	"math/bits"
)

const rankBlockBytes = 32 // 256 bits per block

// Bitset is a fixed-size bit array, equivalent to Lucene's FixedBitSet.
// Bit layout matches the .del file format: byte[i/8] & (1 << (i%8)).
type Bitset struct {
	bits      []byte
	size      int
	rankTable []int // rankTable[b] = popcount of bits[0 : b*rankBlockBytes]
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

// countRange returns the number of set bits in positions [from, to).
func (b *Bitset) countRange(from, to int) int {
	if from < 0 {
		from = 0
	}
	if to > b.size {
		to = b.size
	}
	if from >= to {
		return 0
	}

	startByte := from / 8
	endByte := to / 8
	startBit := from % 8
	endBit := to % 8

	// Same byte case.
	if startByte == endByte {
		mask := byte((1 << uint(endBit)) - (1 << uint(startBit)))
		return bits.OnesCount8(b.bits[startByte] & mask)
	}

	n := 0
	// Partial first byte.
	if startBit > 0 {
		n += bits.OnesCount8(b.bits[startByte] & (0xFF << uint(startBit)))
		startByte++
	}
	// Full bytes.
	for i := startByte; i < endByte; i++ {
		n += bits.OnesCount8(b.bits[i])
	}
	// Partial last byte.
	if endBit > 0 && endByte < len(b.bits) {
		n += bits.OnesCount8(b.bits[endByte] & byte((1<<uint(endBit))-1))
	}
	return n
}

// BuildRankTable precomputes a block-level prefix sum so that
// Rank(i) can answer in O(1). Must be called after all Set() calls
// are done and before calling Rank().
func (b *Bitset) BuildRankTable() {
	nBlocks := (len(b.bits) + rankBlockBytes - 1) / rankBlockBytes
	b.rankTable = make([]int, nBlocks+1)
	cumulative := 0
	for block := 0; block < nBlocks; block++ {
		b.rankTable[block] = cumulative
		start := block * rankBlockBytes
		end := start + rankBlockBytes
		if end > len(b.bits) {
			end = len(b.bits)
		}
		for _, v := range b.bits[start:end] {
			cumulative += bits.OnesCount8(v)
		}
	}
	b.rankTable[nBlocks] = cumulative
}

// Rank returns the number of set bits in positions [0, i).
// BuildRankTable must be called before using this method.
func (b *Bitset) Rank(i int) int {
	if i <= 0 {
		return 0
	}
	if i >= b.size {
		i = b.size
	}
	block := (i / 8) / rankBlockBytes
	count := b.rankTable[block]
	// Count remaining bytes in the partial block.
	byteIdx := block * rankBlockBytes
	targetByte := i / 8
	for byteIdx < targetByte {
		count += bits.OnesCount8(b.bits[byteIdx])
		byteIdx++
	}
	// Count remaining bits in the final byte.
	rem := i % 8
	if rem > 0 && targetByte < len(b.bits) {
		count += bits.OnesCount8(b.bits[targetByte] & byte((1<<uint(rem))-1))
	}
	return count
}

// Bytes returns the raw byte slice for persistence.
func (b *Bitset) Bytes() []byte {
	return b.bits
}
