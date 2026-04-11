package index

import "encoding/binary"

// Murmur3Hash computes MurmurHash3 x86 32-bit, matching Lucene's
// StringHelper.murmurhash3_x86_32 used by Elasticsearch for shard routing.
func Murmur3Hash(data []byte, seed int32) int32 {
	const (
		c1 = 0xcc9e2d51
		c2 = 0x1b873593
	)

	h1 := uint32(seed)
	length := len(data)
	roundedEnd := length & ^3 // round down to 4-byte block boundary

	// Body: process 4-byte blocks
	for i := 0; i < roundedEnd; i += 4 {
		k1 := binary.LittleEndian.Uint32(data[i:])
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= c2

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19)
		h1 = h1*5 + 0xe6546b64
	}

	// Tail: process remaining bytes
	var k1 uint32
	switch length & 3 {
	case 3:
		k1 ^= uint32(data[roundedEnd+2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(data[roundedEnd+1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(data[roundedEnd])
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= c2
		h1 ^= k1
	}

	// Finalization mix
	h1 ^= uint32(length)
	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return int32(h1)
}
