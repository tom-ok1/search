package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
)

// MMapIndexInput provides random-access reads over a memory-mapped file.
// This is GoSearch's equivalent of Lucene's MMapDirectory + MemorySegmentIndexInput.
type MMapIndexInput struct {
	data   []byte // mmap'd byte slice
	pos    int    // current sequential read position
	length int    // total length
	owner  bool   // true if this instance owns the mmap (responsible for munmap)
}

// OpenMMap memory-maps the file at the given path for read-only access.
func OpenMMap(path string) (*MMapIndexInput, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := int(fi.Size())
	if size == 0 {
		return &MMapIndexInput{data: nil, length: 0, owner: false}, nil
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, fmt.Errorf("mmap failed: %w", err)
	}

	return &MMapIndexInput{data: data, length: size, owner: true}, nil
}

// --- Sequential read methods ---

// ReadByte reads a single byte and advances the position.
func (m *MMapIndexInput) ReadByte() (byte, error) {
	if m.pos >= m.length {
		return 0, fmt.Errorf("read past end of mmap: pos=%d, length=%d", m.pos, m.length)
	}
	b := m.data[m.pos]
	m.pos++
	return b, nil
}

// ReadBytes reads n bytes sequentially and advances the position.
func (m *MMapIndexInput) ReadBytes(n int) ([]byte, error) {
	if m.pos+n > m.length {
		return nil, fmt.Errorf("read past end of mmap: pos=%d, n=%d, length=%d", m.pos, n, m.length)
	}
	buf := make([]byte, n)
	copy(buf, m.data[m.pos:m.pos+n])
	m.pos += n
	return buf, nil
}

// ReadVInt reads a variable-length encoded unsigned integer (same encoding as FSDirectory).
func (m *MMapIndexInput) ReadVInt() (int, error) {
	val, n := binary.Uvarint(m.data[m.pos:])
	if n <= 0 {
		return 0, fmt.Errorf("invalid varint at pos %d", m.pos)
	}
	m.pos += n
	return int(val), nil
}

// ReadUvarint reads a variable-length encoded uint64 and advances the position.
func (m *MMapIndexInput) ReadUvarint() (uint64, error) {
	val, n := binary.Uvarint(m.data[m.pos:])
	if n <= 0 {
		return 0, fmt.Errorf("invalid uvarint at pos %d", m.pos)
	}
	m.pos += n
	return val, nil
}

// ReadUint16 reads a little-endian uint16 and advances the position.
func (m *MMapIndexInput) ReadUint16() (uint16, error) {
	if m.pos+2 > m.length {
		return 0, fmt.Errorf("read past end of mmap")
	}
	v := binary.LittleEndian.Uint16(m.data[m.pos:])
	m.pos += 2
	return v, nil
}

// ReadUint32 reads a little-endian uint32 and advances the position.
func (m *MMapIndexInput) ReadUint32() (uint32, error) {
	if m.pos+4 > m.length {
		return 0, fmt.Errorf("read past end of mmap")
	}
	v := binary.LittleEndian.Uint32(m.data[m.pos:])
	m.pos += 4
	return v, nil
}

// ReadUint64 reads a little-endian uint64 and advances the position.
func (m *MMapIndexInput) ReadUint64() (uint64, error) {
	if m.pos+8 > m.length {
		return 0, fmt.Errorf("read past end of mmap")
	}
	v := binary.LittleEndian.Uint64(m.data[m.pos:])
	m.pos += 8
	return v, nil
}

// --- Random access methods ---

// ReadByteAt reads a single byte at the given offset without changing position.
func (m *MMapIndexInput) ReadByteAt(offset int) (byte, error) {
	if offset < 0 || offset >= m.length {
		return 0, fmt.Errorf("read past end of mmap: offset=%d, length=%d", offset, m.length)
	}
	return m.data[offset], nil
}

// ReadUint32At reads a little-endian uint32 at the given byte offset without changing position.
func (m *MMapIndexInput) ReadUint32At(offset int) (uint32, error) {
	if offset+4 > m.length {
		return 0, fmt.Errorf("read past end of mmap: offset=%d, length=%d", offset, m.length)
	}
	return binary.LittleEndian.Uint32(m.data[offset:]), nil
}

// ReadUint64At reads a little-endian uint64 at the given byte offset without changing position.
func (m *MMapIndexInput) ReadUint64At(offset int) (uint64, error) {
	if offset+8 > m.length {
		return 0, fmt.Errorf("read past end of mmap: offset=%d, length=%d", offset, m.length)
	}
	return binary.LittleEndian.Uint64(m.data[offset:]), nil
}

// --- Position control ---

// Seek sets the sequential read position. Panics if pos is negative or beyond length.
func (m *MMapIndexInput) Seek(pos int) {
	if pos < 0 || pos > m.length {
		panic(fmt.Sprintf("mmap: seek position %d out of range [0, %d]", pos, m.length))
	}
	m.pos = pos
}

// Position returns the current sequential read position.
func (m *MMapIndexInput) Position() int {
	return m.pos
}

// Length returns the total size of the mapped data.
func (m *MMapIndexInput) Length() int {
	return m.length
}

// --- Slicing ---

// Slice creates a sub-view of this MMapIndexInput without copying data.
// The returned slice does not own the underlying mmap.
func (m *MMapIndexInput) Slice(offset, length int) (*MMapIndexInput, error) {
	if offset < 0 || length < 0 || offset+length > m.length {
		return nil, fmt.Errorf("slice out of bounds: offset=%d, length=%d, total=%d", offset, length, m.length)
	}
	return &MMapIndexInput{
		data:   m.data[offset : offset+length],
		pos:    0,
		length: length,
		owner:  false,
	}, nil
}

// Clone creates a new MMapIndexInput sharing the same underlying data
// but with an independent read position. The clone does not own the mmap.
func (m *MMapIndexInput) Clone() DataInput {
	return &MMapIndexInput{
		data:   m.data,
		pos:    0,
		length: m.length,
		owner:  false,
	}
}

// --- Cleanup ---

// Close unmaps the memory-mapped file (only if this instance owns it).
func (m *MMapIndexInput) Close() error {
	if m.owner && m.data != nil {
		err := syscall.Munmap(m.data)
		m.data = nil
		return err
	}
	return nil
}
