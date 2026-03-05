package store

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func writeTempFile(t *testing.T, dir string, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestMMapReadByte(t *testing.T) {
	dir := t.TempDir()
	path := writeTempFile(t, dir, "test.bin", []byte{0x01, 0x02, 0x03})

	m, err := OpenMMap(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	if m.Length() != 3 {
		t.Fatalf("expected length 3, got %d", m.Length())
	}

	b, err := m.ReadByte()
	if err != nil || b != 0x01 {
		t.Errorf("expected 0x01, got 0x%02x, err=%v", b, err)
	}
	b, err = m.ReadByte()
	if err != nil || b != 0x02 {
		t.Errorf("expected 0x02, got 0x%02x, err=%v", b, err)
	}
	b, err = m.ReadByte()
	if err != nil || b != 0x03 {
		t.Errorf("expected 0x03, got 0x%02x, err=%v", b, err)
	}

	_, err = m.ReadByte()
	if err == nil {
		t.Error("expected error reading past end")
	}
}

func TestMMapReadUint32(t *testing.T) {
	dir := t.TempDir()

	var buf [12]byte
	binary.LittleEndian.PutUint32(buf[0:], 42)
	binary.LittleEndian.PutUint32(buf[4:], 100)
	binary.LittleEndian.PutUint32(buf[8:], 999)

	path := writeTempFile(t, dir, "test.bin", buf[:])

	m, err := OpenMMap(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// Sequential reads
	v, err := m.ReadUint32()
	if err != nil || v != 42 {
		t.Errorf("expected 42, got %d, err=%v", v, err)
	}
	v, err = m.ReadUint32()
	if err != nil || v != 100 {
		t.Errorf("expected 100, got %d, err=%v", v, err)
	}

	// Random access read (does not change position)
	v, err = m.ReadUint32At(0)
	if err != nil || v != 42 {
		t.Errorf("ReadUint32At(0): expected 42, got %d, err=%v", v, err)
	}
	v, err = m.ReadUint32At(8)
	if err != nil || v != 999 {
		t.Errorf("ReadUint32At(8): expected 999, got %d, err=%v", v, err)
	}

	// Position should still be at 8 (after two sequential uint32 reads)
	if m.Position() != 8 {
		t.Errorf("expected position 8, got %d", m.Position())
	}
}

func TestMMapReadUint64(t *testing.T) {
	dir := t.TempDir()

	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:], 12345678901234)
	binary.LittleEndian.PutUint64(buf[8:], 98765432109876)

	path := writeTempFile(t, dir, "test.bin", buf[:])

	m, err := OpenMMap(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	v, err := m.ReadUint64()
	if err != nil || v != 12345678901234 {
		t.Errorf("expected 12345678901234, got %d, err=%v", v, err)
	}

	v, err = m.ReadUint64At(8)
	if err != nil || v != 98765432109876 {
		t.Errorf("ReadUint64At(8): expected 98765432109876, got %d, err=%v", v, err)
	}
}

func TestMMapReadVInt(t *testing.T) {
	dir := t.TempDir()

	// Encode some varints
	var buf [20]byte
	n := 0
	n += binary.PutUvarint(buf[n:], 0)
	n += binary.PutUvarint(buf[n:], 127)
	n += binary.PutUvarint(buf[n:], 128)
	n += binary.PutUvarint(buf[n:], 16384)

	path := writeTempFile(t, dir, "test.bin", buf[:n])

	m, err := OpenMMap(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	expected := []int{0, 127, 128, 16384}
	for _, want := range expected {
		got, err := m.ReadVInt()
		if err != nil {
			t.Fatalf("ReadVInt error: %v", err)
		}
		if got != want {
			t.Errorf("ReadVInt: got %d, want %d", got, want)
		}
	}
}

func TestMMapSeek(t *testing.T) {
	dir := t.TempDir()
	path := writeTempFile(t, dir, "test.bin", []byte{10, 20, 30, 40, 50})

	m, err := OpenMMap(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	m.Seek(3)
	if m.Position() != 3 {
		t.Errorf("expected position 3, got %d", m.Position())
	}

	b, err := m.ReadByte()
	if err != nil || b != 40 {
		t.Errorf("expected 40 at position 3, got %d", b)
	}

	m.Seek(0)
	b, err = m.ReadByte()
	if err != nil || b != 10 {
		t.Errorf("expected 10 at position 0, got %d", b)
	}
}

func TestMMapSlice(t *testing.T) {
	dir := t.TempDir()
	path := writeTempFile(t, dir, "test.bin", []byte{10, 20, 30, 40, 50})

	m, err := OpenMMap(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	s, err := m.Slice(1, 3)
	if err != nil {
		t.Fatal(err)
	}

	if s.Length() != 3 {
		t.Errorf("slice length: expected 3, got %d", s.Length())
	}

	b, _ := s.ReadByte()
	if b != 20 {
		t.Errorf("expected 20, got %d", b)
	}
	b, _ = s.ReadByte()
	if b != 30 {
		t.Errorf("expected 30, got %d", b)
	}
	b, _ = s.ReadByte()
	if b != 40 {
		t.Errorf("expected 40, got %d", b)
	}

	// Should not affect parent position
	if m.Position() != 0 {
		t.Errorf("parent position should not change, got %d", m.Position())
	}

	// Closing slice should not munmap (not owner)
	if err := s.Close(); err != nil {
		t.Errorf("slice close error: %v", err)
	}
}

func TestMMapEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := writeTempFile(t, dir, "empty.bin", []byte{})

	m, err := OpenMMap(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	if m.Length() != 0 {
		t.Errorf("expected length 0, got %d", m.Length())
	}

	_, err = m.ReadByte()
	if err == nil {
		t.Error("expected error reading from empty mmap")
	}
}
