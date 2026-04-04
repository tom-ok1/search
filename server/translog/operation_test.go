package translog

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestIndexOperation_Roundtrip(t *testing.T) {
	original := &IndexOperation{
		ID:         "doc-1",
		Source:     []byte(`{"title":"hello","body":"world"}`),
		SequenceNo: 42,
		PrimTerm:   1,
	}

	var buf bytes.Buffer
	if err := original.Serialize(&buf); err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	got, err := ReadOperation(&buf)
	if err != nil {
		t.Fatalf("ReadOperation failed: %v", err)
	}

	idx, ok := got.(*IndexOperation)
	if !ok {
		t.Fatalf("expected *IndexOperation, got %T", got)
	}

	if idx.ID != original.ID {
		t.Errorf("ID: got %q, want %q", idx.ID, original.ID)
	}
	if !bytes.Equal(idx.Source, original.Source) {
		t.Errorf("Source: got %q, want %q", idx.Source, original.Source)
	}
	if idx.SequenceNo != original.SequenceNo {
		t.Errorf("SequenceNo: got %d, want %d", idx.SequenceNo, original.SequenceNo)
	}
	if idx.OpType() != OpTypeIndex {
		t.Errorf("OpType: got %d, want %d", idx.OpType(), OpTypeIndex)
	}
}

func TestDeleteOperation_Roundtrip(t *testing.T) {
	original := &DeleteOperation{
		ID:         "doc-2",
		SequenceNo: 7,
		PrimTerm:   1,
	}

	var buf bytes.Buffer
	if err := original.Serialize(&buf); err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	got, err := ReadOperation(&buf)
	if err != nil {
		t.Fatalf("ReadOperation failed: %v", err)
	}

	del, ok := got.(*DeleteOperation)
	if !ok {
		t.Fatalf("expected *DeleteOperation, got %T", got)
	}

	if del.ID != original.ID {
		t.Errorf("ID: got %q, want %q", del.ID, original.ID)
	}
	if del.SequenceNo != original.SequenceNo {
		t.Errorf("SequenceNo: got %d, want %d", del.SequenceNo, original.SequenceNo)
	}
	if del.OpType() != OpTypeDelete {
		t.Errorf("OpType: got %d, want %d", del.OpType(), OpTypeDelete)
	}
}

func TestReadOperation_CRCMismatch(t *testing.T) {
	original := &IndexOperation{
		ID:         "doc-1",
		Source:     []byte(`{"title":"test"}`),
		SequenceNo: 1,
		PrimTerm:   1,
	}

	var buf bytes.Buffer
	if err := original.Serialize(&buf); err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Corrupt the CRC (last 4 bytes)
	data := buf.Bytes()
	binary.LittleEndian.PutUint32(data[len(data)-4:], 0xDEADBEEF)

	_, err := ReadOperation(bytes.NewReader(data))
	if err == nil {
		t.Fatal("expected CRC mismatch error, got nil")
	}
}

func TestReadOperation_UnknownOpType(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(99) // unknown op type
	// Write seqNo and primaryTerm so those reads succeed
	binary.Write(&buf, binary.LittleEndian, int64(0))
	binary.Write(&buf, binary.LittleEndian, int64(0))

	_, err := ReadOperation(&buf)
	if err == nil {
		t.Fatal("expected error for unknown op type, got nil")
	}
}
