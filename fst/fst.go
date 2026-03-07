package fst

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Arc flag bits
const (
	bitFinalArc byte = 0x01 // this is a "final" accept arc (no label, no target)
	bitLastArc  byte = 0x02 // last arc of this node
	bitHasOutput byte = 0x10 // arc carries an output
)

// FST is a read-only finite state transducer mapping []byte keys to uint64 outputs.
type FST struct {
	data      []byte
	startNode int64
}

// Get performs an exact lookup for the given key.
// Returns the output value and true if found, or (0, false) if not found.
func (f *FST) Get(key []byte) (uint64, bool) {
	if len(f.data) == 0 {
		return 0, false
	}

	var output uint64
	nodeAddr := f.startNode

	for _, b := range key {
		found, arcOutput, target, _ := f.findArc(nodeAddr, b)
		if !found {
			return 0, false
		}
		output = outputAdd(output, arcOutput)
		nodeAddr = target
	}

	// Check if current node is final (has a final arc)
	finalOutput, isFinal := f.findFinalArc(nodeAddr)
	if !isFinal {
		return 0, false
	}
	output = outputAdd(output, finalOutput)
	return output, true
}

// findArc scans the arcs at nodeAddr for the given label.
// Returns (found, output, targetAddr, isFinal).
func (f *FST) findArc(nodeAddr int64, label byte) (bool, uint64, int64, bool) {
	pos := int(nodeAddr)

	for {
		if pos >= len(f.data) {
			return false, 0, 0, false
		}

		flags := f.data[pos]
		pos++

		if flags&bitFinalArc != 0 {
			// Final arc — skip output if present, then check if last
			if flags&bitHasOutput != 0 {
				_, n := binary.Uvarint(f.data[pos:])
				pos += n
			}
			if flags&bitLastArc != 0 {
				return false, 0, 0, false
			}
			continue
		}

		arcLabel := f.data[pos]
		pos++

		var arcOutput uint64
		if flags&bitHasOutput != 0 {
			val, n := binary.Uvarint(f.data[pos:])
			arcOutput = val
			pos += n
		}

		// Read target address
		target, n := binary.Uvarint(f.data[pos:])
		pos += n

		if arcLabel == label {
			return true, arcOutput, int64(target), false
		}

		if flags&bitLastArc != 0 {
			return false, 0, 0, false
		}
	}
}

// findFinalArc checks if the node at nodeAddr has a final arc.
// Returns (finalOutput, isFinal).
func (f *FST) findFinalArc(nodeAddr int64) (uint64, bool) {
	pos := int(nodeAddr)

	for {
		if pos >= len(f.data) {
			return 0, false
		}

		flags := f.data[pos]
		pos++

		if flags&bitFinalArc != 0 {
			var finalOutput uint64
			if flags&bitHasOutput != 0 {
				val, _ := binary.Uvarint(f.data[pos:])
				finalOutput = val
			}
			return finalOutput, true
		}

		// Skip label
		pos++

		// Skip output
		if flags&bitHasOutput != 0 {
			_, n := binary.Uvarint(f.data[pos:])
			pos += n
		}

		// Skip target
		_, n := binary.Uvarint(f.data[pos:])
		pos += n

		if flags&bitLastArc != 0 {
			return 0, false
		}
	}
}

// WriteTo serializes the FST to the given writer.
// Format: [startNode: int64][dataLen: uint32][data: bytes]
func (f *FST) WriteTo(w io.Writer) (int64, error) {
	var header [12]byte
	binary.LittleEndian.PutUint64(header[0:8], uint64(f.startNode))
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(f.data)))
	n1, err := w.Write(header[:])
	if err != nil {
		return int64(n1), err
	}
	n2, err := w.Write(f.data)
	return int64(n1 + n2), err
}

// ReadFST deserializes an FST from the given reader.
func ReadFST(r io.Reader) (*FST, error) {
	var header [12]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("read FST header: %w", err)
	}
	startNode := int64(binary.LittleEndian.Uint64(header[0:8]))
	dataLen := binary.LittleEndian.Uint32(header[8:12])

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("read FST data: %w", err)
	}

	return &FST{
		data:      data,
		startNode: startNode,
	}, nil
}

// FSTFromBytes creates an FST from raw bytes (as stored in a .tidx file).
// Format: [startNode: int64][dataLen: uint32][data: bytes]
func FSTFromBytes(b []byte) (*FST, error) {
	if len(b) < 12 {
		return nil, fmt.Errorf("FST bytes too short: %d", len(b))
	}
	startNode := int64(binary.LittleEndian.Uint64(b[0:8]))
	dataLen := int(binary.LittleEndian.Uint32(b[8:12]))
	if 12+dataLen > len(b) {
		return nil, fmt.Errorf("FST data truncated: need %d, have %d", 12+dataLen, len(b))
	}
	return &FST{
		data:      b[12 : 12+dataLen],
		startNode: startNode,
	}, nil
}

// SerializedSize returns the total size in bytes when serialized.
func (f *FST) SerializedSize() int {
	return 12 + len(f.data) // 8 (startNode) + 4 (dataLen) + data
}
