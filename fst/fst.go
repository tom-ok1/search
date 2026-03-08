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

// arcInfo describes a single arc in a node.
type arcInfo struct {
	label   byte
	output  uint64
	target  int64
	isFinal bool
}

// readArcsAt reads all arcs at the given node address.
// Final arcs are placed first so that iterators emit shorter keys before longer ones.
func (f *FST) readArcsAt(nodeAddr int64) []arcInfo {
	var regular []arcInfo
	var finalArc *arcInfo
	pos := int(nodeAddr)
	data := f.data

	for {
		if pos >= len(data) {
			break
		}

		flags := data[pos]
		pos++

		if flags&bitFinalArc != 0 {
			var finalOutput uint64
			if flags&bitHasOutput != 0 {
				val, n := binary.Uvarint(data[pos:])
				finalOutput = val
				pos += n
			}
			finalArc = &arcInfo{isFinal: true, output: finalOutput}
			if flags&bitLastArc != 0 {
				break
			}
			continue
		}

		label := data[pos]
		pos++

		var arcOutput uint64
		if flags&bitHasOutput != 0 {
			val, n := binary.Uvarint(data[pos:])
			arcOutput = val
			pos += n
		}

		target, n := binary.Uvarint(data[pos:])
		pos += n

		regular = append(regular, arcInfo{
			label:  label,
			output: arcOutput,
			target: int64(target),
		})

		if flags&bitLastArc != 0 {
			break
		}
	}

	// Final arc first, then regular arcs in label order
	var arcs []arcInfo
	if finalArc != nil {
		arcs = append(arcs, *finalArc)
	}
	arcs = append(arcs, regular...)
	return arcs
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
		arcs := f.readArcsAt(nodeAddr)
		found := false
		for _, arc := range arcs {
			if !arc.isFinal && arc.label == b {
				output = outputAdd(output, arc.output)
				nodeAddr = arc.target
				found = true
				break
			}
		}
		if !found {
			return 0, false
		}
	}

	// Check if current node is final (has a final arc)
	arcs := f.readArcsAt(nodeAddr)
	for _, arc := range arcs {
		if arc.isFinal {
			output = outputAdd(output, arc.output)
			return output, true
		}
	}
	return 0, false
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
