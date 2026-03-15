package fst

import (
	"fmt"

	"gosearch/store"
)

// Arc flag bits
const (
	bitFinalArc  byte = 0x01 // this is a "final" accept arc (no label, no target)
	bitLastArc   byte = 0x02 // last arc of this node
	bitHasOutput byte = 0x10 // arc carries an output
)

// FST is a read-only finite state transducer mapping []byte keys to uint64 outputs.
type FST struct {
	input     store.DataInput
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
	input := f.input

	input.Seek(int(nodeAddr))

	for {
		if input.Position() >= input.Length() {
			break
		}

		flags, err := input.ReadByte()
		if err != nil {
			break
		}

		if flags&bitFinalArc != 0 {
			var finalOutput uint64
			if flags&bitHasOutput != 0 {
				val, err := input.ReadUvarint()
				if err != nil {
					break
				}
				finalOutput = val
			}
			finalArc = &arcInfo{isFinal: true, output: finalOutput}
			if flags&bitLastArc != 0 {
				break
			}
			continue
		}

		label, err := input.ReadByte()
		if err != nil {
			break
		}

		var arcOutput uint64
		if flags&bitHasOutput != 0 {
			val, err := input.ReadUvarint()
			if err != nil {
				break
			}
			arcOutput = val
		}

		target, err := input.ReadUvarint()
		if err != nil {
			break
		}

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
	if f.input.Length() == 0 {
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

// FSTFromInput creates an FST from a MMapIndexInput containing the serialized FST.
//
// Trailer format: [data: bytes][startNode: int64][dataLen: uint32]
// The trailer (12 bytes) is at the end of the file.
func FSTFromInput(input *store.MMapIndexInput) (*FST, error) {
	totalLen := input.Length()
	if totalLen < 12 {
		return nil, fmt.Errorf("FST input too short: %d", totalLen)
	}
	// Read trailer from end of file.
	dataLen, err := input.ReadUint32At(totalLen - 4)
	if err != nil {
		return nil, err
	}
	startNode, err := input.ReadUint64At(totalLen - 12)
	if err != nil {
		return nil, err
	}
	if int(dataLen)+12 > totalLen {
		return nil, fmt.Errorf("FST data truncated: need %d, have %d", int(dataLen)+12, totalLen)
	}
	dataInput, err := input.Slice(0, int(dataLen))
	if err != nil {
		return nil, err
	}
	return &FST{
		input:     dataInput,
		startNode: int64(startNode),
	}, nil
}
