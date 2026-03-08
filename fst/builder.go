package fst

import (
	"encoding/binary"
	"fmt"
)

// Builder constructs an FST incrementally from keys added in sorted order.
// This follows the algorithm from Mihov & Maurel, as used in Lucene's FSTCompiler.
type Builder struct {
	frontier []*uncompiledNode
	buf      []byte // compiled node bytes (nodes appended sequentially)
	lastKey  []byte
	nodeMap  map[uint64]int64 // hash → address for suffix sharing
	count    int
}

type uncompiledNode struct {
	numArcs     int
	labels      []byte
	outputs     []uint64
	targets     []int64 // compiled address, -1 = not yet compiled
	isFinal     bool
	finalOutput uint64
}

func newUncompiledNode() *uncompiledNode {
	return &uncompiledNode{}
}

func (n *uncompiledNode) addArc(label byte, target int64) {
	n.labels = append(n.labels, label)
	n.outputs = append(n.outputs, noOutput)
	n.targets = append(n.targets, target)
	n.numArcs++
}

func (n *uncompiledNode) replaceLast(target int64) {
	n.targets[n.numArcs-1] = target
}

func (n *uncompiledNode) clear() {
	n.numArcs = 0
	n.labels = n.labels[:0]
	n.outputs = n.outputs[:0]
	n.targets = n.targets[:0]
	n.isFinal = false
	n.finalOutput = noOutput
}

// NewBuilder creates a new FST builder.
func NewBuilder() *Builder {
	b := &Builder{
		nodeMap: make(map[uint64]int64),
	}
	b.frontier = append(b.frontier, newUncompiledNode())
	return b
}

// Add adds a key-value pair to the FST. Keys must be added in strictly sorted order.
func (b *Builder) Add(key []byte, output uint64) error {
	if b.count > 0 {
		if compareBytes(key, b.lastKey) <= 0 {
			return fmt.Errorf("keys must be added in sorted order: %q <= %q", key, b.lastKey)
		}
	}

	prefixLen := 0
	for prefixLen < len(key) && prefixLen < len(b.lastKey) {
		if key[prefixLen] != b.lastKey[prefixLen] {
			break
		}
		prefixLen++
	}

	b.freezeTail(prefixLen + 1)

	for len(b.frontier) <= len(key) {
		b.frontier = append(b.frontier, newUncompiledNode())
	}

	for i := prefixLen; i < len(key); i++ {
		b.frontier[i+1].clear()
		b.frontier[i].addArc(key[i], -1)
	}

	b.frontier[len(key)].isFinal = true
	b.frontier[len(key)].finalOutput = noOutput

	b.pushOutput(key, output, prefixLen)

	b.lastKey = append(b.lastKey[:0], key...)
	b.count++
	return nil
}

func (b *Builder) pushOutput(key []byte, output uint64, prefixLen int) {
	for i := 0; i < prefixLen; i++ {
		arcIdx := b.frontier[i].numArcs - 1
		existingOutput := b.frontier[i].outputs[arcIdx]

		common := outputCommon(existingOutput, output)
		wordSuffix := outputSubtract(existingOutput, common)
		output = outputSubtract(output, common)

		b.frontier[i].outputs[arcIdx] = common

		if wordSuffix != noOutput {
			next := b.frontier[i+1]
			for j := 0; j < next.numArcs; j++ {
				next.outputs[j] = outputAdd(next.outputs[j], wordSuffix)
			}
			if next.isFinal {
				next.finalOutput = outputAdd(next.finalOutput, wordSuffix)
			}
		}
	}

	if prefixLen < len(key) {
		arcIdx := b.frontier[prefixLen].numArcs - 1
		b.frontier[prefixLen].outputs[arcIdx] = output
	} else {
		b.frontier[prefixLen].finalOutput = outputAdd(b.frontier[prefixLen].finalOutput, output)
	}
}

// Finish compiles remaining nodes and returns the serialized FST bytes.
// Format: [startNode: int64][dataLen: uint32][data: bytes]
func (b *Builder) Finish() ([]byte, error) {
	if b.count == 0 {
		return nil, fmt.Errorf("cannot build empty FST")
	}

	b.freezeTail(0)
	rootAddr := b.compileNode(b.frontier[0])

	out := make([]byte, 12+len(b.buf))
	binary.LittleEndian.PutUint64(out[0:8], uint64(rootAddr))
	binary.LittleEndian.PutUint32(out[8:12], uint32(len(b.buf)))
	copy(out[12:], b.buf)
	return out, nil
}

func (b *Builder) freezeTail(downTo int) {
	for i := len(b.lastKey); i >= downTo; i-- {
		if i >= len(b.frontier) {
			continue
		}
		node := b.frontier[i]
		addr := b.compileNode(node)

		if i > 0 && b.frontier[i-1].numArcs > 0 {
			b.frontier[i-1].replaceLast(addr)
		}
	}
}

func (b *Builder) compileNode(node *uncompiledNode) int64 {
	h := b.hashNode(node)
	if addr, ok := b.nodeMap[h]; ok {
		return addr
	}

	addr := b.writeNode(node)
	b.nodeMap[h] = addr
	return addr
}

func (b *Builder) hashNode(node *uncompiledNode) uint64 {
	var h uint64 = 17
	for i := 0; i < node.numArcs; i++ {
		h = h*31 + uint64(node.labels[i])
		h = h*31 + node.outputs[i]
		h = h*31 + uint64(node.targets[i])
	}
	if node.isFinal {
		h = h*31 + 1
	}
	h = h*31 + node.finalOutput
	return h
}

// writeNode serializes a node to b.buf and returns the node's address
// (the byte offset of the first arc).
//
// Arc format:
//
//	flags: 1 byte
//	label: 1 byte (absent for final arcs)
//	output: varint (only if BIT_HAS_OUTPUT)
//	target: varint (only if regular arc and not BIT_STOP_NODE)
//
// For a node with arcs and isFinal, we write all regular arcs first,
// then a final arc with BIT_FINAL_ARC | BIT_LAST_ARC.
func (b *Builder) writeNode(node *uncompiledNode) int64 {
	nodeStart := int64(len(b.buf))

	totalArcs := node.numArcs
	if node.isFinal {
		totalArcs++ // +1 for the final arc
	}

	arcIdx := 0
	for i := 0; i < node.numArcs; i++ {
		var flags byte
		arcIdx++
		if arcIdx == totalArcs {
			flags |= bitLastArc
		}

		if node.outputs[i] != noOutput {
			flags |= bitHasOutput
		}

		b.buf = append(b.buf, flags)
		b.buf = append(b.buf, node.labels[i])

		if flags&bitHasOutput != 0 {
			b.appendUvarint(node.outputs[i])
		}

		// Write target address
		b.appendUvarint(uint64(node.targets[i]))
	}

	if node.isFinal {
		var flags byte = bitFinalArc | bitLastArc
		if node.finalOutput != noOutput {
			flags |= bitHasOutput
		}
		b.buf = append(b.buf, flags)
		if flags&bitHasOutput != 0 {
			b.appendUvarint(node.finalOutput)
		}
	}

	return nodeStart
}

func (b *Builder) appendUvarint(v uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	b.buf = append(b.buf, buf[:n]...)
}

func compareBytes(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
