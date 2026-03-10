package fst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const defaultNodeMapLimit = 100000

// Builder constructs an FST incrementally from keys added in sorted order.
// This follows the algorithm from Mihov & Maurel, as used in Lucene's FSTCompiler.
// Compiled node bytes are streamed directly to the provided io.Writer.
type Builder struct {
	frontier []*uncompiledNode
	w        io.Writer
	written  int64 // bytes written to w so far
	lastKey  []byte
	cache    nodeCache
	count    int
	scratch  [1]byte // reusable buffer for emitByte to avoid allocation
}

// nodeCache is a fixed-capacity FIFO cache mapping node hashes to compiled addresses.
// When full, the oldest entry is evicted via a ring buffer of keys.
type nodeCache struct {
	m      map[uint64]int64
	keys   []uint64 // ring buffer tracking insertion order
	cursor int
	limit  int
}

func newNodeCache(limit int) nodeCache {
	return nodeCache{
		m:     make(map[uint64]int64),
		keys:  make([]uint64, limit),
		limit: limit,
	}
}

func (c *nodeCache) get(h uint64) (int64, bool) {
	addr, ok := c.m[h]
	return addr, ok
}

func (c *nodeCache) put(h uint64, addr int64) {
	if _, exists := c.m[h]; exists {
		c.m[h] = addr
		return
	}
	if len(c.m) >= c.limit {
		delete(c.m, c.keys[c.cursor])
	}
	c.m[h] = addr
	c.keys[c.cursor] = h
	c.cursor = (c.cursor + 1) % c.limit
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

// NewBuilderWithWriter creates an FST builder that streams compiled node bytes
// directly to w. This avoids holding the entire FST data buffer in memory.
func NewBuilderWithWriter(w io.Writer) *Builder {
	b := &Builder{
		w:     w,
		cache: newNodeCache(defaultNodeMapLimit),
	}
	b.frontier = append(b.frontier, newUncompiledNode())
	return b
}

// Add adds a key-value pair to the FST. Keys must be added in strictly sorted order.
func (b *Builder) Add(key []byte, output uint64) error {
	if b.count > 0 {
		if bytes.Compare(key, b.lastKey) <= 0 {
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
	for i := range prefixLen {
		arcIdx := b.frontier[i].numArcs - 1
		existingOutput := b.frontier[i].outputs[arcIdx]

		common := outputCommon(existingOutput, output)
		wordSuffix := outputSubtract(existingOutput, common)
		output = outputSubtract(output, common)

		b.frontier[i].outputs[arcIdx] = common

		if wordSuffix != noOutput {
			next := b.frontier[i+1]
			for j := range next.numArcs {
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

// Finish compiles remaining nodes and writes the trailer to the writer.
//
// Trailer format: [data: bytes][startNode: int64][dataLen: uint32]
func (b *Builder) Finish() error {
	if b.count == 0 {
		return fmt.Errorf("cannot build empty FST")
	}

	b.freezeTail(0)
	rootAddr := b.compileNode(b.frontier[0])

	var trailer [12]byte
	binary.LittleEndian.PutUint64(trailer[0:8], uint64(rootAddr))
	binary.LittleEndian.PutUint32(trailer[8:12], uint32(b.written))
	if _, err := b.w.Write(trailer[:]); err != nil {
		return fmt.Errorf("fst write trailer: %w", err)
	}
	return nil
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
	if addr, ok := b.cache.get(h); ok {
		return addr
	}

	addr := b.writeNode(node)
	b.cache.put(h, addr)

	return addr
}

func (b *Builder) hashNode(node *uncompiledNode) uint64 {
	var h uint64 = 17
	for i := range node.numArcs {
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

// writeNode serializes a node to the writer and returns the node's address.
func (b *Builder) writeNode(node *uncompiledNode) int64 {
	nodeStart := b.written

	totalArcs := node.numArcs
	if node.isFinal {
		totalArcs++
	}

	arcIdx := 0
	for i := range node.numArcs {
		var flags byte
		arcIdx++
		if arcIdx == totalArcs {
			flags |= bitLastArc
		}

		if node.outputs[i] != noOutput {
			flags |= bitHasOutput
		}

		b.emitByte(flags)
		b.emitByte(node.labels[i])

		if flags&bitHasOutput != 0 {
			b.emitUvarint(node.outputs[i])
		}

		b.emitUvarint(uint64(node.targets[i]))
	}

	if node.isFinal {
		var flags byte = bitFinalArc | bitLastArc
		if node.finalOutput != noOutput {
			flags |= bitHasOutput
		}
		b.emitByte(flags)
		if flags&bitHasOutput != 0 {
			b.emitUvarint(node.finalOutput)
		}
	}

	return nodeStart
}

func (b *Builder) emitByte(v byte) {
	b.scratch[0] = v
	b.w.Write(b.scratch[:])
	b.written++
}

func (b *Builder) emitUvarint(v uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	b.w.Write(buf[:n])
	b.written += int64(n)
}
