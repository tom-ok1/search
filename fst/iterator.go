package fst

// iterFrame holds the DFS traversal state for one node.
type iterFrame struct {
	arcs   []arcInfo // cached arcs for this node (read once per node visit)
	arcIdx int       // which arc we're currently processing within this node
	output uint64    // accumulated output up to (but not including) arcs at this node
}

// FSTIterator performs a depth-first traversal of an FST, yielding all
// (key, output) pairs in lexicographic order.
type FSTIterator struct {
	fst    *FST
	stack  []iterFrame
	key    []byte
	output uint64
	done   bool
	inited bool
}

// Iterator returns a new FSTIterator that yields all entries in sorted order.
func (f *FST) Iterator() *FSTIterator {
	return &FSTIterator{
		fst: f,
	}
}

// Next advances to the next (key, output) pair.
// Returns false when all entries have been enumerated.
func (it *FSTIterator) Next() bool {
	if it.done {
		return false
	}
	if it.fst.input.Length() == 0 {
		it.done = true
		return false
	}

	if !it.inited {
		it.inited = true
		// Push the root node
		it.pushFrame(it.fst.startNode, 0)
		return it.advance()
	}

	return it.advance()
}

// Key returns the current key. Only valid after Next() returns true.
func (it *FSTIterator) Key() []byte {
	return it.key
}

// Value returns the current output value. Only valid after Next() returns true.
func (it *FSTIterator) Value() uint64 {
	return it.output
}

// pushFrame reads arcs for the given node and pushes a new frame onto the stack.
func (it *FSTIterator) pushFrame(nodeAddr int64, output uint64) {
	arcs := it.fst.readArcsAt(nodeAddr)
	n := len(it.stack)
	if n < cap(it.stack) {
		// Reuse existing frame slot to avoid allocation.
		it.stack = it.stack[:n+1]
		frame := &it.stack[n]
		frame.arcs = arcs
		frame.arcIdx = 0
		frame.output = output
	} else {
		it.stack = append(it.stack, iterFrame{
			arcs:   arcs,
			arcIdx: 0,
			output: output,
		})
	}
}

// advance performs DFS to find the next final state.
func (it *FSTIterator) advance() bool {
	for len(it.stack) > 0 {
		frame := &it.stack[len(it.stack)-1]

		if frame.arcIdx >= len(frame.arcs) {
			// Pop this frame
			it.stack = it.stack[:len(it.stack)-1]
			if len(it.key) > 0 {
				it.key = it.key[:len(it.key)-1]
			}
			continue
		}

		arc := frame.arcs[frame.arcIdx]
		frame.arcIdx++

		if arc.isFinal {
			// Final arc: emit (key, accumulated output + final output)
			it.output = outputAdd(frame.output, arc.output)
			return true
		}

		// Regular arc: push target node and continue DFS
		it.key = append(it.key, arc.label)
		accumulated := outputAdd(frame.output, arc.output)
		it.pushFrame(arc.target, accumulated)
	}

	it.done = true
	return false
}
