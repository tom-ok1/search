// index/delete_queue.go
package index

import (
	"sync"
	"sync/atomic"
)

// deleteNode is a node in the delete queue's linked list.
type deleteNode struct {
	field string
	term  string
	next  *deleteNode
}

// apply adds this node's delete term to the given BufferedUpdates.
func (n *deleteNode) apply(bu *BufferedUpdates, docIDUpto int) {
	bu.addTerm(n.field, n.term, docIDUpto)
}

// DeleteSlice is a per-DWPT window into the DeleteQueue.
// Nodes between head.next and tail (inclusive) are the pending deletes.
// Not thread-safe — each DWPT owns its own slice.
//
// Lucene equivalent: DocumentsWriterDeleteQueue.DeleteSlice
type DeleteSlice struct {
	sliceHead *deleteNode
	sliceTail *deleteNode
}

func (ds *DeleteSlice) isEmpty() bool {
	return ds.sliceHead == ds.sliceTail
}

// apply walks from sliceHead.next to sliceTail, calling each node's apply.
// After applying, the slice is reset (head = tail).
func (ds *DeleteSlice) apply(bu *BufferedUpdates, docIDUpto int) {
	if ds.isEmpty() {
		return
	}
	current := ds.sliceHead
	for current != ds.sliceTail {
		current = current.next
		current.apply(bu, docIDUpto)
	}
	ds.reset()
}

func (ds *DeleteSlice) reset() {
	ds.sliceHead = ds.sliceTail
}

// DeleteQueue is a non-blocking linked-list queue for delete operations.
// All deletes are appended to the tail. Each DWPT maintains a DeleteSlice
// tracking its position. The queue also has a globalSlice and
// globalBufferedUpdates for cross-segment application.
//
// Lucene equivalent: org.apache.lucene.index.DocumentsWriterDeleteQueue
type DeleteQueue struct {
	mu   sync.Mutex
	tail atomic.Pointer[deleteNode]

	globalBufferLock      sync.Mutex
	globalSlice           *DeleteSlice
	globalBufferedUpdates *BufferedUpdates
}

func newDeleteQueue() *DeleteQueue {
	sentinel := &deleteNode{}
	dq := &DeleteQueue{
		globalSlice: &DeleteSlice{
			sliceHead: sentinel,
			sliceTail: sentinel,
		},
		globalBufferedUpdates: newBufferedUpdates(),
	}
	dq.tail.Store(sentinel)
	return dq
}

// addDelete appends a delete-by-term to the queue. Thread-safe.
func (dq *DeleteQueue) addDelete(field, term string) {
	node := &deleteNode{field: field, term: term}
	dq.mu.Lock()
	dq.tail.Load().next = node
	dq.tail.Store(node)
	dq.mu.Unlock()

	dq.tryApplyGlobalSlice()
}

// newSlice creates a new DeleteSlice starting at the current tail.
func (dq *DeleteQueue) newSlice() *DeleteSlice {
	t := dq.tail.Load()
	return &DeleteSlice{sliceHead: t, sliceTail: t}
}

// updateSlice advances the slice's tail to the queue's current tail.
// Returns true if new deletes were found.
func (dq *DeleteQueue) updateSlice(slice *DeleteSlice) bool {
	currentTail := dq.tail.Load()

	if slice.sliceTail != currentTail {
		slice.sliceTail = currentTail
		return true
	}
	return false
}

// tryApplyGlobalSlice opportunistically updates the global buffer.
func (dq *DeleteQueue) tryApplyGlobalSlice() {
	if dq.globalBufferLock.TryLock() {
		defer dq.globalBufferLock.Unlock()

		currentTail := dq.tail.Load()

		if dq.globalSlice.sliceTail != currentTail {
			dq.globalSlice.sliceTail = currentTail
			dq.globalSlice.apply(dq.globalBufferedUpdates, maxDocIDUpto)
		}
	}
}

// freezeGlobalBuffer freezes the global buffer and returns FrozenBufferedUpdates.
// If callerSlice is non-nil, its tail is advanced to the current queue tail
// so the caller can subsequently apply remaining deletes with the correct docIDUpto.
// Only sliceTail is set — the caller must call slice.apply() afterward.
// Returns nil if no global deletes to freeze.
func (dq *DeleteQueue) freezeGlobalBuffer(callerSlice *DeleteSlice) *FrozenBufferedUpdates {
	dq.globalBufferLock.Lock()
	defer dq.globalBufferLock.Unlock()

	currentTail := dq.tail.Load()

	if callerSlice != nil {
		callerSlice.sliceTail = currentTail
	}

	if dq.globalSlice.sliceTail != currentTail {
		dq.globalSlice.sliceTail = currentTail
		dq.globalSlice.apply(dq.globalBufferedUpdates, maxDocIDUpto)
	}

	if dq.globalBufferedUpdates.any() {
		frozen := newFrozenBufferedUpdates(dq.globalBufferedUpdates)
		dq.globalBufferedUpdates.clear()
		return frozen
	}
	return nil
}

// anyChanges returns true if the queue has unapplied deletes.
func (dq *DeleteQueue) anyChanges() bool {
	dq.globalBufferLock.Lock()
	defer dq.globalBufferLock.Unlock()

	currentTail := dq.tail.Load()

	return dq.globalBufferedUpdates.any() || !dq.globalSlice.isEmpty() || dq.globalSlice.sliceTail != currentTail
}

const maxDocIDUpto = int(^uint(0) >> 1) // math.MaxInt
