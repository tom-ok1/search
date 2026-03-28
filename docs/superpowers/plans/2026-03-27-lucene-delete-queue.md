# Lucene-Style Delete Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace gosearch's simple `deleteMap` + seqNo-based delete handling with Lucene's `DocumentsWriterDeleteQueue` + `DeleteSlice` + `BufferedUpdates` + `FrozenBufferedUpdates` architecture.

**Architecture:** Deletes are added to a non-blocking linked-list queue. Each DWPT maintains a `DeleteSlice` (window into the queue) and applies deletes to its local `BufferedUpdates` during `addDocument` with `docIDUpto` semantics. On flush, remaining global deletes are frozen into `FrozenBufferedUpdates` for cross-segment application. This replaces the current approach where deletes accumulate in a simple map and are applied in batch at flush time using per-document seqNo comparison.

**Tech Stack:** Go 1.23.6, standard library only (sync, sync/atomic)

**Key Lucene reference files in repo:**
- `lucene/lucene/core/src/java/org/apache/lucene/index/DocumentsWriterDeleteQueue.java`
- `lucene/lucene/core/src/java/org/apache/lucene/index/BufferedUpdates.java`
- `lucene/lucene/core/src/java/org/apache/lucene/index/FrozenBufferedUpdates.java`
- `lucene/lucene/core/src/java/org/apache/lucene/index/DocumentsWriterPerThread.java`

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `index/buffered_updates.go` | Mutable per-DWPT buffer: term → docIDUpto map |
| Create | `index/buffered_updates_test.go` | Unit tests for BufferedUpdates |
| Create | `index/delete_queue.go` | DeleteQueue (linked list), DeleteSlice, Node types |
| Create | `index/delete_queue_test.go` | Unit tests for DeleteQueue and DeleteSlice |
| Create | `index/frozen_buffered_updates.go` | Immutable frozen deletes for cross-segment application |
| Create | `index/frozen_buffered_updates_test.go` | Unit tests for FrozenBufferedUpdates |
| Modify | `index/dwpt.go` | Add deleteQueue/deleteSlice/pendingUpdates, prepareFlush, docIDUpto-based flush |
| Modify | `index/dwpt_test.go` | Update for new DWPT constructor and behavior |
| Modify | `index/dwpt_pool.go` | Pass deleteQueue to newDWPT |
| Modify | `index/flush_ticket.go` | Add FrozenBufferedUpdates to FlushTicket |
| Modify | `index/documents_writer.go` | Replace deleteMap with DeleteQueue, new doFlush flow |
| Modify | `index/writer.go` | Apply FrozenBufferedUpdates, remove old applyDeleteTerms/seqNo logic |
| Modify | `index/segment_info.go` | Remove MaxSeqNo field |

---

### Task 1: Create BufferedUpdates

Lucene equivalent: `org.apache.lucene.index.BufferedUpdates` (simplified — term deletes only, no queries or doc values updates).

This is a mutable, per-DWPT buffer that accumulates delete terms with `docIDUpto` semantics. When a delete for term T arrives with `docIDUpto=N`, it means "delete all docs in this DWPT with docID < N that match term T." If the same term is added again with a higher docIDUpto, the higher value wins.

**Files:**
- Create: `index/buffered_updates.go`
- Create: `index/buffered_updates_test.go`

- [ ] **Step 1: Write the failing test for BufferedUpdates**

```go
// index/buffered_updates_test.go
package index

import "testing"

func TestBufferedUpdatesAddTerm(t *testing.T) {
	bu := newBufferedUpdates()

	bu.addTerm("title", "java", 5)
	if bu.numTerms() != 1 {
		t.Fatalf("numTerms: got %d, want 1", bu.numTerms())
	}

	// Same term with higher docIDUpto wins
	bu.addTerm("title", "java", 10)
	if bu.numTerms() != 1 {
		t.Fatalf("numTerms: got %d, want 1", bu.numTerms())
	}
	got := bu.getDocIDUpto("title", "java")
	if got != 10 {
		t.Fatalf("docIDUpto: got %d, want 10", got)
	}

	// Same term with lower docIDUpto is ignored
	bu.addTerm("title", "java", 3)
	got = bu.getDocIDUpto("title", "java")
	if got != 10 {
		t.Fatalf("docIDUpto should remain 10, got %d", got)
	}

	// Different term
	bu.addTerm("body", "rust", 7)
	if bu.numTerms() != 2 {
		t.Fatalf("numTerms: got %d, want 2", bu.numTerms())
	}
}

func TestBufferedUpdatesAny(t *testing.T) {
	bu := newBufferedUpdates()
	if bu.any() {
		t.Fatal("expected no updates initially")
	}
	bu.addTerm("f", "t", 1)
	if !bu.any() {
		t.Fatal("expected updates after addTerm")
	}
}

func TestBufferedUpdatesClear(t *testing.T) {
	bu := newBufferedUpdates()
	bu.addTerm("f", "t", 1)
	bu.addTerm("g", "u", 2)
	bu.clear()
	if bu.any() {
		t.Fatal("expected no updates after clear")
	}
	if bu.numTerms() != 0 {
		t.Fatalf("numTerms: got %d, want 0", bu.numTerms())
	}
}

func TestBufferedUpdatesTerms(t *testing.T) {
	bu := newBufferedUpdates()
	bu.addTerm("title", "java", 5)
	bu.addTerm("body", "rust", 3)

	terms := bu.terms()
	if len(terms) != 2 {
		t.Fatalf("terms length: got %d, want 2", len(terms))
	}

	// Verify all terms are present
	found := make(map[deleteTermKey]int)
	for _, dt := range terms {
		found[deleteTermKey{Field: dt.Field, Term: dt.Term}] = dt.DocIDUpto
	}
	if found[deleteTermKey{Field: "title", Term: "java"}] != 5 {
		t.Error("missing or wrong docIDUpto for title:java")
	}
	if found[deleteTermKey{Field: "body", Term: "rust"}] != 3 {
		t.Error("missing or wrong docIDUpto for body:rust")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./index/ -run TestBufferedUpdates -v`
Expected: Compilation error — `newBufferedUpdates` undefined.

- [ ] **Step 3: Write BufferedUpdates implementation**

```go
// index/buffered_updates.go
package index

// bufferedDeleteTerm represents a delete term with its docIDUpto.
type bufferedDeleteTerm struct {
	Field     string
	Term      string
	DocIDUpto int
}

// BufferedUpdates holds buffered deletes for a single DWPT.
// Each delete term maps to a docIDUpto: the delete applies to all documents
// in this segment with docID < docIDUpto that match the term.
//
// Lucene equivalent: org.apache.lucene.index.BufferedUpdates (term deletes only)
type BufferedUpdates struct {
	deleteTerms map[deleteTermKey]int // field+term → docIDUpto
}

func newBufferedUpdates() *BufferedUpdates {
	return &BufferedUpdates{
		deleteTerms: make(map[deleteTermKey]int),
	}
}

// addTerm records a delete-by-term with the given docIDUpto.
// If the same term already exists with a lower docIDUpto, the higher value wins.
// This matches Lucene's BufferedUpdates.addTerm semantics.
func (bu *BufferedUpdates) addTerm(field, term string, docIDUpto int) {
	key := deleteTermKey{Field: field, Term: term}
	if current, ok := bu.deleteTerms[key]; ok && docIDUpto <= current {
		return
	}
	bu.deleteTerms[key] = docIDUpto
}

// getDocIDUpto returns the docIDUpto for the given term, or -1 if not found.
func (bu *BufferedUpdates) getDocIDUpto(field, term string) int {
	if v, ok := bu.deleteTerms[deleteTermKey{Field: field, Term: term}]; ok {
		return v
	}
	return -1
}

// numTerms returns the number of distinct delete terms.
func (bu *BufferedUpdates) numTerms() int {
	return len(bu.deleteTerms)
}

// any returns true if there are any buffered deletes.
func (bu *BufferedUpdates) any() bool {
	return len(bu.deleteTerms) > 0
}

// clear removes all buffered deletes.
func (bu *BufferedUpdates) clear() {
	bu.deleteTerms = make(map[deleteTermKey]int)
}

// terms returns a snapshot of all delete terms.
func (bu *BufferedUpdates) terms() []bufferedDeleteTerm {
	result := make([]bufferedDeleteTerm, 0, len(bu.deleteTerms))
	for key, docIDUpto := range bu.deleteTerms {
		result = append(result, bufferedDeleteTerm{
			Field:     key.Field,
			Term:      key.Term,
			DocIDUpto: docIDUpto,
		})
	}
	return result
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./index/ -run TestBufferedUpdates -v`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add index/buffered_updates.go index/buffered_updates_test.go
git commit -m "feat: add BufferedUpdates for per-DWPT delete term buffer with docIDUpto semantics"
```

---

### Task 2: Create DeleteQueue and DeleteSlice

Lucene equivalent: `org.apache.lucene.index.DocumentsWriterDeleteQueue` + inner `DeleteSlice` + `Node` classes.

The DeleteQueue is a non-blocking linked-list queue. Deletes are appended as nodes. Each DWPT gets a `DeleteSlice` — a window into the queue. When a DWPT calls `updateSlice`, it advances its slice tail to the current queue tail. `apply` walks the slice from head to tail and adds each delete to the DWPT's `BufferedUpdates` with the given `docIDUpto`.

The queue also has a `globalSlice` and `globalBufferedUpdates` for tracking deletes that haven't been consumed by any DWPT (for cross-segment application).

**Files:**
- Create: `index/delete_queue.go`
- Create: `index/delete_queue_test.go`

- [ ] **Step 1: Write the failing test for DeleteQueue**

```go
// index/delete_queue_test.go
package index

import "testing"

func TestDeleteQueueAddAndUpdateSlice(t *testing.T) {
	dq := newDeleteQueue()
	slice := dq.newSlice()

	// Initially empty slice
	if !slice.isEmpty() {
		t.Fatal("expected empty slice initially")
	}

	// Add a delete
	dq.addDelete("title", "java")

	// Update the slice — should now see the new delete
	hasUpdates := dq.updateSlice(slice)
	if !hasUpdates {
		t.Fatal("expected slice to have updates")
	}

	// Apply to a BufferedUpdates with docIDUpto=5
	bu := newBufferedUpdates()
	slice.apply(bu, 5)
	if bu.numTerms() != 1 {
		t.Fatalf("numTerms: got %d, want 1", bu.numTerms())
	}
	if bu.getDocIDUpto("title", "java") != 5 {
		t.Fatalf("docIDUpto: got %d, want 5", bu.getDocIDUpto("title", "java"))
	}

	// Slice should be empty after apply
	if !slice.isEmpty() {
		t.Fatal("expected empty slice after apply")
	}
}

func TestDeleteQueueMultipleDeletes(t *testing.T) {
	dq := newDeleteQueue()
	slice := dq.newSlice()

	dq.addDelete("title", "java")
	dq.addDelete("body", "rust")
	dq.addDelete("title", "python")

	dq.updateSlice(slice)
	bu := newBufferedUpdates()
	slice.apply(bu, 10)

	if bu.numTerms() != 3 {
		t.Fatalf("numTerms: got %d, want 3", bu.numTerms())
	}
	if bu.getDocIDUpto("title", "java") != 10 {
		t.Error("wrong docIDUpto for title:java")
	}
	if bu.getDocIDUpto("body", "rust") != 10 {
		t.Error("wrong docIDUpto for body:rust")
	}
	if bu.getDocIDUpto("title", "python") != 10 {
		t.Error("wrong docIDUpto for title:python")
	}
}

func TestDeleteQueueTwoSlicesIndependent(t *testing.T) {
	dq := newDeleteQueue()

	// Slice A created before first delete
	sliceA := dq.newSlice()
	dq.addDelete("f", "t1")

	// Slice B created after first delete
	sliceB := dq.newSlice()
	dq.addDelete("f", "t2")

	// Update both slices
	dq.updateSlice(sliceA)
	dq.updateSlice(sliceB)

	buA := newBufferedUpdates()
	sliceA.apply(buA, 10)
	// Slice A sees both t1 and t2
	if buA.numTerms() != 2 {
		t.Fatalf("sliceA numTerms: got %d, want 2", buA.numTerms())
	}

	buB := newBufferedUpdates()
	sliceB.apply(buB, 10)
	// Slice B only sees t2 (created after t1)
	if buB.numTerms() != 1 {
		t.Fatalf("sliceB numTerms: got %d, want 1", buB.numTerms())
	}
	if buB.getDocIDUpto("f", "t2") != 10 {
		t.Error("sliceB missing f:t2")
	}
}

func TestDeleteQueueUpdateSliceNoNewDeletes(t *testing.T) {
	dq := newDeleteQueue()
	slice := dq.newSlice()

	// No deletes added — updateSlice returns false
	hasUpdates := dq.updateSlice(slice)
	if hasUpdates {
		t.Fatal("expected no updates when queue is empty")
	}
}

func TestDeleteQueueGlobalBuffer(t *testing.T) {
	dq := newDeleteQueue()

	dq.addDelete("title", "java")
	dq.addDelete("body", "rust")

	// Freeze global buffer
	frozen := dq.freezeGlobalBuffer(nil)
	if frozen == nil {
		t.Fatal("expected non-nil FrozenBufferedUpdates")
	}
	if len(frozen.deleteTerms) != 2 {
		t.Fatalf("frozen terms: got %d, want 2", len(frozen.deleteTerms))
	}

	// Freezing again without new deletes should return nil
	frozen2 := dq.freezeGlobalBuffer(nil)
	if frozen2 != nil {
		t.Fatal("expected nil when no new deletes since last freeze")
	}
}

func TestDeleteQueueFreezeAdvancesCallerSlice(t *testing.T) {
	dq := newDeleteQueue()
	slice := dq.newSlice()

	dq.addDelete("f", "t1")

	// Freeze should advance the caller's slice to current tail
	frozen := dq.freezeGlobalBuffer(slice)
	if frozen == nil {
		t.Fatal("expected non-nil frozen updates")
	}

	// After freeze, the slice should be at the tail
	// Applying should produce nothing (head == tail)
	bu := newBufferedUpdates()
	slice.apply(bu, 10)
	if bu.any() {
		t.Fatal("expected no terms after freeze advanced the slice")
	}
}

func TestDeleteQueueAnyChanges(t *testing.T) {
	dq := newDeleteQueue()
	if dq.anyChanges() {
		t.Fatal("expected no changes initially")
	}

	dq.addDelete("f", "t")
	if !dq.anyChanges() {
		t.Fatal("expected changes after addDelete")
	}

	dq.freezeGlobalBuffer(nil)
	if dq.anyChanges() {
		t.Fatal("expected no changes after freeze consumed all")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./index/ -run TestDeleteQueue -v`
Expected: Compilation error — `newDeleteQueue` undefined.

- [ ] **Step 3: Write DeleteQueue and DeleteSlice implementation**

```go
// index/delete_queue.go
package index

import "sync"

// deleteNode is a node in the delete queue's linked list.
// Each node holds a single delete-by-term operation.
type deleteNode struct {
	field string
	term  string
	next  *deleteNode // set once, then immutable
}

// apply adds this node's delete term to the given BufferedUpdates.
func (n *deleteNode) apply(bu *BufferedUpdates, docIDUpto int) {
	bu.addTerm(n.field, n.term, docIDUpto)
}

// DeleteSlice is a per-DWPT window into the DeleteQueue.
// It tracks a head and tail pointer; nodes between head.next and tail
// (inclusive) are the pending deletes for this slice.
//
// Not thread-safe — each DWPT owns its own slice.
//
// Lucene equivalent: DocumentsWriterDeleteQueue.DeleteSlice
type DeleteSlice struct {
	sliceHead *deleteNode // sentinel — we don't apply this node
	sliceTail *deleteNode
}

// isEmpty returns true if the slice has no pending deletes.
func (ds *DeleteSlice) isEmpty() bool {
	return ds.sliceHead == ds.sliceTail
}

// apply walks from sliceHead.next to sliceTail (inclusive), calling
// each node's apply method with the given docIDUpto. After applying,
// the slice is reset (head = tail).
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

// reset makes the slice empty by setting head = tail.
func (ds *DeleteSlice) reset() {
	ds.sliceHead = ds.sliceTail
}

// DeleteQueue is a non-blocking linked-list queue for delete operations.
// All deletes (from any thread) are appended to the tail. Each DWPT
// maintains a DeleteSlice that tracks its position in the queue.
//
// The queue also maintains a globalSlice and globalBufferedUpdates for
// tracking deletes that need cross-segment application.
//
// Lucene equivalent: org.apache.lucene.index.DocumentsWriterDeleteQueue
type DeleteQueue struct {
	mu   sync.Mutex
	tail *deleteNode // current end of the linked list

	globalBufferLock      sync.Mutex
	globalSlice           *DeleteSlice
	globalBufferedUpdates *BufferedUpdates
}

// newDeleteQueue creates a new DeleteQueue with a sentinel tail node.
func newDeleteQueue() *DeleteQueue {
	sentinel := &deleteNode{} // sentinel — never applied
	return &DeleteQueue{
		tail: sentinel,
		globalSlice: &DeleteSlice{
			sliceHead: sentinel,
			sliceTail: sentinel,
		},
		globalBufferedUpdates: newBufferedUpdates(),
	}
}

// addDelete appends a delete-by-term to the queue.
// Thread-safe.
func (dq *DeleteQueue) addDelete(field, term string) {
	node := &deleteNode{field: field, term: term}
	dq.mu.Lock()
	dq.tail.next = node
	dq.tail = node
	dq.mu.Unlock()

	dq.tryApplyGlobalSlice()
}

// newSlice creates a new DeleteSlice starting at the current tail.
// The slice initially has no pending deletes (head == tail).
func (dq *DeleteQueue) newSlice() *DeleteSlice {
	dq.mu.Lock()
	t := dq.tail
	dq.mu.Unlock()
	return &DeleteSlice{sliceHead: t, sliceTail: t}
}

// updateSlice advances the slice's tail to the queue's current tail.
// Returns true if new deletes were found (i.e., the old tail != new tail).
// The caller should then call slice.apply() to consume them.
//
// Thread-safe (acquires queue lock).
func (dq *DeleteQueue) updateSlice(slice *DeleteSlice) bool {
	dq.mu.Lock()
	currentTail := dq.tail
	dq.mu.Unlock()

	if slice.sliceTail != currentTail {
		slice.sliceTail = currentTail
		return true
	}
	return false
}

// tryApplyGlobalSlice opportunistically updates the global buffer.
// Uses tryLock to avoid blocking — if the lock is held, the update
// will happen on the next call.
func (dq *DeleteQueue) tryApplyGlobalSlice() {
	if dq.globalBufferLock.TryLock() {
		defer dq.globalBufferLock.Unlock()

		dq.mu.Lock()
		currentTail := dq.tail
		dq.mu.Unlock()

		if dq.globalSlice.sliceTail != currentTail {
			dq.globalSlice.sliceTail = currentTail
			dq.globalSlice.apply(dq.globalBufferedUpdates, maxDocIDUpto)
		}
	}
}

// freezeGlobalBuffer freezes the global buffer and returns a FrozenBufferedUpdates.
// If callerSlice is non-nil, it is advanced to the current tail so that
// the caller won't re-process the same deletes.
//
// Returns nil if there are no global deletes to freeze.
//
// Thread-safe (acquires globalBufferLock).
func (dq *DeleteQueue) freezeGlobalBuffer(callerSlice *DeleteSlice) *FrozenBufferedUpdates {
	dq.globalBufferLock.Lock()
	defer dq.globalBufferLock.Unlock()

	dq.mu.Lock()
	currentTail := dq.tail
	dq.mu.Unlock()

	if callerSlice != nil {
		callerSlice.sliceTail = currentTail
	}

	// Advance global slice to current tail and apply
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
	return dq.globalBufferedUpdates.any() || !dq.globalSlice.isEmpty() || dq.globalSlice.sliceTail != dq.tail
}

// maxDocIDUpto is used when applying global deletes — they apply to
// all documents (no upper bound).
const maxDocIDUpto = int(^uint(0) >> 1) // math.MaxInt
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./index/ -run TestDeleteQueue -v`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add index/delete_queue.go index/delete_queue_test.go
git commit -m "feat: add DeleteQueue and DeleteSlice linked-list based delete management"
```

---

### Task 3: Create FrozenBufferedUpdates

Lucene equivalent: `org.apache.lucene.index.FrozenBufferedUpdates` (term deletes only).

An immutable snapshot of delete terms for cross-segment application. Created by freezing a `BufferedUpdates`. For cross-segment deletes, `docIDUpto` is not used — all matching documents in other segments are deleted.

**Files:**
- Create: `index/frozen_buffered_updates.go`
- Create: `index/frozen_buffered_updates_test.go`

- [ ] **Step 1: Write the failing test**

```go
// index/frozen_buffered_updates_test.go
package index

import "testing"

func TestFrozenBufferedUpdatesFromBufferedUpdates(t *testing.T) {
	bu := newBufferedUpdates()
	bu.addTerm("title", "java", 5)
	bu.addTerm("body", "rust", 10)

	frozen := newFrozenBufferedUpdates(bu)

	if len(frozen.deleteTerms) != 2 {
		t.Fatalf("deleteTerms: got %d, want 2", len(frozen.deleteTerms))
	}

	// Verify the frozen terms are correct
	found := make(map[deleteTermKey]bool)
	for _, dt := range frozen.deleteTerms {
		found[deleteTermKey{Field: dt.Field, Term: dt.Term}] = true
	}
	if !found[deleteTermKey{Field: "title", Term: "java"}] {
		t.Error("missing title:java")
	}
	if !found[deleteTermKey{Field: "body", Term: "rust"}] {
		t.Error("missing body:rust")
	}
}

func TestFrozenBufferedUpdatesAny(t *testing.T) {
	bu := newBufferedUpdates()
	frozen := newFrozenBufferedUpdates(bu)
	if frozen.any() {
		t.Fatal("expected no terms")
	}

	bu.addTerm("f", "t", 1)
	frozen = newFrozenBufferedUpdates(bu)
	if !frozen.any() {
		t.Fatal("expected terms")
	}
}

func TestFrozenBufferedUpdatesImmutable(t *testing.T) {
	bu := newBufferedUpdates()
	bu.addTerm("f", "t", 1)
	frozen := newFrozenBufferedUpdates(bu)

	// Modifying the original BufferedUpdates should not affect frozen
	bu.addTerm("g", "u", 2)
	bu.clear()

	if len(frozen.deleteTerms) != 1 {
		t.Fatalf("frozen should be unaffected by source changes: got %d terms", len(frozen.deleteTerms))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./index/ -run TestFrozenBufferedUpdates -v`
Expected: Compilation error — `newFrozenBufferedUpdates` undefined.

- [ ] **Step 3: Write FrozenBufferedUpdates implementation**

```go
// index/frozen_buffered_updates.go
package index

// frozenDeleteTerm is a delete term in a FrozenBufferedUpdates.
// Unlike BufferedUpdates, there is no docIDUpto — frozen deletes
// apply to all matching documents in cross-segment application.
type frozenDeleteTerm struct {
	Field string
	Term  string
}

// FrozenBufferedUpdates is an immutable snapshot of delete operations
// for cross-segment application. Created by freezing a BufferedUpdates
// (typically the global buffer in DeleteQueue).
//
// When applied to existing segments, all matching documents are deleted
// regardless of docID (since these segments were fully committed before
// the delete was issued).
//
// Lucene equivalent: org.apache.lucene.index.FrozenBufferedUpdates
type FrozenBufferedUpdates struct {
	deleteTerms []frozenDeleteTerm
}

// newFrozenBufferedUpdates creates an immutable snapshot from a BufferedUpdates.
// The snapshot is independent — subsequent changes to the source don't affect it.
func newFrozenBufferedUpdates(bu *BufferedUpdates) *FrozenBufferedUpdates {
	terms := make([]frozenDeleteTerm, 0, len(bu.deleteTerms))
	for key := range bu.deleteTerms {
		terms = append(terms, frozenDeleteTerm{
			Field: key.Field,
			Term:  key.Term,
		})
	}
	return &FrozenBufferedUpdates{deleteTerms: terms}
}

// any returns true if there are any frozen delete terms.
func (f *FrozenBufferedUpdates) any() bool {
	return len(f.deleteTerms) > 0
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./index/ -run TestFrozenBufferedUpdates -v`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add index/frozen_buffered_updates.go index/frozen_buffered_updates_test.go
git commit -m "feat: add FrozenBufferedUpdates for immutable cross-segment delete snapshots"
```

---

### Task 4: Update DWPT to use DeleteQueue

This is the core behavioral change. The DWPT now:
1. Holds a reference to the global `DeleteQueue` and its own `DeleteSlice`
2. Maintains a `pendingUpdates` (`BufferedUpdates`) for accumulated deletes
3. After each `addDocument`, calls `deleteQueue.updateSlice` to catch up with global deletes, then applies them with `docIDUpto = numDocsInRAM` (the current doc count, so the delete applies to all previously added docs but NOT the just-added one)
4. In `prepareFlush()`, freezes the global buffer and applies remaining deletes
5. In `flush()`, applies `pendingUpdates` using `docIDUpto` instead of per-document seqNo

The `docSeqNos` field is removed — it's no longer needed.

**Files:**
- Modify: `index/dwpt.go`
- Modify: `index/dwpt_test.go`

- [ ] **Step 1: Write failing tests for new DWPT behavior**

Add these tests to `index/dwpt_test.go`:

```go
func TestDWPTAppliesDeletesDuringAddDocument(t *testing.T) {
	dq := newDeleteQueue()
	dwpt := newDWPT("_seg0", newTestAnalyzer(), dq)

	doc0 := document.NewDocument()
	doc0.AddField("title", "java programming", document.FieldTypeText)
	dwpt.addDocument(doc0)

	// Delete issued after doc0 was added
	dq.addDelete("title", "java")

	doc1 := document.NewDocument()
	doc1.AddField("title", "go programming", document.FieldTypeText)
	dwpt.addDocument(doc1)

	// After adding doc1, the DWPT should have consumed the delete.
	// The delete should be in pendingUpdates with docIDUpto=1
	// (applies to doc0 but not doc1).
	if !dwpt.pendingUpdates.any() {
		t.Fatal("expected pendingUpdates to have the delete")
	}
	upto := dwpt.pendingUpdates.getDocIDUpto("title", "java")
	if upto != 1 {
		t.Fatalf("docIDUpto: got %d, want 1 (should apply to doc0 only)", upto)
	}
}

func TestDWPTDeleteBeforeAnyDoc(t *testing.T) {
	dq := newDeleteQueue()
	dwpt := newDWPT("_seg0", newTestAnalyzer(), dq)

	// Delete issued before any documents
	dq.addDelete("title", "java")

	doc0 := document.NewDocument()
	doc0.AddField("title", "java programming", document.FieldTypeText)
	dwpt.addDocument(doc0)

	// The delete was issued before doc0, so docIDUpto should be 0
	// (applies to no documents in this DWPT)
	upto := dwpt.pendingUpdates.getDocIDUpto("title", "java")
	if upto != 0 {
		t.Fatalf("docIDUpto: got %d, want 0 (delete was before any docs)", upto)
	}
}

func TestDWPTPrepareFlush(t *testing.T) {
	dq := newDeleteQueue()
	dwpt := newDWPT("_seg0", newTestAnalyzer(), dq)

	doc0 := document.NewDocument()
	doc0.AddField("title", "hello", document.FieldTypeText)
	dwpt.addDocument(doc0)

	// Add a delete after the document
	dq.addDelete("title", "hello")

	// prepareFlush should:
	// 1. Apply remaining slice to pendingUpdates
	// 2. Freeze global buffer and return FrozenBufferedUpdates
	frozen := dwpt.prepareFlush()
	if frozen == nil {
		t.Fatal("expected non-nil FrozenBufferedUpdates from prepareFlush")
	}
	if !frozen.any() {
		t.Fatal("expected frozen updates to have terms")
	}
}

func TestDWPTFlushAppliesPendingUpdates(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	dq := newDeleteQueue()
	dwpt := newDWPT("_seg0", newTestAnalyzer(), dq)

	doc0 := document.NewDocument()
	doc0.AddField("title", "java", document.FieldTypeKeyword)
	dwpt.addDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("title", "go", document.FieldTypeKeyword)
	dwpt.addDocument(doc1)

	// Delete java — issued after both docs
	dq.addDelete("title", "java")

	// Prepare and flush
	dwpt.prepareFlush()
	info, err := dwpt.flush(dir)
	if err != nil {
		t.Fatal(err)
	}

	// doc0 (java) should be deleted, doc1 (go) should survive
	if info.DelCount != 1 {
		t.Fatalf("DelCount: got %d, want 1", info.DelCount)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./index/ -run "TestDWPTAppliesDeletes|TestDWPTDeleteBefore|TestDWPTPrepareFlush|TestDWPTFlushAppliesPendingUpdates" -v`
Expected: Compilation errors — `newDWPT` signature changed, `pendingUpdates` field not found.

- [ ] **Step 3: Update DWPT implementation**

Modify `index/dwpt.go`:

1. **Change the struct** — remove `docSeqNos`, add `deleteQueue`, `deleteSlice`, `pendingUpdates`:

```go
type DocumentsWriterPerThread struct {
	segment        *InMemorySegment
	analyzer       *analysis.Analyzer
	bytesUsed      int64
	flushPending   bool
	deleteQueue    *DeleteQueue
	deleteSlice    *DeleteSlice
	pendingUpdates *BufferedUpdates
}
```

2. **Change constructor** — accept `DeleteQueue`, create slice and pendingUpdates:

```go
func newDWPT(segmentName string, analyzer *analysis.Analyzer, deleteQueue *DeleteQueue) *DocumentsWriterPerThread {
	return &DocumentsWriterPerThread{
		segment:        newInMemorySegment(segmentName),
		analyzer:       analyzer,
		deleteQueue:    deleteQueue,
		deleteSlice:    deleteQueue.newSlice(),
		pendingUpdates: newBufferedUpdates(),
	}
}
```

3. **Change `addDocument`** — remove `seqNo` parameter, update delete slice after adding:

```go
func (dwpt *DocumentsWriterPerThread) addDocument(doc *document.Document) (int64, error) {
	seg := dwpt.segment
	docID := seg.docCount
	seg.docCount++

	var bytesAdded int64
	// ... (existing field indexing code — unchanged) ...

	dwpt.bytesUsed += bytesAdded

	// After adding the document, catch up with global deletes.
	// docIDUpto = docID means "apply to all docs with ID < docID"
	// i.e., all docs added before this one, NOT this doc itself.
	if dwpt.deleteQueue.updateSlice(dwpt.deleteSlice) {
		dwpt.deleteSlice.apply(dwpt.pendingUpdates, docID)
	} else {
		dwpt.deleteSlice.reset()
	}

	return bytesAdded, nil
}
```

4. **Add `prepareFlush()`** — freeze global buffer and apply remaining slice:

```go
// prepareFlush freezes the global delete buffer and applies all remaining
// deletes in this DWPT's slice to pendingUpdates.
// Returns FrozenBufferedUpdates for cross-segment application (may be nil).
//
// Lucene equivalent: DocumentsWriterPerThread.prepareFlush()
func (dwpt *DocumentsWriterPerThread) prepareFlush() *FrozenBufferedUpdates {
	numDocsInRAM := dwpt.segment.docCount
	globalUpdates := dwpt.deleteQueue.freezeGlobalBuffer(dwpt.deleteSlice)
	dwpt.deleteSlice.apply(dwpt.pendingUpdates, numDocsInRAM)
	return globalUpdates
}
```

5. **Change `flush()`** — use `pendingUpdates` with `docIDUpto` instead of `deleteTerms`/`seqNo`:

```go
func (dwpt *DocumentsWriterPerThread) flush(dir store.Directory) (*SegmentCommitInfo, error) {
	seg := dwpt.segment
	if seg.docCount == 0 {
		return nil, nil
	}

	// Apply buffered delete terms to the in-memory segment.
	// A delete term applies to all documents with docID < docIDUpto.
	for _, dt := range dwpt.pendingUpdates.terms() {
		fi, exists := seg.fields[dt.Field]
		if !exists {
			continue
		}
		pl, exists := fi.postings[dt.Term]
		if !exists {
			continue
		}
		for _, posting := range pl.Postings {
			if posting.DocID < dt.DocIDUpto {
				seg.MarkDeleted(posting.DocID)
			}
		}
	}
	// Clear delete terms — they've been applied to this segment
	dwpt.pendingUpdates.clear()

	files, fields, err := WriteSegmentV2(dir, seg)
	if err != nil {
		return nil, fmt.Errorf("flush segment %s: %w", seg.name, err)
	}

	delCount := len(seg.deletedDocs)

	info := &SegmentCommitInfo{
		Name:     seg.name,
		MaxDoc:   seg.docCount,
		DelCount: delCount,
		Fields:   fields,
		Files:    files,
	}

	// Write .del file if any documents were deleted.
	if delCount > 0 {
		delFile, err := writeDeleteBitset(dir, seg.name, seg.docCount, seg.deletedDocs)
		if err != nil {
			return nil, fmt.Errorf("write intra-segment deletes for %s: %w", seg.name, err)
		}
		info.Files = append(info.Files, delFile)
	}

	return info, nil
}
```

6. **Update `reset()`** — reinitialize slice and pendingUpdates:

```go
func (dwpt *DocumentsWriterPerThread) reset(name string) {
	dwpt.segment = newInMemorySegment(name)
	dwpt.bytesUsed = 0
	dwpt.flushPending = false
	dwpt.deleteSlice = dwpt.deleteQueue.newSlice()
	dwpt.pendingUpdates = newBufferedUpdates()
}
```

- [ ] **Step 4: Fix existing DWPT tests**

Update `index/dwpt_test.go` — existing tests use `newDWPT(name, analyzer)` and `addDocument(doc, seqNo)`. Change to:
- `newDWPT(name, analyzer, newDeleteQueue())` (create a private queue for unit tests)
- `addDocument(doc)` (no seqNo parameter)
- `flush(dir)` (no deleteTerms parameter)

The existing tests (`TestDWPTAddDocument`, `TestDWPTAddMultipleDocuments`, `TestDWPTFlush`, `TestDWPTEstimateBytesUsed`) should use:

```go
// Replace:  newDWPT("_seg0", newTestAnalyzer())
// With:     newDWPT("_seg0", newTestAnalyzer(), newDeleteQueue())

// Replace:  dwpt.addDocument(doc, 0)
// With:     dwpt.addDocument(doc)

// Replace:  dwpt.flush(dir, nil)
// With:     dwpt.flush(dir)
```

- [ ] **Step 5: Run all DWPT tests**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./index/ -run TestDWPT -v`
Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add index/dwpt.go index/dwpt_test.go
git commit -m "feat: update DWPT to use DeleteQueue, DeleteSlice, and docIDUpto-based deletes"
```

---

### Task 5: Update FlushTicket to carry FrozenBufferedUpdates

The FlushTicket needs to carry the `FrozenBufferedUpdates` returned by `prepareFlush()` so they can be applied to other segments during publishing.

**Files:**
- Modify: `index/flush_ticket.go`

- [ ] **Step 1: Update FlushTicket struct**

Add `globalUpdates` field to `FlushTicket`:

```go
type FlushTicket struct {
	result        *SegmentCommitInfo
	globalUpdates *FrozenBufferedUpdates // cross-segment deletes from prepareFlush
	err           error
	done          chan struct{}
}
```

Update `markDone` to accept `FrozenBufferedUpdates`:

```go
func (q *FlushTicketQueue) markDone(ticket *FlushTicket, info *SegmentCommitInfo, globalUpdates *FrozenBufferedUpdates, err error) {
	ticket.result = info
	ticket.globalUpdates = globalUpdates
	ticket.err = err
	close(ticket.done)
}
```

- [ ] **Step 2: Run existing ticket tests to verify they still compile**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./index/ -run TestFlushTicket -v`
Expected: Compilation error in callers of `markDone` — will be fixed in the next task.

- [ ] **Step 3: Commit**

```bash
git add index/flush_ticket.go
git commit -m "feat: add FrozenBufferedUpdates field to FlushTicket"
```

---

### Task 6: Update DocumentsWriter to use DeleteQueue

Replace `deleteMap`/`nextSeqNo` with `DeleteQueue`. The `doFlush` method now calls `prepareFlush()` on the DWPT and passes frozen updates through the ticket queue.

**Files:**
- Modify: `index/documents_writer.go`
- Modify: `index/dwpt_pool.go`

- [ ] **Step 1: Update dwpt_pool.go to pass DeleteQueue**

The pool needs access to the DeleteQueue to pass it to newly created DWPTs:

```go
type perThreadPool struct {
	mu             sync.Mutex
	free           []*DocumentsWriterPerThread
	active         map[*DocumentsWriterPerThread]bool
	fullFlush      bool
	flushingActive map[*DocumentsWriterPerThread]bool
	flushOnReturn  []*DocumentsWriterPerThread
	flushRemaining int
	fullFlushDone  chan struct{}
	nameFunc       func() string
	analyzer       *analysis.Analyzer
	deleteQueue    *DeleteQueue
}

func newPerThreadPool(nameFunc func() string, analyzer *analysis.Analyzer, deleteQueue *DeleteQueue) *perThreadPool {
	return &perThreadPool{
		active:      make(map[*DocumentsWriterPerThread]bool),
		nameFunc:    nameFunc,
		analyzer:    analyzer,
		deleteQueue: deleteQueue,
	}
}
```

Update `getAndLock` to pass deleteQueue:

```go
func (p *perThreadPool) getAndLock() *DocumentsWriterPerThread {
	p.mu.Lock()
	defer p.mu.Unlock()

	var dwpt *DocumentsWriterPerThread
	if len(p.free) > 0 {
		dwpt = p.free[len(p.free)-1]
		p.free = p.free[:len(p.free)-1]
	} else {
		dwpt = newDWPT(p.nameFunc(), p.analyzer, p.deleteQueue)
	}
	p.active[dwpt] = true
	return dwpt
}
```

- [ ] **Step 2: Rewrite DocumentsWriter**

Replace the entire struct and methods to use DeleteQueue:

```go
type DocumentsWriter struct {
	mu           sync.Mutex
	pool         *perThreadPool
	flushControl *FlushControl
	ticketQueue  *FlushTicketQueue
	deleteQueue  *DeleteQueue
	dir          store.Directory
	onSegmentFlushed func(info *SegmentCommitInfo)
}

func newDocumentsWriter(dir store.Directory, analyzer *analysis.Analyzer, ramBufferSize int64, maxBufferedDocs int, nameFunc func() string) *DocumentsWriter {
	deleteQueue := newDeleteQueue()
	pool := newPerThreadPool(nameFunc, analyzer, deleteQueue)
	return &DocumentsWriter{
		pool:         pool,
		flushControl: newFlushControl(ramBufferSize, maxBufferedDocs, pool),
		ticketQueue:  newFlushTicketQueue(),
		deleteQueue:  deleteQueue,
		dir:          dir,
	}
}
```

Update `addDocument` — remove seqNo allocation:

```go
func (dw *DocumentsWriter) addDocument(doc *document.Document) error {
	dw.flushControl.waitIfStalled()

	dwpt := dw.pool.getAndLock()

	bytesAdded, err := dwpt.addDocument(doc)
	if err != nil {
		dw.pool.returnAndUnlock(dwpt)
		return err
	}

	flushDWPT := dw.flushControl.doAfterDocument(dwpt, bytesAdded)
	if flushDWPT != nil {
		dw.pool.remove(flushDWPT)
		if err := dw.doFlush(flushDWPT); err != nil {
			return err
		}
	} else {
		dw.pool.returnAndUnlock(dwpt)
	}

	if err := dw.publishFlushedSegments(); err != nil {
		return err
	}

	return nil
}
```

Update `doFlush` — call `prepareFlush()` instead of `snapshotDeletes()`:

```go
func (dw *DocumentsWriter) doFlush(dwpt *DocumentsWriterPerThread) error {
	ticket := dw.ticketQueue.addTicket()

	// prepareFlush freezes global deletes and applies remaining DWPT-local deletes
	globalUpdates := dwpt.prepareFlush()

	info, err := dwpt.flush(dw.dir)
	dw.flushControl.doAfterFlush(dwpt)
	dw.ticketQueue.markDone(ticket, info, globalUpdates, err)

	if err != nil {
		return err
	}
	return nil
}
```

Update `deleteDocuments` — delegate to DeleteQueue:

```go
func (dw *DocumentsWriter) deleteDocuments(field, term string) {
	dw.deleteQueue.addDelete(field, term)
}
```

Remove `snapshotDeletes()` and `takePendingDeletes()` entirely.

Add method to freeze remaining global buffer (called at commit/NRT time):

```go
// freezeGlobalBuffer freezes any remaining global deletes for cross-segment application.
func (dw *DocumentsWriter) freezeGlobalBuffer() *FrozenBufferedUpdates {
	return dw.deleteQueue.freezeGlobalBuffer(nil)
}
```

Keep `publishFlushedSegments` and `flushAllThreads` largely the same, but update `publishFlushedSegments` to expose global updates:

```go
func (dw *DocumentsWriter) publishFlushedSegments() error {
	published := dw.ticketQueue.publishCompleted()
	for _, ticket := range published {
		if ticket.err != nil {
			return ticket.err
		}
		if ticket.result != nil && dw.onSegmentFlushed != nil {
			dw.onSegmentFlushed(ticket.result)
		}
		// Global updates are handled by the IndexWriter
		if ticket.globalUpdates != nil && ticket.globalUpdates.any() {
			if dw.onGlobalUpdates != nil {
				dw.onGlobalUpdates(ticket.globalUpdates)
			}
		}
	}
	return nil
}
```

Add a callback for global updates:

```go
type DocumentsWriter struct {
	// ... existing fields ...
	onSegmentFlushed func(info *SegmentCommitInfo)
	onGlobalUpdates  func(updates *FrozenBufferedUpdates)
}
```

- [ ] **Step 3: Verify compilation**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go build ./index/`
Expected: Compilation errors in `writer.go` — will be fixed in the next task.

- [ ] **Step 4: Commit**

```bash
git add index/documents_writer.go index/dwpt_pool.go
git commit -m "feat: replace deleteMap with DeleteQueue in DocumentsWriter"
```

---

### Task 7: Update IndexWriter

The IndexWriter needs to:
1. Handle `FrozenBufferedUpdates` from the ticket queue (apply to existing segments)
2. At commit/NRT time, freeze any remaining global deletes
3. Remove `applyDeleteTerms()` and the `DeleteTerm` struct
4. Remove `MaxSeqNo` from `SegmentCommitInfo`

**Files:**
- Modify: `index/writer.go`
- Modify: `index/segment_info.go`

- [ ] **Step 1: Update IndexWriter to handle FrozenBufferedUpdates**

Remove the `DeleteTerm` struct from `writer.go`.

Add `applyFrozenUpdates` method:

```go
// applyFrozenUpdates applies frozen delete terms to all existing segments.
// This is the cross-segment delete: terms in the frozen updates are resolved
// against every segment's postings, and matching documents are marked deleted.
func (w *IndexWriter) applyFrozenUpdates(frozen *FrozenBufferedUpdates) error {
	if frozen == nil || !frozen.any() {
		return nil
	}
	for _, info := range w.segmentInfos.Segments {
		rau := w.getOrCreateRAU(info)
		reader, err := rau.getReader()
		if err != nil {
			return fmt.Errorf("open segment %s for delete: %w", info.Name, err)
		}
		for _, dt := range frozen.deleteTerms {
			iter := reader.PostingsIterator(dt.Field, dt.Term)
			for iter.Next() {
				rau.Delete(iter.DocID())
			}
		}
	}
	return nil
}
```

Update the constructor to register the `onGlobalUpdates` callback:

```go
func NewIndexWriter(dir store.Directory, analyzer *analysis.Analyzer, bufferSize int) *IndexWriter {
	w := &IndexWriter{
		dir:         dir,
		analyzer:    analyzer,
		readerMap:   make(map[string]*ReadersAndUpdates),
		fileDeleter: NewFileDeleter(dir),
	}
	// ... existing segment loading code ...

	w.docWriter = newDocumentsWriter(dir, analyzer, defaultRAMBufferSize, bufferSize, func() string {
		return w.nextSegmentName()
	})
	w.docWriter.onSegmentFlushed = func(info *SegmentCommitInfo) {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.segmentInfos.Segments = append(w.segmentInfos.Segments, info)
		w.segmentInfos.Version++
	}
	w.docWriter.onGlobalUpdates = func(updates *FrozenBufferedUpdates) {
		w.mu.Lock()
		defer w.mu.Unlock()
		// Apply frozen deletes to all existing segments (cross-segment)
		w.applyFrozenUpdates(updates)
	}

	return w
}
```

Update `Commit()` — replace `takePendingDeletes`/`applyDeleteTerms` with `freezeGlobalBuffer`/`applyFrozenUpdates`:

```go
func (w *IndexWriter) Commit() error {
	// 1. Flush all threads
	if err := w.docWriter.flushAllThreads(); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// 2. Freeze and apply any remaining global deletes
	frozen := w.docWriter.freezeGlobalBuffer()
	if err := w.applyFrozenUpdates(frozen); err != nil {
		return err
	}

	// 3-10. (unchanged: write .del files, fsync, write segments_N, etc.)
	// ... existing code from step 3 onward ...
}
```

Update `Flush()` similarly:

```go
func (w *IndexWriter) Flush() error {
	if err := w.docWriter.flushAllThreads(); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	frozen := w.docWriter.freezeGlobalBuffer()
	w.applyFrozenUpdates(frozen)

	return w.autoMerge()
}
```

Update `nrtSegments()` similarly:

```go
func (w *IndexWriter) nrtSegments() ([]SegmentReader, []string, error) {
	if err := w.docWriter.flushAllThreads(); err != nil {
		return nil, nil, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Freeze and apply remaining global deletes
	frozen := w.docWriter.freezeGlobalBuffer()
	if err := w.applyFrozenUpdates(frozen); err != nil {
		return nil, nil, err
	}

	// ... rest unchanged ...
}
```

Update `DeleteDocuments` — just delegate:

```go
func (w *IndexWriter) DeleteDocuments(field, term string) error {
	w.docWriter.deleteDocuments(field, term)
	return nil
}
```

- [ ] **Step 2: Remove MaxSeqNo from SegmentCommitInfo**

In `index/segment_info.go`, remove the `MaxSeqNo` field:

```go
type SegmentCommitInfo struct {
	Name     string   `json:"name"`
	MaxDoc   int      `json:"max_doc"`
	DelCount int      `json:"del_count"`
	Fields   []string `json:"fields"`
	Files    []string `json:"files"`
}
```

- [ ] **Step 3: Verify compilation**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go build ./...`
Expected: May have compilation errors in test files — fix any remaining references to `DeleteTerm`, `MaxSeqNo`, old method signatures.

- [ ] **Step 4: Commit**

```bash
git add index/writer.go index/segment_info.go
git commit -m "feat: use FrozenBufferedUpdates for cross-segment deletes, remove seqNo-based logic"
```

---

### Task 8: Fix all tests

With the structural changes complete, fix compilation errors and verify all existing tests pass. The key changes needed:

1. **`index/writer_test.go`**: Remove any direct use of `DeleteTerm`, `MaxSeqNo`, `docSeqNos`, `snapshotDeletes`, `takePendingDeletes`. The external API (`AddDocument`, `DeleteDocuments`, `Commit`, `Flush`, `OpenNRTReader`) should be unchanged.

2. **`index/flush_ticket_test.go`**: Update `markDone` calls to include the `globalUpdates` parameter (pass `nil`).

3. **`index/flush_control_test.go`**: Update `newPerThreadPool` calls to include `deleteQueue`.

4. **`index/e2e_test.go`**: Should mostly work unchanged since it uses the public API.

5. **`search/searcher_test.go`**: Should be unchanged.

6. **`server/index/shard.go`**: Check if it references any changed APIs.

**Files:**
- Modify: `index/writer_test.go`
- Modify: `index/flush_ticket_test.go`
- Modify: `index/flush_control_test.go`
- Modify: `index/e2e_test.go` (if needed)
- Modify: `index/reader_test.go` (if needed)
- Modify: `server/index/shard.go` (if needed)

- [ ] **Step 1: Fix compilation errors across all test files**

Fix `flush_ticket_test.go`:
```go
// Replace: q.markDone(ticket, info, nil)
// With:    q.markDone(ticket, info, nil, nil)
```

Fix `flush_control_test.go`:
```go
// Replace: newPerThreadPool(nameFunc, analyzer)
// With:    newPerThreadPool(nameFunc, analyzer, newDeleteQueue())
```

Fix `writer_test.go` — search for any direct use of internal delete APIs and update. The public API (`AddDocument`, `DeleteDocuments`, `Commit`, etc.) should remain unchanged, so most writer tests should compile without changes.

Check `server/index/shard.go` for any references to changed APIs.

- [ ] **Step 2: Run the full test suite**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./... -v -count=1`
Expected: All tests PASS. If any fail, debug and fix.

- [ ] **Step 3: Run the delete-specific tests**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./index/ -run "Delete|delete" -v -count=1`
Expected: All PASS. Pay special attention to:
- `TestDeleteDocument`
- `TestDeleteInBufferBeforeFlush`
- `TestDeleteMultipleMatchingDocs`
- `TestDeleteAcrossMultipleSegments`
- `TestCommitPersistsDeletes`
- `TestNewWriterDeletesAcrossSessions`
- `TestAddThenDelete`
- `TestDeleteThenAdd`
- `TestUpdateSameDocTwice`

- [ ] **Step 4: Run e2e tests**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./index/ -run TestE2E -v -count=1`
Expected: All PASS.

- [ ] **Step 5: Run search tests**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./search/ -v -count=1`
Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "fix: update all tests for DeleteQueue-based delete handling"
```

---

### Task 9: Add integration test for the new delete flow

Add a targeted test that exercises the complete Lucene-style delete flow: DeleteQueue → DeleteSlice → BufferedUpdates → FrozenBufferedUpdates → cross-segment application.

**Files:**
- Modify: `index/e2e_test.go`

- [ ] **Step 1: Write the integration test**

```go
func TestE2EDeleteQueueCrossSegmentDeletes(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	writer := NewIndexWriter(dir, analyzer, 2) // 2 docs per segment

	// Segment 1: doc0 (java), doc1 (go)
	doc0 := document.NewDocument()
	doc0.AddField("title", "java", document.FieldTypeKeyword)
	doc0.AddField("body", "learn java programming", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("title", "go", document.FieldTypeKeyword)
	doc1.AddField("body", "learn go programming", document.FieldTypeText)
	writer.AddDocument(doc1)
	// Auto-flush creates segment 1

	// Segment 2: doc2 (rust), doc3 (java again)
	doc2 := document.NewDocument()
	doc2.AddField("title", "rust", document.FieldTypeKeyword)
	doc2.AddField("body", "learn rust programming", document.FieldTypeText)
	writer.AddDocument(doc2)

	doc3 := document.NewDocument()
	doc3.AddField("title", "java", document.FieldTypeKeyword)
	doc3.AddField("body", "advanced java concepts", document.FieldTypeText)
	writer.AddDocument(doc3)
	// Auto-flush creates segment 2

	// Delete all "java" docs — this should apply across BOTH segments
	writer.DeleteDocuments("title", "java")

	// Commit to apply cross-segment deletes
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify: 2 docs deleted (doc0 and doc3), 2 alive (doc1 and doc2)
	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 4 {
		t.Errorf("TotalDocCount: got %d, want 4", reader.TotalDocCount())
	}
	if reader.LiveDocCount() != 2 {
		t.Errorf("LiveDocCount: got %d, want 2", reader.LiveDocCount())
	}

	// Search for "programming" — should find only go and rust docs
	searcher := search.NewIndexSearcher(reader)
	query := search.NewTermQuery("body", "programming")
	collector := search.NewTopKCollector(10)
	results := searcher.Search(query, collector)
	if len(results) != 2 {
		t.Errorf("search results: got %d, want 2", len(results))
	}
}

func TestE2EDeleteDuringConcurrentAdds(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	writer := NewIndexWriter(dir, analyzer, 100)

	// Add some initial docs
	for i := 0; i < 10; i++ {
		doc := document.NewDocument()
		doc.AddField("id", fmt.Sprintf("doc%d", i), document.FieldTypeKeyword)
		doc.AddField("body", "hello world", document.FieldTypeText)
		writer.AddDocument(doc)
	}

	// Delete a term that exists in all docs
	writer.DeleteDocuments("body", "hello")

	// Add more docs after the delete
	for i := 10; i < 20; i++ {
		doc := document.NewDocument()
		doc.AddField("id", fmt.Sprintf("doc%d", i), document.FieldTypeKeyword)
		doc.AddField("body", "hello world", document.FieldTypeText)
		writer.AddDocument(doc)
	}

	// The delete should affect docs 0-9 but NOT docs 10-19
	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.TotalDocCount() != 20 {
		t.Errorf("TotalDocCount: got %d, want 20", reader.TotalDocCount())
	}
	if reader.LiveDocCount() != 10 {
		t.Errorf("LiveDocCount: got %d, want 10 (docs 10-19 alive)", reader.LiveDocCount())
	}
}
```

- [ ] **Step 2: Run the integration tests**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./index/ -run "TestE2EDeleteQueueCrossSegment|TestE2EDeleteDuringConcurrent" -v`
Expected: All PASS.

- [ ] **Step 3: Run full test suite one final time**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./... -count=1`
Expected: All PASS.

- [ ] **Step 4: Commit**

```bash
git add index/e2e_test.go
git commit -m "test: add integration tests for DeleteQueue cross-segment delete flow"
```
