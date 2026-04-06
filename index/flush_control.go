package index

import "sync"

// FlushControl tracks RAM usage across all DWPTs and decides when to flush.
// It also implements backpressure by stalling indexing goroutines when
// too much RAM is pending flush.
type FlushControl struct {
	mu              sync.Mutex
	activeBytes     int64 // bytes in actively indexing DWPTs
	flushBytes      int64 // bytes in DWPTs that are pending flush
	ramBufferSize   int64
	maxBufferedDocs int   // max docs per DWPT before flush (0 = no limit)
	stallLimit      int64 // 2x ramBufferSize
	stallCond       *sync.Cond
	stalled         bool
	flushQueue      []*DocumentsWriterPerThread
	pool            *perThreadPool
	infoStream      InfoStream
	metrics         *IndexWriterMetrics
}

func newFlushControl(ramBufferSize int64, maxBufferedDocs int, pool *perThreadPool) *FlushControl {
	fc := &FlushControl{
		ramBufferSize:   ramBufferSize,
		maxBufferedDocs: maxBufferedDocs,
		stallLimit:      2 * ramBufferSize,
		pool:            pool,
		infoStream:      &NoOpInfoStream{},
	}
	fc.stallCond = sync.NewCond(&fc.mu)
	return fc
}

// doAfterDocument is called after a document is indexed.
// If total active bytes exceed the RAM buffer threshold or the DWPT has
// reached the max buffered doc count, the DWPT is marked as flush-pending
// and added to the flush queue. Returns true if the DWPT is now flush-pending
// (and should NOT be returned to the free pool by the caller).
func (fc *FlushControl) doAfterDocument(dwpt *DocumentsWriterPerThread, bytesAdded int64) bool {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.activeBytes += bytesAdded

	shouldFlush := false
	if fc.activeBytes >= fc.ramBufferSize && !dwpt.flushPending {
		shouldFlush = true
	}
	if fc.maxBufferedDocs > 0 && dwpt.segment.docCount >= fc.maxBufferedDocs && !dwpt.flushPending {
		shouldFlush = true
	}

	if shouldFlush {
		dwpt.flushPending = true
		dwptBytes := dwpt.estimateBytesUsed()
		fc.activeBytes -= dwptBytes
		fc.flushBytes += dwptBytes
		fc.flushQueue = append(fc.flushQueue, dwpt)

		if fc.activeBytes+fc.flushBytes >= fc.stallLimit {
			fc.stalled = true
		}

		return true
	}

	return false
}

// nextPendingFlush returns the next DWPT that needs flushing, or nil.
// The DWPT is dequeued from the flush queue and removed from the pool.
func (fc *FlushControl) nextPendingFlush() *DocumentsWriterPerThread {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if len(fc.flushQueue) == 0 {
		return nil
	}
	dwpt := fc.flushQueue[0]
	fc.flushQueue = fc.flushQueue[1:]
	fc.pool.remove(dwpt)
	return dwpt
}

// doAfterFlush is called after a DWPT has been flushed to disk.
// Decrements flushBytes and potentially unstalls blocked goroutines.
func (fc *FlushControl) doAfterFlush(dwpt *DocumentsWriterPerThread) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.flushBytes -= dwpt.estimateBytesUsed()
	if fc.flushBytes < 0 {
		fc.flushBytes = 0
	}

	if fc.stalled && fc.activeBytes+fc.flushBytes < fc.stallLimit {
		fc.stalled = false
		fc.stallCond.Broadcast()
	}
}

// waitIfStalled blocks the calling goroutine if backpressure is active.
func (fc *FlushControl) waitIfStalled() {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	for fc.stalled {
		fc.stallCond.Wait()
	}
}

// markForFullFlush flushes all DWPTs. Free DWPTs are collected immediately;
// active DWPTs (currently used by addDocument) are captured when they are
// returned to the pool, so we wait for them without touching in-use buffers.
func (fc *FlushControl) markForFullFlush() []*DocumentsWriterPerThread {
	// Collect free DWPTs and enter full flush mode for active ones.
	freed := fc.pool.drainFreeAndMarkActive()

	// Wait until all active DWPTs finish their current document and return.
	returned := fc.pool.waitAndDrainActive()

	all := append(freed, returned...)

	fc.mu.Lock()
	defer fc.mu.Unlock()

	var toFlush []*DocumentsWriterPerThread
	for _, dwpt := range all {
		if dwpt.segment.docCount > 0 {
			if !dwpt.flushPending {
				dwptBytes := dwpt.estimateBytesUsed()
				fc.activeBytes -= dwptBytes
				fc.flushBytes += dwptBytes
				dwpt.flushPending = true
			}
			toFlush = append(toFlush, dwpt)
		}
	}
	return toFlush
}
