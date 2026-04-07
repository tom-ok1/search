package index

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// FlushControl tracks RAM usage across all DWPTs and decides when to flush.
// It also implements backpressure by stalling indexing goroutines when
// too much RAM is pending flush.
type FlushControl struct {
	mu              sync.Mutex
	activeBytes     atomic.Int64 // bytes in actively indexing DWPTs
	flushBytes      int64        // bytes in DWPTs that are pending flush
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
	newActive := fc.activeBytes.Add(bytesAdded)

	// Fast path: no flush needed (common case — lock-free)
	byDocCount := fc.maxBufferedDocs > 0 && dwpt.segment.docCount >= fc.maxBufferedDocs
	if newActive < fc.ramBufferSize && !byDocCount {
		return false
	}

	// Slow path: flush may be needed — acquire lock
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if dwpt.flushPending {
		return false
	}

	// Re-check RAM threshold under lock (another goroutine may have flushed)
	shouldFlush := fc.activeBytes.Load() >= fc.ramBufferSize || byDocCount
	if !shouldFlush {
		return false
	}

	dwpt.flushPending = true
	dwptBytes := dwpt.estimateBytesUsed()
	fc.activeBytes.Add(-dwptBytes)
	fc.flushBytes += dwptBytes
	fc.flushQueue = append(fc.flushQueue, dwpt)

	if fc.metrics != nil {
		fc.metrics.FlushPendingBytes.Store(fc.flushBytes)
	}
	if fc.infoStream.IsEnabled("DWFC") {
		activeBytes := fc.activeBytes.Load()
		fc.infoStream.Message("DWFC", fmt.Sprintf(
			"flush triggered: ramBytes=%.1f MB > limit=%.1f MB",
			float64(activeBytes+fc.flushBytes)/(1024*1024),
			float64(fc.ramBufferSize)/(1024*1024)))
	}

	activeBytes := fc.activeBytes.Load()
	if activeBytes+fc.flushBytes >= fc.stallLimit {
		fc.stalled = true
	}

	return true
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
	if fc.metrics != nil {
		fc.metrics.FlushPendingBytes.Store(fc.flushBytes)
	}

	activeBytes := fc.activeBytes.Load()
	if fc.stalled && activeBytes+fc.flushBytes < fc.stallLimit {
		fc.stalled = false
		fc.stallCond.Broadcast()
	}
}

// waitIfStalled blocks the calling goroutine if backpressure is active.
func (fc *FlushControl) waitIfStalled() {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if !fc.stalled {
		return
	}

	if fc.metrics != nil {
		fc.metrics.StallCount.Add(1)
	}
	if fc.infoStream.IsEnabled("DW") {
		fc.infoStream.Message("DW", fmt.Sprintf(
			"now stalling: activeBytes=%.1f MB flushBytes=%.1f MB",
			float64(fc.activeBytes.Load())/(1024*1024),
			float64(fc.flushBytes)/(1024*1024)))
	}

	start := time.Now()
	for fc.stalled {
		fc.stallCond.Wait()
	}
	elapsed := time.Since(start)

	if fc.metrics != nil {
		fc.metrics.StallTimeNanos.Add(elapsed.Nanoseconds())
	}
	if fc.infoStream.IsEnabled("DW") {
		fc.infoStream.Message("DW", fmt.Sprintf("done stalling for %.1f ms", float64(elapsed.Nanoseconds())/1e6))
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
				fc.activeBytes.Add(-dwptBytes)
				fc.flushBytes += dwptBytes
				dwpt.flushPending = true
			}
			toFlush = append(toFlush, dwpt)
		}
	}
	return toFlush
}
