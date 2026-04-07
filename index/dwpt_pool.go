package index

import (
	"sync"
	"sync/atomic"

	"gosearch/analysis"
)

// perThreadPool manages a pool of DocumentsWriterPerThread instances.
// Each indexing goroutine checks out a DWPT, uses it without locks, then returns it.
// The common (non-full-flush) path uses sync.Pool and is completely lock-free.
type perThreadPool struct {
	mu             sync.Mutex
	syncPool       sync.Pool
	active         map[*DocumentsWriterPerThread]bool
	inFullFlush    atomic.Bool
	flushingActive map[*DocumentsWriterPerThread]bool // DWPTs that were active when full flush started
	flushOnReturn  []*DocumentsWriterPerThread        // DWPTs returned during full flush
	flushRemaining int                                // number of flushingActive DWPTs not yet returned
	fullFlushDone  chan struct{}                      // closed when flushRemaining reaches 0
	nameFunc       func() string
	fieldAnalyzers *analysis.FieldAnalyzers
	deleteQueue    *DeleteQueue
}

func newPerThreadPool(nameFunc func() string, fieldAnalyzers *analysis.FieldAnalyzers, deleteQueue *DeleteQueue) *perThreadPool {
	p := &perThreadPool{
		active:         make(map[*DocumentsWriterPerThread]bool),
		nameFunc:       nameFunc,
		fieldAnalyzers: fieldAnalyzers,
		deleteQueue:    deleteQueue,
	}
	p.syncPool.New = func() any {
		return newDWPT(p.nameFunc(), p.fieldAnalyzers, p.deleteQueue)
	}
	return p
}

// getAndLock checks out a DWPT for exclusive use by the caller.
// Uses sync.Pool instead of a mutex-guarded free list for DWPT allocation.
func (p *perThreadPool) getAndLock() *DocumentsWriterPerThread {
	dwpt := p.syncPool.Get().(*DocumentsWriterPerThread)

	p.mu.Lock()
	p.active[dwpt] = true
	p.mu.Unlock()

	return dwpt
}

// returnAndUnlock returns a DWPT to the pool for reuse.
// If this DWPT was active when a full flush started, it is routed to the
// flush list instead of the sync.Pool.
func (p *perThreadPool) returnAndUnlock(dwpt *DocumentsWriterPerThread) {
	p.mu.Lock()
	delete(p.active, dwpt)

	if p.inFullFlush.Load() && p.flushingActive[dwpt] {
		delete(p.flushingActive, dwpt)
		p.flushOnReturn = append(p.flushOnReturn, dwpt)
		p.flushRemaining--
		if p.flushRemaining == 0 {
			close(p.fullFlushDone)
		}
		p.mu.Unlock()
	} else {
		p.mu.Unlock()
		p.syncPool.Put(dwpt)
	}
}

// remove removes a DWPT from the pool permanently (e.g., after flush).
func (p *perThreadPool) remove(dwpt *DocumentsWriterPerThread) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.active, dwpt)

	if p.inFullFlush.Load() && p.flushingActive[dwpt] {
		delete(p.flushingActive, dwpt)
		p.flushRemaining--
		if p.flushRemaining == 0 {
			close(p.fullFlushDone)
		}
	}
}

// drainFreeAndMarkActive enters full flush mode and marks all currently active
// DWPTs for flushing. sync.Pool doesn't expose a "drain all" API, but free
// DWPTs have already been returned (0 docs or already flushed) and don't need
// to be included in full flush. Returns nil (no free DWPTs to drain).
// Caller must call waitAndDrainActive() afterward.
func (p *perThreadPool) drainFreeAndMarkActive() []*DocumentsWriterPerThread {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.active) > 0 {
		p.inFullFlush.Store(true)
		p.flushingActive = make(map[*DocumentsWriterPerThread]bool, len(p.active))
		for dwpt := range p.active {
			p.flushingActive[dwpt] = true
		}
		p.flushRemaining = len(p.flushingActive)
		p.flushOnReturn = nil
		p.fullFlushDone = make(chan struct{})
	} else {
		p.inFullFlush.Store(true)
		p.flushRemaining = 0
	}

	return nil
}

// waitAndDrainActive blocks until all DWPTs that were active at the time of
// drainFreeAndMarkActive have been returned, then returns them and exits
// full flush mode. New DWPTs created after the full flush started are not
// affected and continue to operate normally.
func (p *perThreadPool) waitAndDrainActive() []*DocumentsWriterPerThread {
	p.mu.Lock()
	if !p.inFullFlush.Load() {
		p.mu.Unlock()
		return nil
	}
	if p.flushRemaining == 0 {
		result := p.flushOnReturn
		p.flushOnReturn = nil
		p.flushingActive = nil
		p.inFullFlush.Store(false)
		p.mu.Unlock()
		return result
	}
	done := p.fullFlushDone
	p.mu.Unlock()

	<-done

	p.mu.Lock()
	defer p.mu.Unlock()
	result := p.flushOnReturn
	p.flushOnReturn = nil
	p.flushingActive = nil
	p.inFullFlush.Store(false)
	return result
}
