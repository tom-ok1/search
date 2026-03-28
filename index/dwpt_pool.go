package index

import (
	"sync"

	"gosearch/analysis"
)

// perThreadPool manages a pool of DocumentsWriterPerThread instances.
// Each indexing goroutine checks out a DWPT, uses it without locks, then returns it.
type perThreadPool struct {
	mu             sync.Mutex
	free           []*DocumentsWriterPerThread
	active         map[*DocumentsWriterPerThread]bool
	fullFlush      bool                               // true during full flush
	flushingActive map[*DocumentsWriterPerThread]bool // DWPTs that were active when full flush started
	flushOnReturn  []*DocumentsWriterPerThread        // DWPTs returned during full flush
	flushRemaining int                                // number of flushingActive DWPTs not yet returned
	fullFlushDone  chan struct{}                      // closed when flushRemaining reaches 0
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

// getAndLock checks out a DWPT for exclusive use by the caller.
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

// returnAndUnlock returns a DWPT to the pool for reuse.
// If this DWPT was active when a full flush started, it is routed to the
// flush list instead of the free list.
func (p *perThreadPool) returnAndUnlock(dwpt *DocumentsWriterPerThread) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.active, dwpt)

	if p.fullFlush && p.flushingActive[dwpt] {
		delete(p.flushingActive, dwpt)
		p.flushOnReturn = append(p.flushOnReturn, dwpt)
		p.flushRemaining--
		if p.flushRemaining == 0 {
			close(p.fullFlushDone)
		}
	} else {
		p.free = append(p.free, dwpt)
	}
}

// remove removes a DWPT from the pool permanently (e.g., after flush).
func (p *perThreadPool) remove(dwpt *DocumentsWriterPerThread) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.active, dwpt)

	if p.fullFlush && p.flushingActive[dwpt] {
		delete(p.flushingActive, dwpt)
		p.flushRemaining--
		if p.flushRemaining == 0 {
			close(p.fullFlushDone)
		}
	}
}

// drainFreeAndMarkActive returns all free DWPTs immediately and enters
// full flush mode. Active DWPTs will be captured when they are returned
// via returnAndUnlock. Caller must call waitAndDrainActive() afterward.
func (p *perThreadPool) drainFreeAndMarkActive() []*DocumentsWriterPerThread {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := p.free
	p.free = nil

	if len(p.active) > 0 {
		p.fullFlush = true
		p.flushingActive = make(map[*DocumentsWriterPerThread]bool, len(p.active))
		for dwpt := range p.active {
			p.flushingActive[dwpt] = true
		}
		p.flushRemaining = len(p.flushingActive)
		p.flushOnReturn = nil
		p.fullFlushDone = make(chan struct{})
	}

	return result
}

// waitAndDrainActive blocks until all DWPTs that were active at the time of
// drainFreeAndMarkActive have been returned, then returns them and exits
// full flush mode. New DWPTs created after the full flush started are not
// affected and continue to operate normally.
func (p *perThreadPool) waitAndDrainActive() []*DocumentsWriterPerThread {
	p.mu.Lock()
	if !p.fullFlush {
		p.mu.Unlock()
		return nil
	}
	if p.flushRemaining == 0 {
		result := p.flushOnReturn
		p.flushOnReturn = nil
		p.flushingActive = nil
		p.fullFlush = false
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
	p.fullFlush = false
	return result
}
