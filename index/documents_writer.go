package index

import (
	"sync"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

// DocumentsWriter coordinates concurrent document indexing using DWPT pool,
// flush control, and ticket queue. It is the core concurrency layer between
// IndexWriter and the per-thread indexing buffers.
type DocumentsWriter struct {
	mu           sync.Mutex
	pool         *perThreadPool
	flushControl *FlushControl
	ticketQueue  *FlushTicketQueue
	deleteQueue  []DeleteTerm
	dir          store.Directory
	// onSegmentFlushed is called when a segment is flushed to disk.
	onSegmentFlushed func(info *SegmentCommitInfo)
}

func newDocumentsWriter(dir store.Directory, analyzer *analysis.Analyzer, ramBufferSize int64, maxBufferedDocs int, nameFunc func() string) *DocumentsWriter {
	pool := newPerThreadPool(nameFunc, analyzer)
	return &DocumentsWriter{
		pool:         pool,
		flushControl: newFlushControl(ramBufferSize, maxBufferedDocs, pool),
		ticketQueue:  newFlushTicketQueue(),
		dir:          dir,
	}
}

// addDocument indexes a document concurrently. The caller does not need to hold any lock.
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
		// This DWPT needs flushing — remove it from the pool and flush
		dw.pool.remove(flushDWPT)
		if err := dw.doFlush(flushDWPT); err != nil {
			return err
		}
	} else {
		dw.pool.returnAndUnlock(dwpt)
	}

	// Try to publish any completed flushes
	if err := dw.publishFlushedSegments(); err != nil {
		return err
	}

	return nil
}

// doFlush flushes a single DWPT to disk.
func (dw *DocumentsWriter) doFlush(dwpt *DocumentsWriterPerThread) error {
	ticket := dw.ticketQueue.addTicket()

	info, err := dwpt.flush(dw.dir)
	dw.flushControl.doAfterFlush(dwpt)
	dw.ticketQueue.markDone(ticket, info, err)

	if err != nil {
		return err
	}
	return nil
}

// publishFlushedSegments publishes any completed flush tickets in order.
func (dw *DocumentsWriter) publishFlushedSegments() error {
	published := dw.ticketQueue.publishCompleted()
	for _, ticket := range published {
		if ticket.err != nil {
			return ticket.err
		}
		if ticket.result != nil && dw.onSegmentFlushed != nil {
			dw.onSegmentFlushed(ticket.result)
		}
	}
	return nil
}

// flushAllThreads flushes all active DWPTs. Called during commit/NRT.
func (dw *DocumentsWriter) flushAllThreads() error {
	toFlush := dw.flushControl.markForFullFlush()

	for _, dwpt := range toFlush {
		if err := dw.doFlush(dwpt); err != nil {
			return err
		}
	}

	return dw.publishFlushedSegments()
}

// deleteDocuments buffers a delete-by-term operation.
func (dw *DocumentsWriter) deleteDocuments(field, term string) {
	dw.mu.Lock()
	defer dw.mu.Unlock()
	dw.deleteQueue = append(dw.deleteQueue, DeleteTerm{Field: field, Term: term})
}

// takePendingDeletes returns and clears the buffered delete terms.
func (dw *DocumentsWriter) takePendingDeletes() []DeleteTerm {
	dw.mu.Lock()
	defer dw.mu.Unlock()
	deletes := dw.deleteQueue
	dw.deleteQueue = nil
	return deletes
}
