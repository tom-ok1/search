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
	deleteQueue  *DeleteQueue
	dir          store.Directory
	// onSegmentFlushed is called when a segment is flushed to disk.
	onSegmentFlushed func(info *SegmentCommitInfo)
	// onGlobalUpdates is called when frozen global deletes need cross-segment application.
	onGlobalUpdates func(updates *FrozenBufferedUpdates)
}

func newDocumentsWriter(dir store.Directory, fieldAnalyzers *analysis.FieldAnalyzers, ramBufferSize int64, maxBufferedDocs int, nameFunc func() string) *DocumentsWriter {
	deleteQueue := newDeleteQueue()
	pool := newPerThreadPool(nameFunc, fieldAnalyzers, deleteQueue)
	return &DocumentsWriter{
		pool:         pool,
		flushControl: newFlushControl(ramBufferSize, maxBufferedDocs, pool),
		ticketQueue:  newFlushTicketQueue(),
		deleteQueue:  deleteQueue,
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

// doFlush flushes a single DWPT to disk.
func (dw *DocumentsWriter) doFlush(dwpt *DocumentsWriterPerThread) error {
	ticket := dw.ticketQueue.addTicket()
	globalUpdates := dwpt.prepareFlush()
	info, err := dwpt.flush(dw.dir)
	dw.flushControl.doAfterFlush(dwpt)
	dw.ticketQueue.markDone(ticket, info, globalUpdates, err)
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
		// Apply global deletes BEFORE registering the new segment.
		// The new segment already handled these deletes via pendingUpdates
		// during flush; applying them after registration would double-delete.
		if ticket.globalUpdates != nil && ticket.globalUpdates.any() && dw.onGlobalUpdates != nil {
			dw.onGlobalUpdates(ticket.globalUpdates)
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
	dw.deleteQueue.addDelete(field, term)
}

// freezeGlobalBuffer freezes and returns the global delete buffer.
func (dw *DocumentsWriter) freezeGlobalBuffer() *FrozenBufferedUpdates {
	return dw.deleteQueue.freezeGlobalBuffer(nil)
}
