package index

import (
	"fmt"
	"sync"
	"sync/atomic"

	"gosearch/analysis"
	"gosearch/document"
	goindex "gosearch/index"
	"gosearch/search"
	"gosearch/server/translog"
	"gosearch/store"
)

const defaultBufferSize = 1000

// VersionConflictEngineError is returned when an if_seq_no/if_primary_term
// check fails during an index or delete operation.
type VersionConflictEngineError struct {
	ID            string
	ExpectedSeqNo int64
	ExpectedTerm  int64
	CurrentSeqNo  int64
	CurrentTerm   int64
}

func (e *VersionConflictEngineError) Error() string {
	return fmt.Sprintf(
		"[%s]: version conflict, required seqNo [%d], primary term [%d]. current document has seqNo [%d] and primary term [%d]",
		e.ID, e.ExpectedSeqNo, e.ExpectedTerm, e.CurrentSeqNo, e.CurrentTerm,
	)
}

// IndexResult holds the outcome of an index operation.
type IndexResult struct {
	SeqNo       int64
	PrimaryTerm int64
	Created     bool
}

// DeleteResult holds the outcome of a delete operation.
type DeleteResult struct {
	SeqNo       int64
	PrimaryTerm int64
	Found       bool
}

// GetResult holds the outcome of a real-time get operation.
type GetResult struct {
	Found       bool
	SeqNo       int64
	PrimaryTerm int64
	Source      []byte
}

// Engine wraps an IndexWriter and manages the IndexReader/IndexSearcher lifecycle.
// It mirrors Elasticsearch's InternalEngine.
type Engine struct {
	writer     *goindex.IndexWriter
	reader     *goindex.IndexReader
	searcher   *search.IndexSearcher
	dir        store.Directory
	translog   *translog.Translog
	versionMap *LiveVersionMap
	mu         sync.RWMutex // protects reader/searcher swap on refresh

	localCheckpoint  atomic.Int64 // last assigned seqNo
	globalCheckpoint int64        // last known durable seqNo
	currentTerm      int64        // primary term for operations
}

// NewEngine creates a new Engine backed by the given directory and field analyzers.
// If translogPath is non-empty, a translog is created for durability.
func NewEngine(dir store.Directory, fieldAnalyzers *analysis.FieldAnalyzers, translogPath string) (*Engine, error) {
	writer := goindex.NewIndexWriter(dir, fieldAnalyzers, defaultBufferSize)

	e := &Engine{
		writer:      writer,
		dir:         dir,
		versionMap:  NewLiveVersionMap(),
		currentTerm: 1,
	}
	e.localCheckpoint.Store(translog.NoOpsPerformed)

	if translogPath != "" {
		config := &translog.TranslogConfig{Dir: translogPath}
		tl, err := translog.NewTranslog(config, "", e.currentTerm, translog.NoOpsPerformed, 1)
		if err != nil {
			writer.Close()
			return nil, fmt.Errorf("open translog: %w", err)
		}
		e.translog = tl
	}

	return e, nil
}

// RecoverFromTranslog replays uncommitted operations from the translog
// using snapshot-based iteration.
func (e *Engine) RecoverFromTranslog(replayIndex func(id string, source []byte) error, replayDelete func(id string) error) error {
	if e.translog == nil {
		return nil
	}

	snapshot, err := e.translog.NewSnapshot(0, int64(^uint64(0)>>1)) // 0 to max int64
	if err != nil {
		return fmt.Errorf("create recovery snapshot: %w", err)
	}
	defer snapshot.Close()

	for {
		op, err := snapshot.Next()
		if err != nil {
			return fmt.Errorf("read operation: %w", err)
		}
		if op == nil {
			break
		}

		switch o := op.(type) {
		case *translog.IndexOperation:
			if err := replayIndex(o.ID, o.Source); err != nil {
				return fmt.Errorf("replay index op for %s: %w", o.ID, err)
			}
		case *translog.DeleteOperation:
			if err := replayDelete(o.ID); err != nil {
				return fmt.Errorf("replay delete op for %s: %w", o.ID, err)
			}
		}

		// Track the highest seqNo we've replayed.
		if op.SeqNo() > e.localCheckpoint.Load() {
			e.localCheckpoint.Store(op.SeqNo())
		}
	}

	return nil
}

// Index adds or updates a document in the engine. It returns whether the
// document was newly created and the assigned sequence number.
func (e *Engine) Index(id string, doc *document.Document, source []byte, ifSeqNo *int64, ifPrimaryTerm *int64) (IndexResult, error) {
	var created bool

	vv, exists := e.versionMap.Get(id)
	if !exists {
		if e.docExistsInIndex(id) {
			created = false
		} else {
			created = true
		}
	} else {
		created = vv.Deleted
	}

	// CAS check: if_seq_no / if_primary_term
	if ifSeqNo != nil && ifPrimaryTerm != nil {
		if !exists {
			return IndexResult{}, &VersionConflictEngineError{
				ID: id, ExpectedSeqNo: *ifSeqNo, ExpectedTerm: *ifPrimaryTerm,
				CurrentSeqNo: 0, CurrentTerm: 0,
			}
		}
		if vv.SeqNo != *ifSeqNo || vv.PrimaryTerm != *ifPrimaryTerm {
			return IndexResult{}, &VersionConflictEngineError{
				ID: id, ExpectedSeqNo: *ifSeqNo, ExpectedTerm: *ifPrimaryTerm,
				CurrentSeqNo: vv.SeqNo, CurrentTerm: vv.PrimaryTerm,
			}
		}
	}

	e.writer.DeleteDocuments("_id", id)
	if err := e.writer.AddDocument(doc); err != nil {
		return IndexResult{}, err
	}

	seqNo := e.localCheckpoint.Add(1)

	e.versionMap.Put(id, VersionValue{
		SeqNo: seqNo, PrimaryTerm: e.currentTerm,
		Source: source, Deleted: false,
	})

	if e.translog != nil {
		tlOp := &translog.IndexOperation{
			ID: id, Source: source,
			SequenceNo: seqNo, PrimTerm: e.currentTerm,
		}
		if _, err := e.translog.Add(tlOp); err != nil {
			return IndexResult{}, fmt.Errorf("translog add: %w", err)
		}
	}

	return IndexResult{SeqNo: seqNo, PrimaryTerm: e.currentTerm, Created: created}, nil
}

// Delete removes a document by its _id. It returns whether the document
// was found and the assigned sequence number.
func (e *Engine) Delete(id string, ifSeqNo *int64, ifPrimaryTerm *int64) (DeleteResult, error) {
	vv, exists := e.versionMap.Get(id)
	found := exists || e.docExistsInIndex(id)

	if ifSeqNo != nil && ifPrimaryTerm != nil {
		if !exists {
			return DeleteResult{}, &VersionConflictEngineError{
				ID: id, ExpectedSeqNo: *ifSeqNo, ExpectedTerm: *ifPrimaryTerm,
				CurrentSeqNo: 0, CurrentTerm: 0,
			}
		}
		if vv.SeqNo != *ifSeqNo || vv.PrimaryTerm != *ifPrimaryTerm {
			return DeleteResult{}, &VersionConflictEngineError{
				ID: id, ExpectedSeqNo: *ifSeqNo, ExpectedTerm: *ifPrimaryTerm,
				CurrentSeqNo: vv.SeqNo, CurrentTerm: vv.PrimaryTerm,
			}
		}
	}

	if err := e.writer.DeleteDocuments("_id", id); err != nil {
		return DeleteResult{}, err
	}

	seqNo := e.localCheckpoint.Add(1)

	e.versionMap.Put(id, VersionValue{
		SeqNo: seqNo, PrimaryTerm: e.currentTerm, Deleted: true,
	})

	if e.translog != nil {
		tlOp := &translog.DeleteOperation{
			ID: id, SequenceNo: seqNo, PrimTerm: e.currentTerm,
		}
		if _, err := e.translog.Add(tlOp); err != nil {
			return DeleteResult{}, fmt.Errorf("translog add: %w", err)
		}
	}

	return DeleteResult{SeqNo: seqNo, PrimaryTerm: e.currentTerm, Found: found}, nil
}

// Get performs a real-time get by checking the version map first, then
// falling back to the Lucene searcher.
func (e *Engine) Get(id string) GetResult {
	// Check version map first (real-time path)
	vv, ok := e.versionMap.Get(id)
	if ok {
		if vv.Deleted {
			return GetResult{Found: false}
		}
		return GetResult{Found: true, SeqNo: vv.SeqNo, PrimaryTerm: vv.PrimaryTerm, Source: vv.Source}
	}

	// Fall back to Lucene searcher
	e.mu.RLock()
	s := e.searcher
	e.mu.RUnlock()

	if s == nil {
		return GetResult{Found: false}
	}

	query := search.NewTermQuery("_id", id)
	collector := search.NewTopKCollector(1)
	results := s.Search(query, collector)
	if len(results) == 0 {
		return GetResult{Found: false}
	}

	source := results[0].Fields["_source"]
	return GetResult{Found: true, Source: []byte(source)}
}

// docExistsInIndex checks whether a document with the given _id exists
// in the current Lucene searcher.
func (e *Engine) docExistsInIndex(id string) bool {
	e.mu.RLock()
	s := e.searcher
	e.mu.RUnlock()

	if s == nil {
		return false
	}

	query := search.NewTermQuery("_id", id)
	collector := search.NewTopKCollector(1)
	results := s.Search(query, collector)
	return len(results) > 0
}

// Flush flushes buffered documents, syncs the translog, rolls the generation,
// and trims unreferenced readers.
func (e *Engine) Flush() error {
	if err := e.writer.Flush(); err != nil {
		return err
	}

	if e.translog != nil {
		if err := e.translog.Sync(); err != nil {
			return fmt.Errorf("translog sync: %w", err)
		}
		if err := e.translog.RollGeneration(); err != nil {
			return fmt.Errorf("translog roll: %w", err)
		}
		e.translog.SetMinRequiredGeneration(e.translog.CurrentGeneration())
		if err := e.translog.TrimUnreferencedReaders(); err != nil {
			return fmt.Errorf("translog trim: %w", err)
		}
	}

	return nil
}

// Refresh opens a new NRT reader from the writer, making recently indexed
// documents visible to search. This mirrors Elasticsearch's refresh semantics.
func (e *Engine) Refresh() error {
	reader, err := goindex.OpenNRTReader(e.writer)
	if err != nil {
		return err
	}

	newSearcher := search.NewIndexSearcher(reader)

	e.mu.Lock()
	oldReader := e.reader
	e.reader = reader
	e.searcher = newSearcher
	e.mu.Unlock()

	// Clear the version map after the new reader is visible, since
	// documents are now searchable via the Lucene index.
	e.versionMap.Clear()

	if oldReader != nil {
		oldReader.Close()
	}
	return nil
}

// Searcher returns the current IndexSearcher. Returns nil if Refresh has
// never been called. The caller must not close the returned searcher.
func (e *Engine) Searcher() *search.IndexSearcher {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.searcher
}

// EngineStats holds document count statistics for the engine.
type EngineStats struct {
	DocCount     int
	DeletedCount int
}

// Stats returns document count statistics from the current reader.
func (e *Engine) Stats() EngineStats {
	e.mu.RLock()
	reader := e.reader
	e.mu.RUnlock()

	if reader == nil {
		return EngineStats{}
	}

	total := reader.TotalDocCount()
	live := reader.LiveDocCount()
	return EngineStats{DocCount: live, DeletedCount: total - live}
}

// Close shuts down the engine, closing the reader and writer.
func (e *Engine) Close() error {
	e.mu.Lock()
	reader := e.reader
	e.reader = nil
	e.searcher = nil
	e.mu.Unlock()

	if e.translog != nil {
		if err := e.translog.Close(); err != nil {
			return fmt.Errorf("close translog: %w", err)
		}
	}

	if reader != nil {
		reader.Close()
	}
	return e.writer.Close()
}
