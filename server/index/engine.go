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

// IndexResult holds the outcome of an index operation.
type IndexResult struct {
	Version int64
	Created bool
}

// DeleteResult holds the outcome of a delete operation.
type DeleteResult struct {
	Version int64
	Found   bool
}

// GetResult holds the outcome of a real-time get operation.
type GetResult struct {
	Found   bool
	Version int64
	Source  []byte
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
	maxVersion atomic.Int64
	mu         sync.RWMutex // protects reader/searcher swap on refresh
}

// NewEngine creates a new Engine backed by the given directory and field analyzers.
// If translogPath is non-empty, a translog is created for durability.
func NewEngine(dir store.Directory, fieldAnalyzers *analysis.FieldAnalyzers, translogPath string) (*Engine, error) {
	writer := goindex.NewIndexWriter(dir, fieldAnalyzers, defaultBufferSize)

	e := &Engine{
		writer:     writer,
		dir:        dir,
		versionMap: NewLiveVersionMap(),
	}

	if translogPath != "" {
		tl, err := translog.NewTranslog(translogPath)
		if err != nil {
			writer.Close()
			return nil, fmt.Errorf("open translog: %w", err)
		}
		e.translog = tl
	}

	return e, nil
}

// RecoverFromTranslog replays uncommitted operations from the translog.
// The caller provides replay functions that have the necessary context
// (e.g., mapping) to reconstruct documents.
func (e *Engine) RecoverFromTranslog(replayIndex func(id string, source []byte) error, replayDelete func(id string) error) error {
	if e.translog == nil {
		return nil
	}

	ops, err := e.translog.Recover()
	if err != nil {
		return fmt.Errorf("recover translog: %w", err)
	}

	for _, op := range ops {
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
	}

	return nil
}

// Index adds or updates a document in the engine. It returns whether the
// document was newly created and the assigned version.
func (e *Engine) Index(id string, doc *document.Document, source []byte) (IndexResult, error) {
	var created bool
	vv, exists := e.versionMap.Get(id)
	if !exists {
		created = !e.docExistsInIndex(id)
	} else {
		created = vv.Deleted
	}

	// Delete-then-add (update semantics)
	e.writer.DeleteDocuments("_id", id)
	if err := e.writer.AddDocument(doc); err != nil {
		return IndexResult{}, err
	}

	version := e.maxVersion.Add(1)
	e.versionMap.Put(id, VersionValue{Version: version, Source: source, Deleted: false})

	if e.translog != nil {
		tlOp := &translog.IndexOperation{ID: id, Source: source, Version: version}
		if err := e.translog.Add(tlOp); err != nil {
			return IndexResult{}, fmt.Errorf("translog add: %w", err)
		}
	}

	return IndexResult{Version: version, Created: created}, nil
}

// Delete removes a document by its _id. It returns whether the document
// was found and the assigned version.
func (e *Engine) Delete(id string) (DeleteResult, error) {
	_, exists := e.versionMap.Get(id)
	found := exists || e.docExistsInIndex(id)

	if err := e.writer.DeleteDocuments("_id", id); err != nil {
		return DeleteResult{}, err
	}

	version := e.maxVersion.Add(1)
	e.versionMap.Put(id, VersionValue{Version: version, Deleted: true})

	if e.translog != nil {
		tlOp := &translog.DeleteOperation{ID: id, Version: version}
		if err := e.translog.Add(tlOp); err != nil {
			return DeleteResult{}, fmt.Errorf("translog add: %w", err)
		}
	}

	return DeleteResult{Version: version, Found: found}, nil
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
		return GetResult{Found: true, Version: vv.Version, Source: vv.Source}
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
	return GetResult{Found: true, Version: 0, Source: []byte(source)}
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

// Flush flushes buffered documents and applies pending deletes to disk segments
// without opening a new reader.
func (e *Engine) Flush() error {
	return e.writer.Flush()
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

// Close shuts down the engine, closing the translog, reader, and writer.
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
