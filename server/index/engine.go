package index

import (
	"sync"

	"gosearch/analysis"
	"gosearch/document"
	goindex "gosearch/index"
	"gosearch/search"
	"gosearch/store"
)

const defaultBufferSize = 1000

// Engine wraps an IndexWriter and manages the IndexReader/IndexSearcher lifecycle.
// It mirrors Elasticsearch's InternalEngine.
type Engine struct {
	writer   *goindex.IndexWriter
	reader   *goindex.IndexReader
	searcher *search.IndexSearcher
	dir      store.Directory
	mu       sync.RWMutex // protects reader/searcher swap on refresh
}

// NewEngine creates a new Engine backed by the given directory and field analyzers.
func NewEngine(dir store.Directory, fieldAnalyzers *analysis.FieldAnalyzers) (*Engine, error) {
	writer := goindex.NewIndexWriter(dir, fieldAnalyzers, defaultBufferSize)
	return &Engine{
		writer: writer,
		dir:    dir,
	}, nil
}

// Index adds a document to the engine's writer.
func (e *Engine) Index(doc *document.Document) error {
	return e.writer.AddDocument(doc)
}

// Delete removes all documents matching the given field/value term.
func (e *Engine) Delete(field, value string) error {
	return e.writer.DeleteDocuments(field, value)
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

	if reader != nil {
		reader.Close()
	}
	return e.writer.Close()
}
