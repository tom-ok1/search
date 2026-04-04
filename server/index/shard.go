package index

import (
	"fmt"
	"path/filepath"

	"gosearch/analysis"
	"gosearch/search"
	"gosearch/server/mapping"
	"gosearch/store"
)

// IndexShard represents a single shard of an index. It owns an Engine
// and provides document-level operations. This mirrors Elasticsearch's IndexShard.
type IndexShard struct {
	shardID   int
	indexName string
	engine    *Engine
	mapping   *mapping.MappingDefinition
}

// NewIndexShard creates a new IndexShard backed by the given directory.
// It builds per-field analyzers from the mapping and registry.
// dataPath is the shard's data directory used for the translog; if empty, no translog is used.
func NewIndexShard(shardID int, indexName string, dir store.Directory, m *mapping.MappingDefinition, registry *analysis.AnalyzerRegistry, dataPath string) (*IndexShard, error) {
	fa := analysis.NewFieldAnalyzers(registry.Get("standard"))
	for fieldName, fm := range m.Properties {
		if fm.Analyzer != "" {
			a := registry.Get(fm.Analyzer)
			if a == nil {
				return nil, fmt.Errorf("unknown analyzer [%s] for field [%s]", fm.Analyzer, fieldName)
			}
			fa.SetFieldAnalyzer(fieldName, a)
		}
	}

	var translogPath string
	if dataPath != "" {
		translogPath = filepath.Join(dataPath, "translog")
	}

	engine, err := NewEngine(dir, fa, translogPath)
	if err != nil {
		return nil, err
	}

	shard := &IndexShard{
		shardID:   shardID,
		indexName: indexName,
		engine:    engine,
		mapping:   m,
	}

	// Recover uncommitted operations from the translog.
	if err := engine.RecoverFromTranslog(
		func(id string, source []byte) error {
			doc, err := mapping.ParseDocument(id, source, m)
			if err != nil {
				return err
			}
			_, err = engine.Index(id, doc, source, nil, nil)
			return err
		},
		func(id string) error {
			_, err := engine.Delete(id, nil, nil)
			return err
		},
	); err != nil {
		engine.Close()
		return nil, fmt.Errorf("recover from translog: %w", err)
	}

	return shard, nil
}

// Index parses the JSON source according to the mapping and indexes the document.
func (s *IndexShard) Index(id string, source []byte, ifSeqNo *int64, ifPrimaryTerm *int64) (IndexResult, error) {
	doc, err := mapping.ParseDocument(id, source, s.mapping)
	if err != nil {
		return IndexResult{}, err
	}

	return s.engine.Index(id, doc, source, ifSeqNo, ifPrimaryTerm)
}

// Delete removes a document by its _id.
func (s *IndexShard) Delete(id string, ifSeqNo *int64, ifPrimaryTerm *int64) (DeleteResult, error) {
	return s.engine.Delete(id, ifSeqNo, ifPrimaryTerm)
}

// Get performs a real-time get for a document by its _id.
func (s *IndexShard) Get(id string) GetResult {
	return s.engine.Get(id)
}

// Refresh makes recently indexed documents visible to search.
func (s *IndexShard) Refresh() error {
	return s.engine.Refresh()
}

// Searcher returns the current IndexSearcher for this shard.
func (s *IndexShard) Searcher() *search.IndexSearcher {
	return s.engine.Searcher()
}

// ShardID returns this shard's numeric ID.
func (s *IndexShard) ShardID() int {
	return s.shardID
}

// IndexName returns the name of the index this shard belongs to.
func (s *IndexShard) IndexName() string {
	return s.indexName
}

// Close shuts down the shard's engine.
func (s *IndexShard) Close() error {
	return s.engine.Close()
}
