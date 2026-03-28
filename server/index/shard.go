package index

import (
	"fmt"

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
func NewIndexShard(shardID int, indexName string, dir store.Directory, m *mapping.MappingDefinition, registry *analysis.AnalyzerRegistry) (*IndexShard, error) {
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

	engine, err := NewEngine(dir, fa)
	if err != nil {
		return nil, err
	}

	return &IndexShard{
		shardID:   shardID,
		indexName: indexName,
		engine:    engine,
		mapping:   m,
	}, nil
}

// Index parses the JSON source according to the mapping and indexes the document.
func (s *IndexShard) Index(id string, source []byte) error {
	doc, err := mapping.ParseDocument(id, source, s.mapping)
	if err != nil {
		return err
	}

	// Delete existing document with same ID first (update = delete + re-add).
	// SeqNo ordering in the IndexWriter ensures the delete only affects
	// documents added before it, not the newly added document.
	if err := s.engine.Delete("_id", id); err != nil {
		return err
	}

	return s.engine.Index(doc)
}

// Delete removes a document by its _id.
func (s *IndexShard) Delete(id string) error {
	return s.engine.Delete("_id", id)
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
