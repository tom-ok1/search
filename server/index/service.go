package index

import (
	"fmt"
	"hash/fnv"
	"path/filepath"

	"gosearch/analysis"
	"gosearch/server/cluster"
	"gosearch/server/mapping"
	"gosearch/store"
)

// IndexService manages all shards for a single index.
// This mirrors Elasticsearch's IndexService.
type IndexService struct {
	metadata *cluster.IndexMetadata
	mapping  *mapping.MappingDefinition
	shards   map[int]*IndexShard
}

// NewIndexService creates a new IndexService, initializing all shards.
// dataPath is the base directory for this index (e.g., data/nodes/0/indices/{index_name}).
func NewIndexService(meta *cluster.IndexMetadata, m *mapping.MappingDefinition, dataPath string, analyzer *analysis.Analyzer) (*IndexService, error) {
	shards := make(map[int]*IndexShard, meta.NumShards)

	for i := 0; i < meta.NumShards; i++ {
		shardPath := filepath.Join(dataPath, fmt.Sprintf("%d", i), "index")
		dir, err := store.NewFSDirectory(shardPath)
		if err != nil {
			// Close already-created shards on error
			for _, s := range shards {
				s.Close()
			}
			return nil, fmt.Errorf("create shard %d directory: %w", i, err)
		}

		shard, err := NewIndexShard(i, meta.Name, dir, m, analyzer)
		if err != nil {
			for _, s := range shards {
				s.Close()
			}
			return nil, fmt.Errorf("create shard %d: %w", i, err)
		}
		shards[i] = shard
	}

	return &IndexService{
		metadata: meta,
		mapping:  m,
		shards:   shards,
	}, nil
}

// Shard returns the IndexShard with the given ID, or nil if not found.
func (is *IndexService) Shard(id int) *IndexShard {
	return is.shards[id]
}

// Mapping returns the mapping definition for this index.
func (is *IndexService) Mapping() *mapping.MappingDefinition {
	return is.mapping
}

// NumShards returns the number of shards in this index.
func (is *IndexService) NumShards() int {
	return len(is.shards)
}

// Close shuts down all shards in this index.
func (is *IndexService) Close() error {
	var firstErr error
	for _, shard := range is.shards {
		if err := shard.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// RouteShard returns the shard ID for a given document ID using consistent hashing.
func RouteShard(id string, numShards int) int {
	h := fnv.New32a()
	h.Write([]byte(id))
	return int(h.Sum32() % uint32(numShards))
}
