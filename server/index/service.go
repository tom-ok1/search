package index

import (
	"fmt"
	"path/filepath"
	"time"

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
	stopCh   chan struct{}
}

// NewIndexService creates a new IndexService, initializing all shards.
// dataPath is the base directory for this index (e.g., data/nodes/0/indices/{index_name}).
func NewIndexService(meta *cluster.IndexMetadata, m *mapping.MappingDefinition, dataPath string, registry *analysis.AnalyzerRegistry) (*IndexService, error) {
	shards := make(map[int]*IndexShard, meta.Settings.NumberOfShards)

	for i := 0; i < meta.Settings.NumberOfShards; i++ {
		shardDataPath := filepath.Join(dataPath, fmt.Sprintf("%d", i))
		shardPath := filepath.Join(shardDataPath, "index")
		dir, err := store.NewFSDirectory(shardPath)
		if err != nil {
			// Close already-created shards on error
			for _, s := range shards {
				s.Close()
			}
			return nil, fmt.Errorf("create shard %d directory: %w", i, err)
		}

		shard, err := NewIndexShard(i, meta.Name, dir, m, registry, shardDataPath)
		if err != nil {
			for _, s := range shards {
				s.Close()
			}
			return nil, fmt.Errorf("create shard %d: %w", i, err)
		}
		shards[i] = shard
	}

	svc := &IndexService{
		metadata: meta,
		mapping:  m,
		shards:   shards,
		stopCh:   make(chan struct{}),
	}

	if meta.Settings.RefreshInterval > 0 {
		go svc.scheduleRefresh(meta.Settings.RefreshInterval)
	}

	return svc, nil
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

// IndexStats holds aggregate document count statistics for the index.
type IndexStats struct {
	DocCount     int
	DeletedCount int
	ShardCount   int
}

// Stats returns aggregate document count statistics across all shards.
func (is *IndexService) Stats() IndexStats {
	var total IndexStats
	total.ShardCount = len(is.shards)
	for _, shard := range is.shards {
		ss := shard.Stats()
		total.DocCount += ss.DocCount
		total.DeletedCount += ss.DeletedCount
	}
	return total
}

// Close shuts down all shards in this index.
func (is *IndexService) Close() error {
	close(is.stopCh)

	var firstErr error
	for _, shard := range is.shards {
		if err := shard.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// scheduleRefresh periodically refreshes all shards at the given interval.
// It stops when stopCh is closed.
func (is *IndexService) scheduleRefresh(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-is.stopCh:
			return
		case <-ticker.C:
			for _, shard := range is.shards {
				shard.Refresh()
			}
		}
	}
}

// RouteShard returns the shard ID for a given document ID using Murmur3 hashing,
// matching Elasticsearch's shard routing (Murmur3HashFunction + Math.floorMod).
func RouteShard(id string, numShards int) int {
	hash := int(Murmur3Hash([]byte(id), 0))
	mod := hash % numShards
	if mod < 0 {
		mod += numShards
	}
	return mod
}
