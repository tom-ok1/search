package gateway

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"gosearch/analysis"
	"gosearch/server/cluster"
	"gosearch/server/index"
	"gosearch/server/mapping"
)

// GatewayMetaState handles loading persisted cluster state and recovering
// index services on node startup. This mirrors Elasticsearch's GatewayMetaState.
type GatewayMetaState struct{}

func NewGatewayMetaState() *GatewayMetaState {
	return &GatewayMetaState{}
}

// Start loads persisted cluster state from disk, recovers all index services,
// and returns the reconstructed ClusterState and index service map.
// If no state file exists, a fresh empty state is returned.
func (g *GatewayMetaState) Start(dataPath string, registry *analysis.AnalyzerRegistry) (*cluster.ClusterState, map[string]*index.IndexService, error) {
	stateDir := filepath.Join(dataPath, "_state")
	ps, err := cluster.NewFilePersistedState(stateDir)
	if err != nil {
		return nil, nil, fmt.Errorf("load persisted state: %w", err)
	}

	cs := cluster.NewClusterStateWith(ps)
	services := make(map[string]*index.IndexService)

	md := ps.GetMetadata()
	var removed []string

	for name, meta := range md.Indices {
		indexDataPath := filepath.Join(dataPath, "nodes", "0", "indices", name)

		svc, err := recoverIndexService(meta, indexDataPath, registry)
		if err != nil {
			log.Printf("WARNING: skipping index %q during recovery: %v", name, err)
			removed = append(removed, name)
			continue
		}

		// Open readers on all shards so existing data becomes searchable
		for i := 0; i < svc.NumShards(); i++ {
			shard := svc.Shard(i)
			if err := shard.Refresh(); err != nil {
				log.Printf("WARNING: failed to refresh shard %d of index %q: %v", i, name, err)
			}
		}

		services[name] = svc
	}

	// Clean up indices that failed to recover
	if len(removed) > 0 {
		cs.UpdateMetadata(func(md *cluster.Metadata) *cluster.Metadata {
			for _, name := range removed {
				delete(md.Indices, name)
			}
			return md
		})
	}

	return cs, services, nil
}

// recoverIndexService reopens an IndexService from existing shard data on disk.
func recoverIndexService(meta *cluster.IndexMetadata, indexDataPath string, registry *analysis.AnalyzerRegistry) (*index.IndexService, error) {
	// Verify the index data directory exists
	if _, err := os.Stat(indexDataPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("index data directory missing: %s", indexDataPath)
	}

	m := meta.Mapping
	if m == nil {
		m = &mapping.MappingDefinition{
			Properties: make(map[string]mapping.FieldMapping),
		}
	}

	return index.NewIndexService(meta, m, indexDataPath, registry)
}

// createIndexService is a convenience for creating a new IndexService
// (used by tests that need to create indices).
func createIndexService(meta *cluster.IndexMetadata, indexDataPath string, registry *analysis.AnalyzerRegistry) (*index.IndexService, error) {
	m := meta.Mapping
	if m == nil {
		m = &mapping.MappingDefinition{
			Properties: make(map[string]mapping.FieldMapping),
		}
	}
	return index.NewIndexService(meta, m, indexDataPath, registry)
}
