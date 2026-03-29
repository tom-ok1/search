package action

import (
	"testing"

	"gosearch/analysis"
	"gosearch/server/cluster"
	"gosearch/server/index"
)

func newTestDeps(t *testing.T) (*cluster.ClusterState, map[string]*index.IndexService, string, *analysis.AnalyzerRegistry) {
	t.Helper()
	cs := cluster.NewClusterState()
	services := make(map[string]*index.IndexService)
	dataPath := t.TempDir()
	registry := analysis.DefaultRegistry()
	return cs, services, dataPath, registry
}
