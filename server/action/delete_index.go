package action

import (
	"fmt"
	"os"
	"path/filepath"

	"gosearch/server/cluster"
	"gosearch/server/index"
)

type DeleteIndexRequest struct {
	Name string
}

type DeleteIndexResponse struct {
	Acknowledged bool
}

type TransportDeleteIndexAction struct {
	clusterState  *cluster.ClusterState
	indexServices map[string]*index.IndexService
	dataPath      string
}

func NewTransportDeleteIndexAction(
	cs *cluster.ClusterState,
	services map[string]*index.IndexService,
	dataPath string,
) *TransportDeleteIndexAction {
	return &TransportDeleteIndexAction{
		clusterState:  cs,
		indexServices: services,
		dataPath:      dataPath,
	}
}

func (a *TransportDeleteIndexAction) Name() string {
	return "indices:admin/delete"
}

func (a *TransportDeleteIndexAction) Execute(req DeleteIndexRequest) (DeleteIndexResponse, error) {
	// Verify index exists
	if a.clusterState.Metadata().Indices[req.Name] == nil {
		return DeleteIndexResponse{}, fmt.Errorf("no such index [%s]", req.Name)
	}

	// Close IndexService
	svc := a.indexServices[req.Name]
	if svc != nil {
		if err := svc.Close(); err != nil {
			return DeleteIndexResponse{}, fmt.Errorf("close index [%s]: %w", req.Name, err)
		}
	}

	// Remove from cluster state
	a.clusterState.UpdateMetadata(func(md *cluster.Metadata) *cluster.Metadata {
		delete(md.Indices, req.Name)
		return md
	})

	// Remove from index services map
	delete(a.indexServices, req.Name)

	// Clean up data directory
	indexDataPath := filepath.Join(a.dataPath, "nodes", "0", "indices", req.Name)
	os.RemoveAll(indexDataPath)

	return DeleteIndexResponse{Acknowledged: true}, nil
}
