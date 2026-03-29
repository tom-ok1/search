package action

import (
	"encoding/json"
	"fmt"

	"gosearch/server/cluster"
	"gosearch/server/index"
)

type IndexDocumentRequest struct {
	Index  string
	ID     string
	Source json.RawMessage
}

type IndexDocumentResponse struct {
	Index  string
	ID     string
	Result string // "created"
}

type TransportIndexAction struct {
	clusterState  *cluster.ClusterState
	indexServices map[string]*index.IndexService
}

func NewTransportIndexAction(
	cs *cluster.ClusterState,
	services map[string]*index.IndexService,
) *TransportIndexAction {
	return &TransportIndexAction{
		clusterState:  cs,
		indexServices: services,
	}
}

func (a *TransportIndexAction) Name() string {
	return "indices:data/write/index"
}

func (a *TransportIndexAction) Execute(req IndexDocumentRequest) (IndexDocumentResponse, error) {
	if a.clusterState.Metadata().Indices[req.Index] == nil {
		return IndexDocumentResponse{}, fmt.Errorf("no such index [%s]", req.Index)
	}

	svc := a.indexServices[req.Index]
	if svc == nil {
		return IndexDocumentResponse{}, fmt.Errorf("no such index [%s]", req.Index)
	}

	shardID := index.RouteShard(req.ID, svc.NumShards())
	shard := svc.Shard(shardID)

	if err := shard.Index(req.ID, req.Source); err != nil {
		return IndexDocumentResponse{}, fmt.Errorf("index document: %w", err)
	}

	return IndexDocumentResponse{
		Index:  req.Index,
		ID:     req.ID,
		Result: "created",
	}, nil
}
