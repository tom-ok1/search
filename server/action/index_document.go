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
	Index   string
	ID      string
	Version int64
	Result  string // "created" or "updated"
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
		return IndexDocumentResponse{}, &IndexNotFoundError{Index: req.Index}
	}

	svc := a.indexServices[req.Index]
	if svc == nil {
		return IndexDocumentResponse{}, &IndexNotFoundError{Index: req.Index}
	}

	shardID := index.RouteShard(req.ID, svc.NumShards())
	shard := svc.Shard(shardID)

	result, err := shard.Index(req.ID, req.Source)
	if err != nil {
		return IndexDocumentResponse{}, fmt.Errorf("index document: %w", err)
	}

	resultStr := "updated"
	if result.Created {
		resultStr = "created"
	}

	return IndexDocumentResponse{
		Index:   req.Index,
		ID:      req.ID,
		Version: result.Version,
		Result:  resultStr,
	}, nil
}
