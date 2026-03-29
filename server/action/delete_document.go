package action

import (
	"fmt"

	"gosearch/server/cluster"
	"gosearch/server/index"
)

type DeleteDocumentRequest struct {
	Index string
	ID    string
}

type DeleteDocumentResponse struct {
	Index  string
	ID     string
	Result string // "deleted"
}

type TransportDeleteDocumentAction struct {
	clusterState  *cluster.ClusterState
	indexServices map[string]*index.IndexService
}

func NewTransportDeleteDocumentAction(
	cs *cluster.ClusterState,
	services map[string]*index.IndexService,
) *TransportDeleteDocumentAction {
	return &TransportDeleteDocumentAction{
		clusterState:  cs,
		indexServices: services,
	}
}

func (a *TransportDeleteDocumentAction) Name() string {
	return "indices:data/write/delete"
}

func (a *TransportDeleteDocumentAction) Execute(req DeleteDocumentRequest) (DeleteDocumentResponse, error) {
	if a.clusterState.Metadata().Indices[req.Index] == nil {
		return DeleteDocumentResponse{}, fmt.Errorf("no such index [%s]", req.Index)
	}

	svc := a.indexServices[req.Index]
	if svc == nil {
		return DeleteDocumentResponse{}, fmt.Errorf("no such index [%s]", req.Index)
	}

	shardID := index.RouteShard(req.ID, svc.NumShards())
	shard := svc.Shard(shardID)

	if err := shard.Delete(req.ID); err != nil {
		return DeleteDocumentResponse{}, fmt.Errorf("delete document: %w", err)
	}

	return DeleteDocumentResponse{
		Index:  req.Index,
		ID:     req.ID,
		Result: "deleted",
	}, nil
}
