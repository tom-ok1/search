package action

import (
	"encoding/json"

	"gosearch/server/cluster"
	"gosearch/server/index"
)

type GetDocumentRequest struct {
	Index string
	ID    string
}

type GetDocumentResponse struct {
	Index       string
	ID          string
	SeqNo       int64
	PrimaryTerm int64
	Found       bool
	Source      json.RawMessage
}

type TransportGetAction struct {
	clusterState  *cluster.ClusterState
	indexServices map[string]*index.IndexService
}

func NewTransportGetAction(
	cs *cluster.ClusterState,
	services map[string]*index.IndexService,
) *TransportGetAction {
	return &TransportGetAction{
		clusterState:  cs,
		indexServices: services,
	}
}

func (a *TransportGetAction) Name() string {
	return "indices:data/read/get"
}

func (a *TransportGetAction) Execute(req GetDocumentRequest) (GetDocumentResponse, error) {
	if a.clusterState.Metadata().Indices[req.Index] == nil {
		return GetDocumentResponse{}, &IndexNotFoundError{Index: req.Index}
	}

	svc := a.indexServices[req.Index]
	if svc == nil {
		return GetDocumentResponse{}, &IndexNotFoundError{Index: req.Index}
	}

	shardID := index.RouteShard(req.ID, svc.NumShards())
	shard := svc.Shard(shardID)

	result := shard.Get(req.ID)

	return GetDocumentResponse{
		Index:       req.Index,
		ID:          req.ID,
		SeqNo:       result.SeqNo,
		PrimaryTerm: result.PrimaryTerm,
		Found:       result.Found,
		Source:      json.RawMessage(result.Source),
	}, nil
}
