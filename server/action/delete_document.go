package action

import (
	"fmt"

	"gosearch/server/cluster"
	"gosearch/server/index"
)

type DeleteDocumentRequest struct {
	Index         string
	ID            string
	IfSeqNo       *int64
	IfPrimaryTerm *int64
}

type DeleteDocumentResponse struct {
	Index       string
	ID          string
	SeqNo       int64
	PrimaryTerm int64
	Result      string // "deleted" or "not_found"
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
		return DeleteDocumentResponse{}, &IndexNotFoundError{Index: req.Index}
	}

	svc := a.indexServices[req.Index]
	if svc == nil {
		return DeleteDocumentResponse{}, &IndexNotFoundError{Index: req.Index}
	}

	shardID := index.RouteShard(req.ID, svc.NumShards())
	shard := svc.Shard(shardID)

	result, err := shard.Delete(req.ID, req.IfSeqNo, req.IfPrimaryTerm)
	if err != nil {
		return DeleteDocumentResponse{}, fmt.Errorf("delete document: %w", err)
	}

	resultStr := "not_found"
	if result.Found {
		resultStr = "deleted"
	}

	return DeleteDocumentResponse{
		Index:       req.Index,
		ID:          req.ID,
		SeqNo:       result.SeqNo,
		PrimaryTerm: result.PrimaryTerm,
		Result:      resultStr,
	}, nil
}
