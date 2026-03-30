package action

import (
	"encoding/json"

	"gosearch/search"
	"gosearch/server/cluster"
	"gosearch/server/index"
)

type GetDocumentRequest struct {
	Index string
	ID    string
}

type GetDocumentResponse struct {
	Index  string
	ID     string
	Found  bool
	Source json.RawMessage
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

	searcher := shard.Searcher()
	if searcher == nil {
		return GetDocumentResponse{Index: req.Index, ID: req.ID, Found: false}, nil
	}

	query := search.NewTermQuery("_id", req.ID)
	collector := search.NewTopKCollector(1)
	results := searcher.Search(query, collector)

	if len(results) == 0 {
		return GetDocumentResponse{Index: req.Index, ID: req.ID, Found: false}, nil
	}

	source := results[0].Fields["_source"]

	return GetDocumentResponse{
		Index:  req.Index,
		ID:     req.ID,
		Found:  true,
		Source: json.RawMessage(source),
	}, nil
}
