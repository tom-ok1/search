package action

import (
	"fmt"

	"gosearch/server/cluster"
	"gosearch/server/index"
)

type RefreshRequest struct {
	Index string
}

type RefreshResponse struct {
	Shards int
}

type TransportRefreshAction struct {
	clusterState  *cluster.ClusterState
	indexServices map[string]*index.IndexService
}

func NewTransportRefreshAction(
	cs *cluster.ClusterState,
	services map[string]*index.IndexService,
) *TransportRefreshAction {
	return &TransportRefreshAction{
		clusterState:  cs,
		indexServices: services,
	}
}

func (a *TransportRefreshAction) Name() string {
	return "indices:admin/refresh"
}

func (a *TransportRefreshAction) Execute(req RefreshRequest) (RefreshResponse, error) {
	if a.clusterState.Metadata().Indices[req.Index] == nil {
		return RefreshResponse{}, fmt.Errorf("no such index [%s]", req.Index)
	}

	svc := a.indexServices[req.Index]
	if svc == nil {
		return RefreshResponse{}, fmt.Errorf("no such index [%s]", req.Index)
	}

	for i := 0; i < svc.NumShards(); i++ {
		shard := svc.Shard(i)
		if err := shard.Refresh(); err != nil {
			return RefreshResponse{}, fmt.Errorf("refresh shard %d: %w", i, err)
		}
	}

	return RefreshResponse{Shards: svc.NumShards()}, nil
}
