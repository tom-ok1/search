package action

import (
	"gosearch/server/cluster"
	"gosearch/server/mapping"
)

type GetIndexRequest struct {
	Name string
}

type GetIndexResponse struct {
	Name     string
	Settings cluster.IndexSettings
	Mapping  *mapping.MappingDefinition
}

type TransportGetIndexAction struct {
	clusterState *cluster.ClusterState
}

func NewTransportGetIndexAction(cs *cluster.ClusterState) *TransportGetIndexAction {
	return &TransportGetIndexAction{clusterState: cs}
}

func (a *TransportGetIndexAction) Name() string {
	return "indices:admin/get"
}

func (a *TransportGetIndexAction) Execute(req GetIndexRequest) (GetIndexResponse, error) {
	meta := a.clusterState.Metadata().Indices[req.Name]
	if meta == nil {
		return GetIndexResponse{}, &IndexNotFoundError{Index: req.Name}
	}

	return GetIndexResponse{
		Name:     meta.Name,
		Settings: meta.Settings,
		Mapping:  meta.Mapping,
	}, nil
}
