package action

import (
	"fmt"
	"path/filepath"
	"regexp"

	"gosearch/analysis"
	"gosearch/server/cluster"
	"gosearch/server/index"
	"gosearch/server/mapping"
)

var validIndexName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

type CreateIndexRequest struct {
	Name     string
	Settings cluster.IndexSettings
	Mappings *mapping.MappingDefinition
}

type CreateIndexResponse struct {
	Acknowledged bool
	Index        string
}

type TransportCreateIndexAction struct {
	clusterState  *cluster.ClusterState
	indexServices map[string]*index.IndexService
	dataPath      string
	registry      *analysis.AnalyzerRegistry
}

func NewTransportCreateIndexAction(
	cs *cluster.ClusterState,
	services map[string]*index.IndexService,
	dataPath string,
	registry *analysis.AnalyzerRegistry,
) *TransportCreateIndexAction {
	return &TransportCreateIndexAction{
		clusterState:  cs,
		indexServices: services,
		dataPath:      dataPath,
		registry:      registry,
	}
}

func (a *TransportCreateIndexAction) Name() string {
	return "indices:admin/create"
}

func (a *TransportCreateIndexAction) Execute(req CreateIndexRequest) (CreateIndexResponse, error) {
	if req.Name == "" {
		return CreateIndexResponse{}, &InvalidIndexNameError{Index: "", Reason: "must not be empty"}
	}
	if !validIndexName.MatchString(req.Name) {
		return CreateIndexResponse{}, &InvalidIndexNameError{Index: req.Name, Reason: "must be lowercase, start with a letter or digit, and contain only [a-z0-9._-]"}
	}

	// Check for duplicate
	if a.clusterState.Metadata().Indices[req.Name] != nil {
		return CreateIndexResponse{}, &IndexAlreadyExistsError{Index: req.Name}
	}

	// Default to 1 shard if not specified
	numShards := req.Settings.NumberOfShards
	if numShards <= 0 {
		numShards = 1
	}

	// Default to empty mapping if not provided
	m := req.Mappings
	if m == nil {
		m = &mapping.MappingDefinition{
			Properties: make(map[string]mapping.FieldMapping),
		}
	}

	// Build IndexMetadata
	meta := &cluster.IndexMetadata{
		Name: req.Name,
		Settings: cluster.IndexSettings{
			NumberOfShards:   numShards,
			NumberOfReplicas: req.Settings.NumberOfReplicas,
		},
		Mapping: m,
		State:   cluster.IndexStateOpen,
	}

	// Create IndexService
	indexDataPath := filepath.Join(a.dataPath, "nodes", "0", "indices", req.Name)
	svc, err := index.NewIndexService(meta, m, indexDataPath, a.registry)
	if err != nil {
		return CreateIndexResponse{}, fmt.Errorf("create index service: %w", err)
	}

	// Update cluster state
	a.clusterState.UpdateMetadata(func(md *cluster.Metadata) *cluster.Metadata {
		md.Indices[req.Name] = meta
		return md
	})

	// Register index service
	a.indexServices[req.Name] = svc

	return CreateIndexResponse{
		Acknowledged: true,
		Index:        req.Name,
	}, nil
}
