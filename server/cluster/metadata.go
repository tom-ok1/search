// server/cluster/metadata.go
package cluster

import (
	"time"

	"gosearch/server/mapping"
)

type IndexState int

const (
	IndexStateOpen IndexState = iota
	IndexStateClosed
)

type Metadata struct {
	Indices map[string]*IndexMetadata
}

type IndexMetadata struct {
	Name     string
	Settings IndexSettings
	Mapping  *mapping.MappingDefinition
	State    IndexState
}

type IndexSettings struct {
	NumberOfShards   int
	NumberOfReplicas int
	RefreshInterval  time.Duration // default 1s; -1 disables auto-refresh
}

const DefaultRefreshInterval = 1 * time.Second

func NewMetadata() *Metadata {
	return &Metadata{
		Indices: make(map[string]*IndexMetadata),
	}
}
