// server/cluster/metadata.go
package cluster

import (
	"encoding/json"
	"fmt"
	"time"

	"gosearch/server/mapping"
)

type IndexState int

const (
	IndexStateOpen IndexState = iota
	IndexStateClosed
)

func (s IndexState) MarshalJSON() ([]byte, error) {
	switch s {
	case IndexStateOpen:
		return json.Marshal("open")
	case IndexStateClosed:
		return json.Marshal("closed")
	default:
		return nil, fmt.Errorf("unknown index state: %d", s)
	}
}

func (s *IndexState) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	switch str {
	case "open":
		*s = IndexStateOpen
	case "closed":
		*s = IndexStateClosed
	default:
		return fmt.Errorf("unknown index state: %q", str)
	}
	return nil
}

type Metadata struct {
	Indices map[string]*IndexMetadata `json:"indices"`
}

type IndexMetadata struct {
	Name     string                     `json:"name"`
	Settings IndexSettings              `json:"settings"`
	Mapping  *mapping.MappingDefinition `json:"mapping,omitempty"`
	State    IndexState                 `json:"state"`
}

type IndexSettings struct {
	NumberOfShards   int
	NumberOfReplicas int
	RefreshInterval  time.Duration // default 1s; -1 disables auto-refresh
}

const DefaultRefreshInterval = 1 * time.Second

type indexSettingsJSON struct {
	NumberOfShards   int    `json:"number_of_shards"`
	NumberOfReplicas int    `json:"number_of_replicas"`
	RefreshInterval  string `json:"refresh_interval"`
}

func (s IndexSettings) MarshalJSON() ([]byte, error) {
	var ri string
	switch {
	case s.RefreshInterval == -1:
		ri = "-1"
	case s.RefreshInterval%time.Second == 0:
		ri = fmt.Sprintf("%ds", int(s.RefreshInterval.Seconds()))
	case s.RefreshInterval%time.Millisecond == 0:
		ri = fmt.Sprintf("%dms", s.RefreshInterval.Milliseconds())
	default:
		ri = s.RefreshInterval.String()
	}
	return json.Marshal(indexSettingsJSON{
		NumberOfShards:   s.NumberOfShards,
		NumberOfReplicas: s.NumberOfReplicas,
		RefreshInterval:  ri,
	})
}

func (s *IndexSettings) UnmarshalJSON(data []byte) error {
	var raw indexSettingsJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	s.NumberOfShards = raw.NumberOfShards
	s.NumberOfReplicas = raw.NumberOfReplicas

	if raw.RefreshInterval == "-1" {
		s.RefreshInterval = -1
		return nil
	}
	d, err := time.ParseDuration(raw.RefreshInterval)
	if err != nil {
		return fmt.Errorf("parse refresh_interval %q: %w", raw.RefreshInterval, err)
	}
	s.RefreshInterval = d
	return nil
}

func NewMetadata() *Metadata {
	return &Metadata{
		Indices: make(map[string]*IndexMetadata),
	}
}
