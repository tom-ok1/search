// server/cluster/metadata.go
package cluster

type IndexState int

const (
	IndexStateOpen IndexState = iota
	IndexStateClosed
)

type Metadata struct {
	Indices map[string]*IndexMetadata
}

type IndexMetadata struct {
	Name      string
	Settings  IndexSettings
	NumShards int
	State     IndexState
}

type IndexSettings struct {
	NumberOfShards   int
	NumberOfReplicas int
}

func NewMetadata() *Metadata {
	return &Metadata{
		Indices: make(map[string]*IndexMetadata),
	}
}
