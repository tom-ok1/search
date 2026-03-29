// server/cluster/persisted_state.go
package cluster

// PersistedState abstracts the storage of cluster metadata.
// Implementations handle how metadata is stored and retrieved —
// in memory, on disk as JSON, etc.
type PersistedState interface {
	GetMetadata() *Metadata
	SetMetadata(metadata *Metadata)
}

// InMemoryPersistedState stores cluster metadata in memory only.
// State is lost when the process exits.
type InMemoryPersistedState struct {
	metadata *Metadata
}

func NewInMemoryPersistedState() *InMemoryPersistedState {
	return &InMemoryPersistedState{
		metadata: NewMetadata(),
	}
}

func (s *InMemoryPersistedState) GetMetadata() *Metadata {
	return s.metadata
}

func (s *InMemoryPersistedState) SetMetadata(metadata *Metadata) {
	s.metadata = metadata
}
