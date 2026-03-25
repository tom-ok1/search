// server/cluster/state.go
package cluster

import "sync"

type ClusterState struct {
	mu       sync.RWMutex
	metadata *Metadata
}

func NewClusterState() *ClusterState {
	return &ClusterState{
		metadata: NewMetadata(),
	}
}

func (cs *ClusterState) Metadata() *Metadata {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.metadata
}

func (cs *ClusterState) UpdateMetadata(fn func(*Metadata) *Metadata) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.metadata = fn(cs.metadata)
}
