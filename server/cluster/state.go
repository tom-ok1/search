// server/cluster/state.go
package cluster

import "sync"

type ClusterState struct {
	mu             sync.RWMutex
	persistedState PersistedState
}

func NewClusterState() *ClusterState {
	return &ClusterState{
		persistedState: NewInMemoryPersistedState(),
	}
}

func (cs *ClusterState) Metadata() *Metadata {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.persistedState.GetMetadata()
}

func (cs *ClusterState) UpdateMetadata(fn func(*Metadata) *Metadata) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	newMd := fn(cs.persistedState.GetMetadata())
	cs.persistedState.SetMetadata(newMd)
}
