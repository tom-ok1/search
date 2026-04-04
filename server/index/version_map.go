package index

import "sync"

// VersionValue holds the version, source document bytes, sequence number,
// primary term, and deletion status for a single document in the LiveVersionMap.
type VersionValue struct {
	Version     int64
	SeqNo       int64
	PrimaryTerm int64
	Source      []byte
	Deleted     bool
}

// LiveVersionMap is an in-memory map from document _id to its latest version
// and source bytes. This enables real-time GET without requiring a Lucene refresh,
// mirroring Elasticsearch's LiveVersionMap from InternalEngine.
type LiveVersionMap struct {
	mu      sync.RWMutex
	entries map[string]VersionValue
}

// NewLiveVersionMap creates a new empty LiveVersionMap.
func NewLiveVersionMap() *LiveVersionMap {
	return &LiveVersionMap{
		entries: make(map[string]VersionValue),
	}
}

// Get returns the VersionValue for the given document id.
// The second return value indicates whether the id was found.
func (m *LiveVersionMap) Get(id string) (VersionValue, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	vv, ok := m.entries[id]
	return vv, ok
}

// Put stores or updates the VersionValue for the given document id.
func (m *LiveVersionMap) Put(id string, vv VersionValue) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[id] = vv
}

// Clear removes all entries from the map.
func (m *LiveVersionMap) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = make(map[string]VersionValue)
}
