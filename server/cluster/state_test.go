// server/cluster/state_test.go
package cluster

import (
	"fmt"
	"testing"
)

func TestClusterState_EmptyMetadata(t *testing.T) {
	cs := NewClusterState()
	meta := cs.Metadata()
	if meta == nil {
		t.Fatal("expected non-nil metadata")
	}
	if len(meta.Indices) != 0 {
		t.Errorf("expected 0 indices, got %d", len(meta.Indices))
	}
}

func TestClusterState_UpdateMetadata(t *testing.T) {
	cs := NewClusterState()
	cs.UpdateMetadata(func(m *Metadata) *Metadata {
		m.Indices["test-index"] = &IndexMetadata{
			Name: "test-index",
			Settings: IndexSettings{
				NumberOfShards:   1,
				NumberOfReplicas: 0,
			},
			State: IndexStateOpen,
		}
		return m
	})

	meta := cs.Metadata()
	if len(meta.Indices) != 1 {
		t.Fatalf("expected 1 index, got %d", len(meta.Indices))
	}
	idx := meta.Indices["test-index"]
	if idx.Name != "test-index" {
		t.Errorf("expected name 'test-index', got %q", idx.Name)
	}
	if idx.State != IndexStateOpen {
		t.Errorf("expected state OPEN, got %v", idx.State)
	}
}

func TestNewClusterStateWith_CustomPersistedState(t *testing.T) {
	ps := NewInMemoryPersistedState()
	ps.SetMetadata(&Metadata{
		Indices: map[string]*IndexMetadata{
			"pre-existing": {Name: "pre-existing", State: IndexStateOpen},
		},
	})

	cs := NewClusterStateWith(ps)

	meta := cs.Metadata()
	if len(meta.Indices) != 1 {
		t.Fatalf("expected 1 index, got %d", len(meta.Indices))
	}
	if meta.Indices["pre-existing"] == nil {
		t.Error("expected 'pre-existing' index to exist")
	}
}

func TestClusterState_ConcurrentAccess(t *testing.T) {
	cs := NewClusterState()
	done := make(chan struct{})
	for i := range 10 {
		go func(n int) {
			defer func() { done <- struct{}{} }()
			cs.UpdateMetadata(func(m *Metadata) *Metadata {
				m.Indices[fmt.Sprintf("index-%d", n)] = &IndexMetadata{
					Name: fmt.Sprintf("index-%d", n),
				}
				return m
			})
			_ = cs.Metadata()
		}(i)
	}
	for range 10 {
		<-done
	}
	meta := cs.Metadata()
	if len(meta.Indices) != 10 {
		t.Errorf("expected 10 indices, got %d", len(meta.Indices))
	}
}
