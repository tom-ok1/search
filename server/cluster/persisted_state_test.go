// server/cluster/persisted_state_test.go
package cluster

import "testing"

func TestInMemoryPersistedState_InitialMetadata(t *testing.T) {
	ps := NewInMemoryPersistedState()
	meta := ps.GetMetadata()
	if meta == nil {
		t.Fatal("expected non-nil metadata")
	}
	if len(meta.Indices) != 0 {
		t.Errorf("expected 0 indices, got %d", len(meta.Indices))
	}
}

func TestInMemoryPersistedState_SetAndGet(t *testing.T) {
	ps := NewInMemoryPersistedState()

	newMeta := &Metadata{
		Indices: map[string]*IndexMetadata{
			"idx": {Name: "idx", State: IndexStateOpen},
		},
	}
	ps.SetMetadata(newMeta)

	got := ps.GetMetadata()
	if len(got.Indices) != 1 {
		t.Fatalf("expected 1 index, got %d", len(got.Indices))
	}
	if got.Indices["idx"].Name != "idx" {
		t.Errorf("expected name 'idx', got %q", got.Indices["idx"].Name)
	}
}
