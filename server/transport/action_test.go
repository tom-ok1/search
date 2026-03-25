// server/transport/action_test.go
package transport

import "testing"

type mockAction struct {
	name string
}

func (a *mockAction) Name() string { return a.name }

func TestActionRegistry_RegisterAndGet(t *testing.T) {
	reg := NewActionRegistry()
	action := &mockAction{name: "indices:data/read/search"}
	reg.Register(action)

	got := reg.Get("indices:data/read/search")
	if got == nil {
		t.Fatal("expected action, got nil")
	}
	if got.Name() != "indices:data/read/search" {
		t.Errorf("expected name 'indices:data/read/search', got %q", got.Name())
	}
}

func TestActionRegistry_GetMissing(t *testing.T) {
	reg := NewActionRegistry()
	got := reg.Get("nonexistent")
	if got != nil {
		t.Errorf("expected nil for missing action, got %v", got)
	}
}
