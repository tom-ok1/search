package transport

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// noopResponseHandler is a stub ResponseHandler for registry tests.
type noopResponseHandler struct{}

func (noopResponseHandler) ReadAndHandle(*StreamInput) error  { return nil }
func (noopResponseHandler) HandleError(*RemoteTransportError) {}

// Test types for typed dispatch test
type testMsg struct{ Value string }

func (m *testMsg) WriteTo(out *StreamOutput) error { return out.WriteString(m.Value) }

func readTestMsg(in *StreamInput) (*testMsg, error) {
	v, err := in.ReadString()
	return &testMsg{Value: v}, err
}

func TestResponseHandlers_AddAndRemove(t *testing.T) {
	rh := NewResponseHandlers()

	reqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx := &ResponseContext{
		Handler: noopResponseHandler{},
		Action:  "test-action",
		NodeID:  "node-1",
		Ctx:     reqCtx,
		Cancel:  cancel,
	}

	id := rh.Add(ctx)
	if id <= 0 {
		t.Fatalf("expected positive ID, got %d", id)
	}

	retrieved := rh.Remove(id)
	if retrieved == nil {
		t.Fatal("expected to retrieve context, got nil")
	}
	if retrieved.Action != "test-action" {
		t.Errorf("expected action test-action, got %s", retrieved.Action)
	}
	if retrieved.NodeID != "node-1" {
		t.Errorf("expected nodeID node-1, got %s", retrieved.NodeID)
	}

	// Second remove should return nil
	second := rh.Remove(id)
	if second != nil {
		t.Errorf("expected nil on second remove, got %v", second)
	}
}

func TestResponseHandlers_IDsAreUnique(t *testing.T) {
	rh := NewResponseHandlers()
	seen := make(map[int64]bool)

	for i := range 100 {
		ctx := &ResponseContext{Action: fmt.Sprintf("action-%d", i)}
		id := rh.Add(ctx)
		if seen[id] {
			t.Fatalf("duplicate ID detected: %d", id)
		}
		seen[id] = true
	}

	if len(seen) != 100 {
		t.Fatalf("expected 100 unique IDs, got %d", len(seen))
	}
}

func TestResponseHandlers_ConcurrentAddRemove(t *testing.T) {
	rh := NewResponseHandlers()
	var wg sync.WaitGroup

	for i := range 100 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ctx := &ResponseContext{Action: fmt.Sprintf("action-%d", n)}
			id := rh.Add(ctx)
			time.Sleep(time.Millisecond)
			retrieved := rh.Remove(id)
			if retrieved == nil {
				t.Errorf("goroutine %d: failed to retrieve context", n)
			}
		}(i)
	}

	wg.Wait()
}

func TestRequestHandlerMap_RegisterAndGet(t *testing.T) {
	m := NewRequestHandlerMap()

	entry := &requestHandlerEntry{
		action:   "test-action",
		executor: PoolGeneric,
		dispatch: func(payload *StreamInput, channel TransportChannel) {},
	}

	m.Register(entry)

	retrieved := m.Get("test-action")
	if retrieved == nil {
		t.Fatal("expected to retrieve entry, got nil")
	}
	if retrieved.action != "test-action" {
		t.Errorf("expected action test-action, got %s", retrieved.action)
	}
	if retrieved.executor != PoolGeneric {
		t.Errorf("expected executor %s, got %s", PoolGeneric, retrieved.executor)
	}

	// Get unknown action
	unknown := m.Get("unknown-action")
	if unknown != nil {
		t.Errorf("expected nil for unknown action, got %v", unknown)
	}
}

func TestRegisterHandler_TypedDispatch(t *testing.T) {
	m := NewRequestHandlerMap()

	var receivedMsg *testMsg
	handler := func(request *testMsg, channel TransportChannel) error {
		receivedMsg = request
		return channel.SendResponse(&testMsg{Value: "response:" + request.Value})
	}

	registerHandler(m, "test-action", PoolGeneric, readTestMsg, handler)

	entry := m.Get("test-action")
	if entry == nil {
		t.Fatal("expected to retrieve registered handler")
	}

	// Create a test message
	var inputBuf bytes.Buffer
	out := NewStreamOutput(&inputBuf)
	testInput := &testMsg{Value: "hello"}
	if err := testInput.WriteTo(out); err != nil {
		t.Fatalf("failed to serialize test input: %v", err)
	}

	// Create a mock channel
	var responseBuf bytes.Buffer
	channel := &mockTransportChannel{writer: &responseBuf}

	// Dispatch the message
	in := NewStreamInput(&inputBuf)
	entry.dispatch(in, channel)

	// Verify the handler was called
	if receivedMsg == nil {
		t.Fatal("handler was not called")
	}
	if receivedMsg.Value != "hello" {
		t.Errorf("expected Value=hello, got %s", receivedMsg.Value)
	}

	// Verify response was sent
	if !channel.responseSent {
		t.Error("expected response to be sent")
	}
}

// Mock channel for testing
type mockTransportChannel struct {
	writer       *bytes.Buffer
	responseSent bool
	errorSent    bool
}

func (m *mockTransportChannel) SendResponse(response Writeable) error {
	m.responseSent = true
	return response.WriteTo(NewStreamOutput(m.writer))
}

func (m *mockTransportChannel) SendError(err error) error {
	m.errorSent = true
	return nil
}
