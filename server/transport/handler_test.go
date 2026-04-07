package transport

import (
	"bytes"
	"testing"
	"time"
)

type testRequest struct{ Value string }

func (r *testRequest) WriteTo(out *StreamOutput) error { return out.WriteString(r.Value) }
func readTestRequest(in *StreamInput) (*testRequest, error) {
	v, err := in.ReadString()
	return &testRequest{Value: v}, err
}

type testResponse struct{ Result string }

func (r *testResponse) WriteTo(out *StreamOutput) error { return out.WriteString(r.Result) }

func TestOutboundHandler_SendRequest(t *testing.T) {
	var buf bytes.Buffer
	oh := NewOutboundHandler()

	req := &testRequest{Value: "hello"}
	if err := oh.SendRequest(&buf, 42, "test:action", req); err != nil {
		t.Fatalf("SendRequest failed: %v", err)
	}

	h, err := ReadHeader(&buf)
	if err != nil {
		t.Fatalf("ReadHeader failed: %v", err)
	}
	if h.RequestID != 42 {
		t.Errorf("RequestID = %d, want 42", h.RequestID)
	}
	if !h.Status.IsRequest() {
		t.Error("expected IsRequest=true")
	}
	if h.Action != "test:action" {
		t.Errorf("Action = %q, want %q", h.Action, "test:action")
	}
}

func TestOutboundHandler_SendResponse(t *testing.T) {
	var buf bytes.Buffer
	oh := NewOutboundHandler()

	resp := &testResponse{Result: "ok"}
	if err := oh.SendResponse(&buf, 42, resp); err != nil {
		t.Fatalf("SendResponse failed: %v", err)
	}

	h, err := ReadHeader(&buf)
	if err != nil {
		t.Fatalf("ReadHeader failed: %v", err)
	}
	if h.RequestID != 42 {
		t.Errorf("RequestID = %d, want 42", h.RequestID)
	}
	if h.Status.IsRequest() {
		t.Error("expected IsRequest=false")
	}
}

func TestInboundHandler_DispatchRequest(t *testing.T) {
	handlers := NewRequestHandlerMap()
	done := make(chan *testRequest, 1)

	RegisterHandler(handlers, "test:echo", "generic", readTestRequest,
		func(req *testRequest, ch TransportChannel) error {
			done <- req
			return ch.SendResponse(&testResponse{Result: "echo:" + req.Value})
		},
	)

	tp := NewThreadPool(map[string]PoolConfig{
		"generic": {Workers: 0}, // DirectExecutor for synchronous dispatch
	})
	defer tp.Shutdown()

	rh := NewResponseHandlers()
	ih := NewInboundHandler(handlers, rh, tp, "test-node")

	// Write a request message using OutboundHandler
	var msgBuf bytes.Buffer
	oh := NewOutboundHandler()
	if err := oh.SendRequest(&msgBuf, 99, "test:echo", &testRequest{Value: "world"}); err != nil {
		t.Fatalf("SendRequest failed: %v", err)
	}

	// HandleMessage dispatches the request
	var respBuf bytes.Buffer
	if err := ih.HandleMessage(&msgBuf, &respBuf); err != nil {
		t.Fatalf("HandleMessage failed: %v", err)
	}

	// Wait for the handler to be called
	select {
	case req := <-done:
		if req.Value != "world" {
			t.Errorf("request Value = %q, want %q", req.Value, "world")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handler dispatch")
	}

	// Verify the response was written
	if respBuf.Len() == 0 {
		t.Fatal("expected response to be written to respWriter")
	}

	respH, err := ReadHeader(&respBuf)
	if err != nil {
		t.Fatalf("ReadHeader on response failed: %v", err)
	}
	if respH.RequestID != 99 {
		t.Errorf("response RequestID = %d, want 99", respH.RequestID)
	}
	if respH.Status.IsRequest() {
		t.Error("expected response IsRequest=false")
	}

	// Read the response payload
	payloadBytes, err := readPayload(&respBuf, respH)
	if err != nil {
		t.Fatalf("readPayload failed: %v", err)
	}
	si := NewStreamInput(bytes.NewReader(payloadBytes))
	result, err := si.ReadString()
	if err != nil {
		t.Fatalf("ReadString failed: %v", err)
	}
	if result != "echo:world" {
		t.Errorf("response Result = %q, want %q", result, "echo:world")
	}
}
