package transport

import (
	"testing"
	"time"
)

type svcTestRequest struct{ Value string }

func (r *svcTestRequest) WriteTo(out *StreamOutput) error { return out.WriteString(r.Value) }

func readSvcTestRequest(in *StreamInput) (*svcTestRequest, error) {
	v, err := in.ReadString()
	return &svcTestRequest{Value: v}, err
}

type svcTestResponse struct{ Result string }

func (r *svcTestResponse) WriteTo(out *StreamOutput) error { return out.WriteString(r.Result) }

func readSvcTestResponse(in *StreamInput) (*svcTestResponse, error) {
	v, err := in.ReadString()
	return &svcTestResponse{Result: v}, err
}

func newTestTransportService(t *testing.T) *TransportService {
	t.Helper()
	ts, err := NewTransportService(TransportServiceConfig{
		BindAddress: "127.0.0.1:0",
		NodeName:    t.Name(),
		PoolConfigs: map[PoolName]PoolConfig{
			PoolGeneric: {Workers: 4, QueueSize: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ts.Stop() })
	return ts
}

func TestTransportService_LocalRequest(t *testing.T) {
	ts := newTestTransportService(t)

	// Register a handler that echoes back "echo:<value>"
	registerHandler(ts.requestHandlers, "test:echo", PoolGeneric,
		readSvcTestRequest,
		func(req *svcTestRequest, ch TransportChannel) error {
			return ch.SendResponse(&svcTestResponse{Result: "echo:" + req.Value})
		},
	)

	done := make(chan struct{})
	var gotResult string

	handler := TypedResponseHandler(
		readSvcTestResponse,
		PoolGeneric,
		func(resp *svcTestResponse) {
			gotResult = resp.Result
			close(done)
		},
		func(err *RemoteTransportError) {
			t.Errorf("unexpected error: %v", err)
			close(done)
		},
	)

	err := ts.SendRequest(ts.LocalNode(), "test:echo", &svcTestRequest{Value: "hello"}, TransportRequestOptions{}, handler)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for response")
	}

	if gotResult != "echo:hello" {
		t.Errorf("got %q, want %q", gotResult, "echo:hello")
	}
}

func TestTransportService_RemoteRequest(t *testing.T) {
	// Start server
	server := newTestTransportService(t)

	// Register handler on server
	registerHandler(server.requestHandlers, "test:echo", PoolGeneric,
		readSvcTestRequest,
		func(req *svcTestRequest, ch TransportChannel) error {
			return ch.SendResponse(&svcTestResponse{Result: "echo:" + req.Value})
		},
	)

	// Start client
	client := newTestTransportService(t)

	// Connect client to server
	serverNode := server.LocalNode()
	if err := client.ConnectToNode(serverNode); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	var gotResult string

	handler := TypedResponseHandler(
		readSvcTestResponse,
		PoolGeneric,
		func(resp *svcTestResponse) {
			gotResult = resp.Result
			close(done)
		},
		func(err *RemoteTransportError) {
			t.Errorf("unexpected error: %v", err)
			close(done)
		},
	)

	err := client.SendRequest(serverNode, "test:echo", &svcTestRequest{Value: "world"}, TransportRequestOptions{}, handler)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for response")
	}

	if gotResult != "echo:world" {
		t.Errorf("got %q, want %q", gotResult, "echo:world")
	}
}
