package transport

import (
	"bytes"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestIntegration_IndexDocumentOverTransport(t *testing.T) {
	// Start server TransportService
	server, err := NewTransportService(TransportServiceConfig{
		BindAddress: "127.0.0.1:0",
		NodeName:    "server",
		PoolConfigs: map[string]PoolConfig{
			"generic": {Workers: 4, QueueSize: 100},
			"index":   {Workers: 2, QueueSize: 50},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	// Register IndexDocument handler on server
	RegisterTypedHandler(server, "indices:data/write/index", "index",
		Reader[*IndexDocumentRequest](ReadIndexDocumentRequest),
		func(req *IndexDocumentRequest, ch TransportChannel) error {
			resp := &IndexDocumentResponse{
				Index:       req.Index,
				ID:          req.ID,
				SeqNo:       1,
				PrimaryTerm: 1,
				Result:      "created",
			}
			return ch.SendResponse(resp)
		},
	)

	// Start client TransportService
	client, err := NewTransportService(TransportServiceConfig{
		BindAddress: "127.0.0.1:0",
		NodeName:    "client",
		PoolConfigs: map[string]PoolConfig{
			"generic": {Workers: 4, QueueSize: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()

	// Connect client to server
	serverNode := server.LocalNode()
	if err := client.ConnectToNode(serverNode); err != nil {
		t.Fatal(err)
	}

	// Send IndexDocument request
	var mu sync.Mutex
	var result *IndexDocumentResponse
	done := make(chan struct{})

	req := &IndexDocumentRequest{
		Index:  "products",
		ID:     "doc-1",
		Source: json.RawMessage(`{"title":"widget"}`),
	}

	handler := TypedResponseHandler(
		Reader[*IndexDocumentResponse](ReadIndexDocumentResponse),
		"generic",
		func(resp *IndexDocumentResponse) {
			mu.Lock()
			result = resp
			mu.Unlock()
			close(done)
		},
		func(err *RemoteTransportError) {
			t.Errorf("unexpected error: %v", err)
			close(done)
		},
	)

	if err := client.SendRequest(serverNode, "indices:data/write/index", req,
		TransportRequestOptions{ConnType: ConnTypeREG}, handler); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for response")
	}

	mu.Lock()
	defer mu.Unlock()
	if result == nil {
		t.Fatal("no response received")
	}
	if result.Index != "products" {
		t.Errorf("Index: got %q, want %q", result.Index, "products")
	}
	if result.ID != "doc-1" {
		t.Errorf("ID: got %q, want %q", result.ID, "doc-1")
	}
	if result.Result != "created" {
		t.Errorf("Result: got %q, want %q", result.Result, "created")
	}
}

func TestIntegration_SearchOverTransport(t *testing.T) {
	server, err := NewTransportService(TransportServiceConfig{
		BindAddress: "127.0.0.1:0",
		NodeName:    "server",
		PoolConfigs: map[string]PoolConfig{
			"generic": {Workers: 4, QueueSize: 100},
			"search":  {Workers: 2, QueueSize: 50},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	RegisterTypedHandler(server, "indices:data/read/search", "search",
		Reader[*SearchRequestMsg](ReadSearchRequestMsg),
		func(req *SearchRequestMsg, ch TransportChannel) error {
			resp := &SearchResponseMsg{
				Took:          5,
				TotalHits:     1,
				TotalRelation: "eq",
				MaxScore:      1.0,
				Hits: []SearchHitMsg{
					{Index: req.Index, ID: "1", Score: 1.0, Source: json.RawMessage(`{"title":"result"}`)},
				},
			}
			return ch.SendResponse(resp)
		},
	)

	client, err := NewTransportService(TransportServiceConfig{
		BindAddress: "127.0.0.1:0",
		NodeName:    "client",
		PoolConfigs: map[string]PoolConfig{
			"generic": {Workers: 4, QueueSize: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()

	serverNode := server.LocalNode()
	if err := client.ConnectToNode(serverNode); err != nil {
		t.Fatal(err)
	}

	var result *SearchResponseMsg
	done := make(chan struct{})

	req := &SearchRequestMsg{
		Index:     "products",
		QueryJSON: map[string]any{"match_all": map[string]any{}},
		Size:      10,
	}

	handler := TypedResponseHandler(
		Reader[*SearchResponseMsg](ReadSearchResponseMsg),
		"generic",
		func(resp *SearchResponseMsg) {
			result = resp
			close(done)
		},
		nil,
	)

	if err := client.SendRequest(serverNode, "indices:data/read/search", req,
		TransportRequestOptions{ConnType: ConnTypeREG}, handler); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for response")
	}

	if result == nil {
		t.Fatal("no response received")
	}
	if result.TotalHits != 1 {
		t.Errorf("TotalHits: got %d, want 1", result.TotalHits)
	}
	if len(result.Hits) != 1 || result.Hits[0].ID != "1" {
		t.Errorf("Hits: got %+v", result.Hits)
	}
	if !bytes.Equal(result.Hits[0].Source, json.RawMessage(`{"title":"result"}`)) {
		t.Errorf("Source: got %q", result.Hits[0].Source)
	}
}
