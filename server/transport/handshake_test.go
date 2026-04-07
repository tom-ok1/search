package transport

import (
	"bytes"
	"testing"
)

func TestHandshakeRequest_Roundtrip(t *testing.T) {
	// Write version=1
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)

	req := &HandshakeRequest{Version: 1}
	if err := req.WriteTo(out); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	// Read back
	in := NewStreamInput(&buf)
	gotReq, err := ReadHandshakeRequest(in)
	if err != nil {
		t.Fatalf("ReadHandshakeRequest failed: %v", err)
	}

	// Verify
	if gotReq.Version != 1 {
		t.Errorf("expected version 1, got %d", gotReq.Version)
	}
}

func TestHandshakeResponse_Roundtrip(t *testing.T) {
	// Write version=2 + nodeID="node-abc"
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)

	resp := &HandshakeResponse{
		Version: 2,
		NodeID:  "node-abc",
	}
	if err := resp.WriteTo(out); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	// Read back
	in := NewStreamInput(&buf)
	gotResp, err := ReadHandshakeResponse(in)
	if err != nil {
		t.Fatalf("ReadHandshakeResponse failed: %v", err)
	}

	// Verify both fields
	if gotResp.Version != 2 {
		t.Errorf("expected version 2, got %d", gotResp.Version)
	}
	if gotResp.NodeID != "node-abc" {
		t.Errorf("expected nodeID 'node-abc', got '%s'", gotResp.NodeID)
	}
}

func TestNegotiateVersion(t *testing.T) {
	tests := []struct {
		local    int32
		remote   int32
		expected int32
	}{
		{1, 1, 1},
		{2, 1, 1},
		{1, 3, 1},
	}

	for _, tt := range tests {
		got := NegotiateVersion(tt.local, tt.remote)
		if got != tt.expected {
			t.Errorf("NegotiateVersion(%d, %d) = %d, want %d",
				tt.local, tt.remote, got, tt.expected)
		}
	}
}
