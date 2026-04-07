package transport

import (
	"testing"
	"time"
)

func TestTcpTransport_ListenAndConnect(t *testing.T) {
	node := DiscoveryNode{ID: "node-1", Name: "test-node", Address: "127.0.0.1:0"}
	handlers := NewRequestHandlerMap()
	respHandlers := NewResponseHandlers()
	tp := NewThreadPool(map[string]PoolConfig{"generic": {Workers: 0}})
	defer tp.Shutdown()

	transport := NewTcpTransport(node, handlers, respHandlers, tp)

	addr, err := transport.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if addr == "" {
		t.Fatal("expected non-empty address")
	}
	t.Logf("listening on %s", addr)

	if err := transport.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestTcpTransport_Handshake(t *testing.T) {
	// Server transport
	serverNode := DiscoveryNode{ID: "server-1", Name: "server", Address: "127.0.0.1:0"}
	handlers := NewRequestHandlerMap()
	respHandlers := NewResponseHandlers()
	tp := NewThreadPool(map[string]PoolConfig{"generic": {Workers: 0}})
	defer tp.Shutdown()

	server := NewTcpTransport(serverNode, handlers, respHandlers, tp)

	addr, err := server.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("server Start failed: %v", err)
	}
	defer server.Stop()

	// Client transport
	clientNode := DiscoveryNode{ID: "client-1", Name: "client", Address: "127.0.0.1:0"}
	client := NewTcpTransport(clientNode, handlers, respHandlers, tp)

	profile := ConnectionProfile{
		ConnPerType:      map[ConnectionType]int{ConnTypeREG: 1},
		ConnectTimeout:   5 * time.Second,
		HandshakeTimeout: 5 * time.Second,
	}

	remoteNode := DiscoveryNode{ID: "server-1", Name: "server", Address: addr}
	nc, err := client.OpenConnection(remoteNode, profile)
	if err != nil {
		t.Fatalf("OpenConnection failed: %v", err)
	}
	defer nc.Close()

	if nc.version != CurrentTransportVersion {
		t.Fatalf("expected negotiated version %d, got %d", CurrentTransportVersion, nc.version)
	}

	// Verify we can get a connection
	conn, err := nc.Conn(ConnTypeREG)
	if err != nil {
		t.Fatalf("Conn(REG) failed: %v", err)
	}
	if conn == nil {
		t.Fatal("expected non-nil connection")
	}
}
