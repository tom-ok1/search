package transport

import (
	"errors"
	"testing"
	"time"
)

func newTestServer(t *testing.T) *TcpTransport {
	t.Helper()
	tp := NewThreadPool(map[string]PoolConfig{"generic": {Workers: 2, QueueSize: 10}})
	t.Cleanup(tp.Shutdown)
	rh := NewRequestHandlerMap()
	resph := NewResponseHandlers()
	node := DiscoveryNode{ID: "server", Name: "server"}
	transport := NewTcpTransport(node, rh, resph, tp)
	addr, err := transport.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { transport.Stop() })
	// Update node address
	transport.localNode.Address = addr
	return transport
}

func testProfile() ConnectionProfile {
	return ConnectionProfile{
		ConnPerType:      map[ConnectionType]int{ConnTypeREG: 1},
		ConnectTimeout:   5 * time.Second,
		HandshakeTimeout: 5 * time.Second,
	}
}

func TestConnectionManager_ConnectAndGet(t *testing.T) {
	server := newTestServer(t)

	// Create client transport and connection manager
	clientTP := NewThreadPool(map[string]PoolConfig{"generic": {Workers: 2, QueueSize: 10}})
	t.Cleanup(clientTP.Shutdown)
	clientNode := DiscoveryNode{ID: "client", Name: "client"}
	clientTransport := NewTcpTransport(clientNode, NewRequestHandlerMap(), NewResponseHandlers(), clientTP)

	cm := NewConnectionManager(clientTransport, testProfile())
	t.Cleanup(func() { cm.Close() })

	// Connect to server
	err := cm.Connect(server.localNode)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// GetConnection should succeed
	conn, err := cm.GetConnection(server.localNode.ID)
	if err != nil {
		t.Fatalf("GetConnection failed: %v", err)
	}
	if conn == nil {
		t.Fatal("GetConnection returned nil connection")
	}
	if conn.node.ID != server.localNode.ID {
		t.Errorf("connection node ID = %q, want %q", conn.node.ID, server.localNode.ID)
	}
}

func TestConnectionManager_GetConnection_NotConnected(t *testing.T) {
	clientTP := NewThreadPool(map[string]PoolConfig{"generic": {Workers: 2, QueueSize: 10}})
	t.Cleanup(clientTP.Shutdown)
	clientNode := DiscoveryNode{ID: "client", Name: "client"}
	clientTransport := NewTcpTransport(clientNode, NewRequestHandlerMap(), NewResponseHandlers(), clientTP)

	cm := NewConnectionManager(clientTransport, testProfile())
	t.Cleanup(func() { cm.Close() })

	// GetConnection for unknown node should return error
	_, err := cm.GetConnection("unknown-node")
	if err == nil {
		t.Fatal("expected error for unknown node, got nil")
	}

	var notConnectedErr *NodeNotConnectedError
	if !errors.As(err, &notConnectedErr) {
		t.Errorf("error type = %T, want *NodeNotConnectedError", err)
	}
	if notConnectedErr.NodeID != "unknown-node" {
		t.Errorf("error NodeID = %q, want %q", notConnectedErr.NodeID, "unknown-node")
	}
}

func TestConnectionManager_Disconnect(t *testing.T) {
	server := newTestServer(t)

	// Create client transport and connection manager
	clientTP := NewThreadPool(map[string]PoolConfig{"generic": {Workers: 2, QueueSize: 10}})
	t.Cleanup(clientTP.Shutdown)
	clientNode := DiscoveryNode{ID: "client", Name: "client"}
	clientTransport := NewTcpTransport(clientNode, NewRequestHandlerMap(), NewResponseHandlers(), clientTP)

	cm := NewConnectionManager(clientTransport, testProfile())
	t.Cleanup(func() { cm.Close() })

	// Connect to server
	err := cm.Connect(server.localNode)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// GetConnection should succeed
	_, err = cm.GetConnection(server.localNode.ID)
	if err != nil {
		t.Fatalf("GetConnection before disconnect failed: %v", err)
	}

	// Disconnect
	cm.DisconnectFromNode(server.localNode.ID)

	// GetConnection should now fail
	_, err = cm.GetConnection(server.localNode.ID)
	if err == nil {
		t.Fatal("expected error after disconnect, got nil")
	}

	var notConnectedErr *NodeNotConnectedError
	if !errors.As(err, &notConnectedErr) {
		t.Errorf("error type after disconnect = %T, want *NodeNotConnectedError", err)
	}
}

func TestConnectionManager_ConnectedNodes(t *testing.T) {
	server := newTestServer(t)

	// Create client transport and connection manager
	clientTP := NewThreadPool(map[string]PoolConfig{"generic": {Workers: 2, QueueSize: 10}})
	t.Cleanup(clientTP.Shutdown)
	clientNode := DiscoveryNode{ID: "client", Name: "client"}
	clientTransport := NewTcpTransport(clientNode, NewRequestHandlerMap(), NewResponseHandlers(), clientTP)

	cm := NewConnectionManager(clientTransport, testProfile())
	t.Cleanup(func() { cm.Close() })

	// Initially empty
	nodes := cm.ConnectedNodes()
	if len(nodes) != 0 {
		t.Errorf("initial ConnectedNodes length = %d, want 0", len(nodes))
	}

	// Connect to server
	err := cm.Connect(server.localNode)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Should have 1 entry
	nodes = cm.ConnectedNodes()
	if len(nodes) != 1 {
		t.Errorf("ConnectedNodes length after connect = %d, want 1", len(nodes))
	}
	if len(nodes) > 0 && nodes[0].ID != server.localNode.ID {
		t.Errorf("ConnectedNodes[0].ID = %q, want %q", nodes[0].ID, server.localNode.ID)
	}
}
