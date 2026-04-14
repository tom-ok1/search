package transport

import "sync"

// ConnectionManager manages connections to remote nodes.
type ConnectionManager struct {
	transport   *TcpTransport
	profile     ConnectionProfile
	connections map[string]*NodeConnection // nodeID → connection
	mu          sync.RWMutex
}

func NewConnectionManager(transport *TcpTransport, profile ConnectionProfile) *ConnectionManager {
	return &ConnectionManager{
		transport:   transport,
		profile:     profile,
		connections: make(map[string]*NodeConnection),
	}
}

func (cm *ConnectionManager) Connect(node DiscoveryNode) error {
	conn, err := cm.transport.OpenConnection(node, cm.profile)
	if err != nil {
		return err
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if old, ok := cm.connections[node.ID]; ok {
		old.Close()
	}
	cm.connections[node.ID] = conn
	return nil
}

func (cm *ConnectionManager) GetConnection(nodeID string) (*NodeConnection, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	conn, ok := cm.connections[nodeID]
	if !ok {
		return nil, &NodeNotConnectedError{NodeID: nodeID}
	}
	return conn, nil
}

func (cm *ConnectionManager) DisconnectFromNode(nodeID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if conn, ok := cm.connections[nodeID]; ok {
		conn.Close()
		delete(cm.connections, nodeID)
	}
}

func (cm *ConnectionManager) ConnectedNodes() []DiscoveryNode {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	nodes := make([]DiscoveryNode, 0, len(cm.connections))
	for _, conn := range cm.connections {
		nodes = append(nodes, conn.node)
	}
	return nodes
}

func (cm *ConnectionManager) Close() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for id, conn := range cm.connections {
		conn.Close()
		delete(cm.connections, id)
	}
	return nil
}
