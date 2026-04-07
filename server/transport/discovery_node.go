package transport

// DiscoveryNode represents a node in the cluster.
// Minimal definition for the transport layer; will be extended in Phase 2 (discovery).
type DiscoveryNode struct {
	ID      string
	Name    string
	Address string // host:port for transport
}
