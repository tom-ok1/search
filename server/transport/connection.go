package transport

import "time"

// ConnectionType categorizes TCP connections by their purpose.
type ConnectionType int

const (
	ConnTypeREG      ConnectionType = iota // general requests
	ConnTypeBULK                           // bulk indexing
	ConnTypeSTATE                          // cluster state publication
	ConnTypeRECOVERY                       // shard recovery
	ConnTypePING                           // keepalive
)

// TransportRequestOptions configures per-request transport behavior.
type TransportRequestOptions struct {
	ConnType ConnectionType
	Timeout  time.Duration
}

// ConnectionProfile configures connection counts per type and timeouts.
type ConnectionProfile struct {
	ConnPerType      map[ConnectionType]int
	ConnectTimeout   time.Duration
	HandshakeTimeout time.Duration
	PingInterval     time.Duration
}

// DefaultConnectionProfile returns the default connection profile following ES defaults.
func DefaultConnectionProfile() ConnectionProfile {
	return ConnectionProfile{
		ConnPerType: map[ConnectionType]int{
			ConnTypeREG:      6,
			ConnTypeBULK:     3,
			ConnTypeSTATE:    1,
			ConnTypeRECOVERY: 2,
			ConnTypePING:     1,
		},
		ConnectTimeout:   30 * time.Second,
		HandshakeTimeout: 10 * time.Second,
		PingInterval:     25 * time.Second,
	}
}
