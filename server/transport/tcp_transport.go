package transport

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var zeroTime time.Time

func deadlineFromTimeout(d time.Duration) time.Time {
	return time.Now().Add(d)
}

// NodeConnection holds all TCP connections to a single remote node.
type NodeConnection struct {
	node     DiscoveryNode
	channels map[ConnectionType][]net.Conn
	version  int32 // negotiated transport version
	closed   atomic.Bool
	counters map[ConnectionType]*atomic.Uint64 // round-robin counters
}

// Conn round-robins within the pool for the given type.
// Falls back to ConnTypeREG if the requested type has no connections.
func (nc *NodeConnection) Conn(ct ConnectionType) (net.Conn, error) {
	if nc.closed.Load() {
		return nil, fmt.Errorf("node connection closed")
	}
	conns := nc.channels[ct]
	if len(conns) == 0 {
		conns = nc.channels[ConnTypeREG]
	}
	if len(conns) == 0 {
		return nil, fmt.Errorf("no connections available for type %d", ct)
	}
	counter := nc.counters[ct]
	if counter == nil {
		counter = nc.counters[ConnTypeREG]
	}
	idx := counter.Add(1) - 1
	return conns[idx%uint64(len(conns))], nil
}

// Close closes all connections in the NodeConnection.
func (nc *NodeConnection) Close() error {
	if !nc.closed.CompareAndSwap(false, true) {
		return nil
	}
	var firstErr error
	for _, conns := range nc.channels {
		for _, c := range conns {
			if err := c.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// TcpTransport manages TCP listening and connection establishment.
type TcpTransport struct {
	localNode        DiscoveryNode
	listener         net.Listener
	requestHandlers  *RequestHandlerMap
	responseHandlers *ResponseHandlers
	threadPool       *ThreadPool
	outbound         *OutboundHandler
	inbound          *InboundHandler
	stopCh           chan struct{}
	wg               sync.WaitGroup
}

// NewTcpTransport creates a new TcpTransport.
func NewTcpTransport(localNode DiscoveryNode, requestHandlers *RequestHandlerMap, responseHandlers *ResponseHandlers, threadPool *ThreadPool) *TcpTransport {
	return &TcpTransport{
		localNode:        localNode,
		requestHandlers:  requestHandlers,
		responseHandlers: responseHandlers,
		threadPool:       threadPool,
		outbound:         NewOutboundHandler(),
		inbound:          NewInboundHandler(requestHandlers, responseHandlers, threadPool, localNode.ID),
		stopCh:           make(chan struct{}),
	}
}

// Start starts the TCP listener and spawns the accept loop goroutine.
// Returns the bound address.
func (t *TcpTransport) Start(bindAddress string) (string, error) {
	ln, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return "", fmt.Errorf("listen on %s: %w", bindAddress, err)
	}
	t.listener = ln

	t.wg.Add(1)
	go t.acceptLoop()

	return ln.Addr().String(), nil
}

// Stop closes the stop channel, closes the listener, and waits for all goroutines.
func (t *TcpTransport) Stop() error {
	close(t.stopCh)
	var err error
	if t.listener != nil {
		err = t.listener.Close()
	}
	t.wg.Wait()
	return err
}

func (t *TcpTransport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.stopCh:
				return
			default:
				continue
			}
		}
		t.wg.Add(1)
		go func(c net.Conn) {
			defer t.wg.Done()
			defer c.Close()
			t.handleConn(c)
		}(conn)
	}
}

func (t *TcpTransport) handleConn(conn net.Conn) {
	for {
		select {
		case <-t.stopCh:
			return
		default:
		}
		if err := t.inbound.HandleMessage(conn, conn); err != nil {
			return
		}
	}
}

// OpenConnection dials TCP connections per the profile, performs handshake on each,
// and returns a NodeConnection. On any failure, closes all already-opened connections.
func (t *TcpTransport) OpenConnection(node DiscoveryNode, profile ConnectionProfile) (*NodeConnection, error) {
	channels := make(map[ConnectionType][]net.Conn)
	counters := make(map[ConnectionType]*atomic.Uint64)
	var allConns []net.Conn

	cleanup := func() {
		for _, c := range allConns {
			c.Close()
		}
	}

	var negotiatedVersion int32

	for ct, count := range profile.ConnPerType {
		conns := make([]net.Conn, 0, count)
		for range count {
			conn, err := net.DialTimeout("tcp", node.Address, profile.ConnectTimeout)
			if err != nil {
				cleanup()
				return nil, fmt.Errorf("dial %s: %w", node.Address, err)
			}
			allConns = append(allConns, conn)

			version, err := t.performHandshake(conn, profile)
			if err != nil {
				cleanup()
				return nil, fmt.Errorf("handshake with %s: %w", node.Address, err)
			}
			negotiatedVersion = version

			conns = append(conns, conn)
		}
		channels[ct] = conns
		counters[ct] = &atomic.Uint64{}
	}

	nc := &NodeConnection{
		node:     node,
		channels: channels,
		version:  negotiatedVersion,
		counters: counters,
	}
	return nc, nil
}

func (t *TcpTransport) performHandshake(conn net.Conn, profile ConnectionProfile) (int32, error) {
	if profile.HandshakeTimeout > 0 {
		conn.SetDeadline(deadlineFromTimeout(profile.HandshakeTimeout))
		defer conn.SetDeadline(zeroTime)
	}

	req := &HandshakeRequest{Version: CurrentTransportVersion}
	if err := t.outbound.SendHandshakeRequest(conn, 0, req); err != nil {
		return 0, fmt.Errorf("send handshake request: %w", err)
	}

	header, err := ReadHeader(conn)
	if err != nil {
		return 0, fmt.Errorf("read handshake response header: %w", err)
	}
	if !header.Status.IsHandshake() {
		return 0, fmt.Errorf("expected handshake response, got status %d", header.Status)
	}

	payloadBytes, err := readPayload(conn, header)
	if err != nil {
		return 0, fmt.Errorf("read handshake response payload: %w", err)
	}

	in := NewStreamInput(&bufferReader{buf: payloadBytes})
	resp, err := ReadHandshakeResponse(in)
	if err != nil {
		return 0, fmt.Errorf("decode handshake response: %w", err)
	}

	return NegotiateVersion(CurrentTransportVersion, resp.Version), nil
}
