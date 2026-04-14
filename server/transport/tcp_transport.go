package transport

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var zeroTime time.Time

func deadlineFromTimeout(d time.Duration) time.Time {
	return time.Now().Add(d)
}

// writerPool is a round-robin pool of mutex-guarded writers.
type writerPool struct {
	writers []*SyncWriter
	counter atomic.Uint64
}

func (p *writerPool) next() *SyncWriter {
	idx := p.counter.Add(1) - 1
	return p.writers[idx%uint64(len(p.writers))]
}

// NodeConnection holds all TCP connections to a single remote node.
type NodeConnection struct {
	node    DiscoveryNode
	pools   map[ConnectionType]*writerPool
	version int32 // negotiated transport version
	closed  atomic.Bool
}

// ConnWriter round-robins within the pool for the given type and returns
// a mutex-guarded writer that serializes writes to the underlying connection.
// Falls back to ConnTypeREG if the requested type has no connections.
func (nc *NodeConnection) ConnWriter(ct ConnectionType) (io.Writer, error) {
	if nc.closed.Load() {
		return nil, fmt.Errorf("node connection closed")
	}
	pool := nc.pools[ct]
	if pool == nil || len(pool.writers) == 0 {
		ct = ConnTypeREG
		pool = nc.pools[ct]
	}
	if pool == nil || len(pool.writers) == 0 {
		return nil, fmt.Errorf("no connections available for type %d", ct)
	}
	return pool.next(), nil
}

// Close closes all connections in the NodeConnection.
func (nc *NodeConnection) Close() error {
	if !nc.closed.CompareAndSwap(false, true) {
		return nil
	}
	var firstErr error
	for _, p := range nc.pools {
		for _, w := range p.writers {
			if err := w.Close(); err != nil && firstErr == nil {
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
	workerPool       *WorkerPool
	outbound         *OutboundHandler
	inbound          *InboundHandler
	stopCh           chan struct{}
	wg               sync.WaitGroup
}

// NewTcpTransport creates a new TcpTransport.
func NewTcpTransport(localNode DiscoveryNode, requestHandlers *RequestHandlerMap, responseHandlers *ResponseHandlers, workerPool *WorkerPool) *TcpTransport {
	return &TcpTransport{
		localNode:        localNode,
		requestHandlers:  requestHandlers,
		responseHandlers: responseHandlers,
		workerPool:       workerPool,
		outbound:         NewOutboundHandler(),
		inbound:          NewInboundHandler(requestHandlers, responseHandlers, workerPool, localNode.ID),
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
	w := NewSyncWriter(conn)
	for {
		select {
		case <-t.stopCh:
			return
		default:
		}
		if err := t.inbound.HandleMessage(conn, w); err != nil {
			return
		}
	}
}

// OpenConnection dials TCP connections per the profile, performs handshake on each,
// and returns a NodeConnection. On any failure, closes all already-opened connections.
func (t *TcpTransport) OpenConnection(node DiscoveryNode, profile ConnectionProfile) (*NodeConnection, error) {
	pools := make(map[ConnectionType]*writerPool)
	var allConns []net.Conn

	cleanup := func() {
		for _, c := range allConns {
			c.Close()
		}
	}

	for ct, count := range profile.ConnPerType {
		cw := make([]*SyncWriter, 0, count)
		for range count {
			conn, err := net.DialTimeout("tcp", node.Address, profile.ConnectTimeout)
			if err != nil {
				cleanup()
				return nil, fmt.Errorf("dial %s: %w", node.Address, err)
			}
			allConns = append(allConns, conn)
			cw = append(cw, NewSyncWriter(conn))
		}
		pools[ct] = &writerPool{writers: cw}
	}

	if len(allConns) == 0 {
		return nil, fmt.Errorf("no connections configured for %s", node.Address)
	}

	version, err := t.performHandshake(allConns[0], profile)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("handshake with %s: %w", node.Address, err)
	}

	nc := &NodeConnection{
		node:    node,
		pools:   pools,
		version: version,
	}

	// Start read loops on outbound connections to receive responses.
	for _, conn := range allConns {
		t.wg.Add(1)
		go func(c net.Conn) {
			defer t.wg.Done()
			t.handleConn(c)
		}(conn)
	}

	return nc, nil
}

func (t *TcpTransport) performHandshake(conn net.Conn, profile ConnectionProfile) (int32, error) {
	if profile.HandshakeTimeout > 0 {
		conn.SetDeadline(time.Now().Add(profile.HandshakeTimeout))
		defer conn.SetDeadline(time.Time{})
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

	in := NewStreamInput(bytes.NewReader(payloadBytes))
	resp, err := ReadHandshakeResponse(in)
	if err != nil {
		return 0, fmt.Errorf("decode handshake response: %w", err)
	}

	return NegotiateVersion(CurrentTransportVersion, resp.Version), nil
}
