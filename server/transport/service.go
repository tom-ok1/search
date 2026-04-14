package transport

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// TransportServiceConfig configures a TransportService.
type TransportServiceConfig struct {
	BindAddress string
	NodeName    string
	PoolConfigs map[PoolName]PoolConfig
	ConnProfile ConnectionProfile // optional, uses default if zero
}

// TransportService wraps connection management, handler registry, and dispatch.
type TransportService struct {
	localNode        DiscoveryNode
	transport        *TcpTransport
	connectionMgr    *ConnectionManager
	requestHandlers  *RequestHandlerMap
	responseHandlers *ResponseHandlers
	workerPool       *WorkerPool
	outbound         *OutboundHandler
}

// NewTransportService creates and starts a new TransportService.
func NewTransportService(config TransportServiceConfig) (*TransportService, error) {
	nodeID := uuid.New().String()
	localNode := DiscoveryNode{ID: nodeID, Name: config.NodeName}

	requestHandlers := NewRequestHandlerMap()
	responseHandlers := NewResponseHandlers()
	workerPool := NewWorkerPool(config.PoolConfigs)

	transport := NewTcpTransport(localNode, requestHandlers, responseHandlers, workerPool)

	addr, err := transport.Start(config.BindAddress)
	if err != nil {
		workerPool.Shutdown()
		return nil, err
	}
	localNode.Address = addr

	profile := config.ConnProfile
	if len(profile.ConnPerType) == 0 {
		profile = ConnectionProfile{
			ConnPerType:      map[ConnectionType]int{ConnTypeREG: 1},
			ConnectTimeout:   5 * time.Second,
			HandshakeTimeout: 5 * time.Second,
		}
	}
	connectionMgr := NewConnectionManager(transport, profile)

	return &TransportService{
		localNode:        localNode,
		transport:        transport,
		connectionMgr:    connectionMgr,
		requestHandlers:  requestHandlers,
		responseHandlers: responseHandlers,
		workerPool:       workerPool,
		outbound:         NewOutboundHandler(),
	}, nil
}

// LocalNode returns the local node info.
func (ts *TransportService) LocalNode() DiscoveryNode {
	return ts.localNode
}

// SendRequest sends a request to the given node. If the node is the local node,
// the request is dispatched locally. Otherwise it is sent over the network.
func (ts *TransportService) SendRequest(node DiscoveryNode, action string, request Writeable, options TransportRequestOptions, handler *responseHandlerWrapper) error {
	if node.ID == ts.localNode.ID {
		return ts.sendLocalRequest(action, request, handler)
	}
	return ts.sendRemoteRequest(node, action, request, options, handler)
}

func (ts *TransportService) sendLocalRequest(action string, request Writeable, handler *responseHandlerWrapper) error {
	entry := ts.requestHandlers.Get(action)
	if entry == nil {
		return fmt.Errorf("no handler registered for action %q", action)
	}

	// Serialize request to bytes
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := request.WriteTo(out); err != nil {
		return fmt.Errorf("serialize local request: %w", err)
	}

	in := NewStreamInput(bytes.NewReader(buf.Bytes()))
	channel := &localTransportChannel{
		handler:    handler,
		workerPool: ts.workerPool,
	}

	return ts.workerPool.Submit(entry.executor, func() {
		entry.dispatch(in, channel)
	})
}

func (ts *TransportService) sendRemoteRequest(node DiscoveryNode, action string, request Writeable, options TransportRequestOptions, handler *responseHandlerWrapper) error {
	conn, err := ts.connectionMgr.GetConnection(node.ID)
	if err != nil {
		return err
	}

	reqCtx, cancel := context.WithTimeout(context.Background(), options.Timeout)
	ctx := &ResponseContext{
		Handler: handler,
		Action:  action,
		NodeID:  node.ID,
		Ctx:     reqCtx,
		Cancel:  cancel,
	}
	requestID := ts.responseHandlers.Add(ctx)

	w, err := conn.ConnWriter(options.ConnType)
	if err != nil {
		ts.responseHandlers.Remove(requestID)
		return err
	}

	if err := ts.outbound.SendRequest(w, requestID, action, request); err != nil {
		ts.responseHandlers.Remove(requestID)
		return &SendRequestError{Action: action, Cause: err}
	}

	return nil
}

// ConnectToNode establishes a connection to the given node.
func (ts *TransportService) ConnectToNode(node DiscoveryNode) error {
	return ts.connectionMgr.Connect(node)
}

// DisconnectFromNode closes the connection to the given node.
func (ts *TransportService) DisconnectFromNode(nodeID string) {
	ts.connectionMgr.DisconnectFromNode(nodeID)
}

// Stop shuts down the transport service.
func (ts *TransportService) Stop() error {
	if err := ts.connectionMgr.Close(); err != nil {
		return err
	}
	if err := ts.transport.Stop(); err != nil {
		return err
	}
	ts.workerPool.Shutdown()
	return nil
}

// responseHandlerWrapper is a type-erased response callback that implements
// the interfaces expected by InboundHandler.handleResponse.
type responseHandlerWrapper struct {
	executorName  PoolName
	readAndHandle func(in *StreamInput) error
	onError       func(*RemoteTransportError)
}

// ReadAndHandle deserializes and handles a successful response.
func (w *responseHandlerWrapper) ReadAndHandle(in *StreamInput) error {
	return w.readAndHandle(in)
}

// HandleError handles an error response.
func (w *responseHandlerWrapper) HandleError(err *RemoteTransportError) {
	if w.onError != nil {
		w.onError(err)
	}
}

// TypedResponseHandler creates a type-erased response handler from typed callbacks.
func TypedResponseHandler[T any](reader Reader[T], executor PoolName, onResponse func(T), onError func(*RemoteTransportError)) *responseHandlerWrapper {
	return &responseHandlerWrapper{
		executorName: executor,
		readAndHandle: func(in *StreamInput) error {
			resp, err := reader(in)
			if err != nil {
				return err
			}
			onResponse(resp)
			return nil
		},
		onError: onError,
	}
}

// localTransportChannel delivers responses locally without going through TCP.
type localTransportChannel struct {
	handler    *responseHandlerWrapper
	workerPool *WorkerPool
	mu         sync.Mutex
	responded  bool
}

// SendResponse serializes the response and delivers it to the handler.
func (c *localTransportChannel) SendResponse(response Writeable) error {
	c.mu.Lock()
	if c.responded {
		c.mu.Unlock()
		return nil
	}
	c.responded = true
	c.mu.Unlock()

	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := response.WriteTo(out); err != nil {
		return err
	}

	in := NewStreamInput(bytes.NewReader(buf.Bytes()))

	return c.workerPool.Submit(c.handler.executorName, func() {
		c.handler.readAndHandle(in)
	})
}

// SendError wraps the error as a RemoteTransportError and delivers it to the handler.
func (c *localTransportChannel) SendError(err error) error {
	c.mu.Lock()
	if c.responded {
		c.mu.Unlock()
		return nil
	}
	c.responded = true
	c.mu.Unlock()

	rte := &RemoteTransportError{Message: err.Error()}
	c.handler.HandleError(rte)
	return nil
}
