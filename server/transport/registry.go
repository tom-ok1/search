package transport

import (
	"context"
	"sync"
	"sync/atomic"
)

// ResponseHandler handles the result of an outbound request.
type ResponseHandler interface {
	ReadAndHandle(*StreamInput) error
	HandleError(*RemoteTransportError)
}

// ResponseContext holds the state for an in-flight outbound request.
type ResponseContext struct {
	Handler ResponseHandler
	Action  string
	NodeID  string
	Ctx     context.Context
	Cancel  context.CancelFunc
}

// ResponseHandlers tracks in-flight requests by requestID.
type ResponseHandlers struct {
	nextID   atomic.Int64
	handlers sync.Map // int64 → *ResponseContext
}

func NewResponseHandlers() *ResponseHandlers {
	return &ResponseHandlers{}
}

func (rh *ResponseHandlers) Add(ctx *ResponseContext) int64 {
	id := rh.nextID.Add(1)
	rh.handlers.Store(id, ctx)
	return id
}

func (rh *ResponseHandlers) Remove(id int64) *ResponseContext {
	v, ok := rh.handlers.LoadAndDelete(id)
	if !ok {
		return nil
	}
	return v.(*ResponseContext)
}

// requestHandlerEntry stores a registered request handler with a type-erased dispatch closure.
type requestHandlerEntry struct {
	action   string
	executor PoolName
	dispatch func(payload *StreamInput, channel TransportChannel)
}

// RequestHandlerMap maps action names to handlers.
type RequestHandlerMap struct {
	mu       sync.RWMutex
	handlers map[string]*requestHandlerEntry
}

func NewRequestHandlerMap() *RequestHandlerMap {
	return &RequestHandlerMap{
		handlers: make(map[string]*requestHandlerEntry),
	}
}

func (m *RequestHandlerMap) Register(entry *requestHandlerEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[entry.action] = entry
}

func (m *RequestHandlerMap) Get(action string) *requestHandlerEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.handlers[action]
}
