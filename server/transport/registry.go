package transport

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ResponseContext holds the state for an in-flight outbound request.
type ResponseContext struct {
	Handler   any
	Action    string
	NodeID    string
	Timeout   time.Duration
	CreatedAt time.Time
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

// Range iterates over all in-flight handlers. Used by timeout reaper.
func (rh *ResponseHandlers) Range(fn func(id int64, ctx *ResponseContext) bool) {
	rh.handlers.Range(func(key, value any) bool {
		return fn(key.(int64), value.(*ResponseContext))
	})
}

// requestHandlerEntry stores a registered request handler with a type-erased dispatch closure.
type requestHandlerEntry struct {
	action   string
	executor string
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

// RegisterHandler is a typed helper that creates a dispatch closure capturing concrete types.
func RegisterHandler[T any](
	m *RequestHandlerMap,
	action string,
	executor string,
	reader Reader[T],
	handler func(request T, channel TransportChannel) error,
) {
	m.Register(&requestHandlerEntry{
		action:   action,
		executor: executor,
		dispatch: func(payload *StreamInput, channel TransportChannel) {
			req, err := reader(payload)
			if err != nil {
				channel.SendError(fmt.Errorf("deserialize request: %w", err))
				return
			}
			if err := handler(req, channel); err != nil {
				channel.SendError(err)
			}
		},
	})
}
