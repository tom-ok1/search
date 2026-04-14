package transport

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

// maxPoolableBufferSize is the cap above which buffers are not returned to the
// pool, preventing a single large request/response from permanently inflating
// pooled memory.
const maxPoolableBufferSize = 64 * 1024 // 64 KiB

var bufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

// getBuffer retrieves a reset buffer from the pool.
func getBuffer() *bytes.Buffer {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// putBuffer returns buf to the pool if its capacity is within the limit.
func putBuffer(buf *bytes.Buffer) {
	if buf.Cap() > maxPoolableBufferSize {
		return // let GC reclaim oversized buffers
	}
	bufferPool.Put(buf)
}

const handshakeAction = "internal:transport/handshake"

// OutboundHandler serializes and sends transport messages.
type OutboundHandler struct{}

func NewOutboundHandler() *OutboundHandler {
	return &OutboundHandler{}
}

// SendRequest writes a complete request message (header + payload) to w.
func (oh *OutboundHandler) SendRequest(w io.Writer, requestID int64, action string, request Writeable) error {
	payload := getBuffer()
	defer putBuffer(payload)

	out := NewStreamOutput(payload)
	if err := request.WriteTo(out); err != nil {
		return fmt.Errorf("serialize request payload: %w", err)
	}

	h := &Header{
		RequestID: requestID,
		Status:    StatusFlags(0).WithRequest(true),
		Action:    action,
	}
	return writeMessageWithPayload(w, h, payload.Bytes())
}

// SendHandshakeRequest writes a handshake request message to w.
func (oh *OutboundHandler) SendHandshakeRequest(w io.Writer, requestID int64, req *HandshakeRequest) error {
	payload := getBuffer()
	defer putBuffer(payload)

	out := NewStreamOutput(payload)
	if err := req.WriteTo(out); err != nil {
		return fmt.Errorf("serialize handshake request: %w", err)
	}

	h := &Header{
		RequestID: requestID,
		Status:    StatusFlags(0).WithRequest(true).WithHandshake(true),
		Action:    handshakeAction,
	}
	return writeMessageWithPayload(w, h, payload.Bytes())
}

// SendHandshakeResponse writes a handshake response message to w.
func (oh *OutboundHandler) SendHandshakeResponse(w io.Writer, requestID int64, resp *HandshakeResponse) error {
	payload := getBuffer()
	defer putBuffer(payload)

	out := NewStreamOutput(payload)
	if err := resp.WriteTo(out); err != nil {
		return fmt.Errorf("serialize handshake response: %w", err)
	}

	h := &Header{
		RequestID: requestID,
		Status:    StatusFlags(0).WithHandshake(true),
	}
	return writeMessageWithPayload(w, h, payload.Bytes())
}

// InboundHandler receives and dispatches incoming messages.
type InboundHandler struct {
	requestHandlers  *RequestHandlerMap
	responseHandlers *ResponseHandlers
	workerPool       *WorkerPool
	localNodeID      string
}

func NewInboundHandler(requestHandlers *RequestHandlerMap, responseHandlers *ResponseHandlers, workerPool *WorkerPool, localNodeID string) *InboundHandler {
	return &InboundHandler{
		requestHandlers:  requestHandlers,
		responseHandlers: responseHandlers,
		workerPool:       workerPool,
		localNodeID:      localNodeID,
	}
}

// HandleMessage reads one complete message from r and dispatches it.
func (ih *InboundHandler) HandleMessage(r io.Reader, respWriter io.Writer) error {
	h, err := ReadHeader(r)
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	payloadBytes, err := readPayload(r, h)
	if err != nil {
		return fmt.Errorf("read payload: %w", err)
	}

	si := NewStreamInput(bytes.NewReader(payloadBytes))

	if h.Status.IsHandshake() && h.Status.IsRequest() {
		return ih.handleHandshake(si, respWriter, h.RequestID)
	}

	if h.Status.IsRequest() {
		return ih.handleRequest(si, respWriter, h)
	}

	return ih.handleResponse(si, h)
}

func (ih *InboundHandler) handleHandshake(si *StreamInput, respWriter io.Writer, requestID int64) error {
	req, err := ReadHandshakeRequest(si)
	if err != nil {
		return fmt.Errorf("read handshake request: %w", err)
	}

	negotiated := NegotiateVersion(CurrentTransportVersion, req.Version)
	resp := &HandshakeResponse{
		Version: negotiated,
		NodeID:  ih.localNodeID,
	}

	oh := NewOutboundHandler()
	return oh.SendHandshakeResponse(respWriter, requestID, resp)
}

func (ih *InboundHandler) handleRequest(si *StreamInput, respWriter io.Writer, h *Header) error {
	entry := ih.requestHandlers.Get(h.Action)
	if entry == nil {
		return fmt.Errorf("no handler registered for action %q", h.Action)
	}

	channel := NewTcpTransportChannel(h.RequestID, respWriter)

	return ih.workerPool.Submit(entry.executor, func() {
		entry.dispatch(si, channel)
	})
}

func (ih *InboundHandler) handleResponse(si *StreamInput, h *Header) error {
	ctx := ih.responseHandlers.Remove(h.RequestID)
	if ctx == nil {
		return fmt.Errorf("no response handler for requestID %d", h.RequestID)
	}

	if h.Status.IsError() {
		rte, err := ReadRemoteTransportError(si)
		if err != nil {
			return fmt.Errorf("read remote transport error: %w", err)
		}
		ctx.Handler.HandleError(rte)
		return nil
	}

	return ctx.Handler.ReadAndHandle(si)
}

// readPayload reads the payload bytes from r based on the header's payload size.
func readPayload(r io.Reader, h *Header) ([]byte, error) {
	size := h.PayloadSize()
	if size <= 0 {
		return nil, nil
	}
	buf := make([]byte, size)
	_, err := io.ReadFull(r, buf)
	return buf, err
}
