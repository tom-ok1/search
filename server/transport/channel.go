package transport

import (
	"io"
	"sync"
)

// TransportChannel is used by request handlers to send responses back.
type TransportChannel interface {
	SendResponse(response Writeable) error
	SendError(err error) error
}

// TcpTransportChannel writes serialized responses over a TCP connection.
type TcpTransportChannel struct {
	requestID int64
	writer    io.Writer
	mu        sync.Mutex
	responded bool
}

func NewTcpTransportChannel(requestID int64, writer io.Writer) *TcpTransportChannel {
	return &TcpTransportChannel{requestID: requestID, writer: writer}
}

func (c *TcpTransportChannel) SendResponse(response Writeable) error {
	c.mu.Lock()
	if c.responded {
		c.mu.Unlock()
		return nil
	}
	c.responded = true
	c.mu.Unlock()

	payload := getBuffer()
	defer putBuffer(payload)

	out := NewStreamOutput(payload)
	if err := response.WriteTo(out); err != nil {
		return err
	}

	h := &Header{
		RequestID: c.requestID,
		Status:    StatusFlags(0),
	}
	return writeMessageWithPayload(c.writer, h, payload.Bytes())
}

func (c *TcpTransportChannel) SendError(err error) error {
	c.mu.Lock()
	if c.responded {
		c.mu.Unlock()
		return nil
	}
	c.responded = true
	c.mu.Unlock()

	errMsg := &RemoteTransportError{Message: err.Error()}
	payload := getBuffer()
	defer putBuffer(payload)

	out := NewStreamOutput(payload)
	if writeErr := errMsg.WriteTo(out); writeErr != nil {
		return writeErr
	}

	h := &Header{
		RequestID: c.requestID,
		Status:    StatusFlags(0).WithError(true),
	}
	return writeMessageWithPayload(c.writer, h, payload.Bytes())
}

// SyncWriter serializes concurrent writes to an underlying io.Writer using a mutex.
// Each Write call is guaranteed not to interleave with any other.
type SyncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

// NewSyncWriter wraps w so that concurrent Write calls are serialized.
func NewSyncWriter(w io.Writer) *SyncWriter {
	return &SyncWriter{w: w}
}

func (sw *SyncWriter) Write(p []byte) (int, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.w.Write(p)
}

// Close closes the underlying writer if it implements io.Closer.
func (sw *SyncWriter) Close() error {
	if c, ok := sw.w.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// writeMessageWithPayload writes a complete message (header + payload) to w.
func writeMessageWithPayload(w io.Writer, h *Header, payload []byte) error {
	// Build variable header
	varBuf := getBuffer()
	defer putBuffer(varBuf)

	varOut := NewStreamOutput(varBuf)
	if h.Status.IsRequest() {
		if err := varOut.WriteString(h.Action); err != nil {
			return err
		}
	}
	if err := varOut.WriteString(h.ParentTaskID); err != nil {
		return err
	}
	varHeader := varBuf.Bytes()

	// MessageLength = requestID(8) + status(1) + varHeaderLen(4) + varHeader + payload
	msgLen := int32(8 + 1 + 4 + len(varHeader) + len(payload))

	// Write complete message to buffer first, then single write to connection
	msg := getBuffer()
	defer putBuffer(msg)

	msg.Write([]byte{'E', 'S'})
	sout := NewStreamOutput(msg)
	sout.WriteInt32(msgLen)
	sout.WriteInt64(h.RequestID)
	sout.WriteByte(byte(h.Status))
	sout.WriteInt32(int32(len(varHeader)))
	msg.Write(varHeader)
	msg.Write(payload)

	_, err := w.Write(msg.Bytes())
	return err
}
