package transport

import (
	"encoding/binary"
	"fmt"
	"io"
)

// StatusFlags is a single byte bitfield for transport message status.
// Bit 0: isRequest (1 = request, 0 = response)
// Bit 1: isError (response carries serialized error)
// Bit 2: isHandshake (handshake message)
type StatusFlags byte

const (
	flagRequest   StatusFlags = 1 << 0 // 0x01
	flagError     StatusFlags = 1 << 1 // 0x02
	flagHandshake StatusFlags = 1 << 2 // 0x04
)

// IsRequest returns true if this is a request message.
func (f StatusFlags) IsRequest() bool {
	return f&flagRequest != 0
}

// IsError returns true if this response carries a serialized error.
func (f StatusFlags) IsError() bool {
	return f&flagError != 0
}

// IsHandshake returns true if this is a handshake message.
func (f StatusFlags) IsHandshake() bool {
	return f&flagHandshake != 0
}

// WithRequest returns a new StatusFlags with the isRequest bit set or cleared.
func (f StatusFlags) WithRequest(v bool) StatusFlags {
	if v {
		return f | flagRequest
	}
	return f &^ flagRequest
}

// WithError returns a new StatusFlags with the isError bit set or cleared.
func (f StatusFlags) WithError(v bool) StatusFlags {
	if v {
		return f | flagError
	}
	return f &^ flagError
}

// WithHandshake returns a new StatusFlags with the isHandshake bit set or cleared.
func (f StatusFlags) WithHandshake(v bool) StatusFlags {
	if v {
		return f | flagHandshake
	}
	return f &^ flagHandshake
}

// Wire format constants.
const (
	// FixedHeaderSize is the size of the fixed portion of the header:
	// marker(2) + messageLength(4) + requestID(8) + status(1) + varHeaderLen(4) = 19 bytes
	FixedHeaderSize = 19

	// Marker bytes that start every message.
	markerByte1 = 'E'
	markerByte2 = 'S'
)

// Header represents a transport protocol message header.
type Header struct {
	MessageLength   int32 // total message length after marker+length fields
	RequestID       int64
	Status          StatusFlags
	Action          string // only for requests
	ParentTaskID    string
	varHeaderLength int32 // stored for payload size calculation
}

// WriteTo serializes the complete header to the given writer.
// Wire format:
//
//	Marker: "ES" (2 bytes)
//	MessageLength (4 bytes, big-endian)
//	RequestID (8 bytes, big-endian)
//	Status (1 byte)
//	VarHeaderLength (4 bytes, big-endian)
//	Variable Header: Action (string, request only) + ParentTaskID (string)
func (h *Header) Encode(w io.Writer) error {
	// Write marker
	marker := []byte{markerByte1, markerByte2}
	if _, err := w.Write(marker); err != nil {
		return fmt.Errorf("writing marker: %w", err)
	}

	// Write MessageLength
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(h.MessageLength))
	if _, err := w.Write(buf[:]); err != nil {
		return fmt.Errorf("writing message length: %w", err)
	}

	// Write RequestID
	var buf8 [8]byte
	binary.BigEndian.PutUint64(buf8[:], uint64(h.RequestID))
	if _, err := w.Write(buf8[:]); err != nil {
		return fmt.Errorf("writing request ID: %w", err)
	}

	// Write Status
	if _, err := w.Write([]byte{byte(h.Status)}); err != nil {
		return fmt.Errorf("writing status: %w", err)
	}

	// Build variable header in memory to compute its length
	var varHeaderBuf []byte
	stream := NewStreamOutput(&bufferWriter{buf: &varHeaderBuf})

	// Action is only written for requests
	if h.Status.IsRequest() {
		if err := stream.WriteString(h.Action); err != nil {
			return fmt.Errorf("writing action: %w", err)
		}
	}

	// ParentTaskID always written
	if err := stream.WriteString(h.ParentTaskID); err != nil {
		return fmt.Errorf("writing parent task ID: %w", err)
	}

	// Write VarHeaderLength
	binary.BigEndian.PutUint32(buf[:], uint32(len(varHeaderBuf)))
	if _, err := w.Write(buf[:]); err != nil {
		return fmt.Errorf("writing var header length: %w", err)
	}

	// Write variable header
	if _, err := w.Write(varHeaderBuf); err != nil {
		return fmt.Errorf("writing variable header: %w", err)
	}

	return nil
}

// ReadHeader reads and parses a header from the given reader.
func ReadHeader(r io.Reader) (*Header, error) {
	// Read and verify marker
	marker := make([]byte, 2)
	if _, err := io.ReadFull(r, marker); err != nil {
		return nil, fmt.Errorf("reading marker: %w", err)
	}
	if marker[0] != markerByte1 || marker[1] != markerByte2 {
		return nil, fmt.Errorf("invalid marker: expected [%c%c], got [%c%c]",
			markerByte1, markerByte2, marker[0], marker[1])
	}

	// Read MessageLength
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, fmt.Errorf("reading message length: %w", err)
	}
	messageLength := int32(binary.BigEndian.Uint32(buf[:]))

	// Read RequestID
	var buf8 [8]byte
	if _, err := io.ReadFull(r, buf8[:]); err != nil {
		return nil, fmt.Errorf("reading request ID: %w", err)
	}
	requestID := int64(binary.BigEndian.Uint64(buf8[:]))

	// Read Status
	var statusBuf [1]byte
	if _, err := io.ReadFull(r, statusBuf[:]); err != nil {
		return nil, fmt.Errorf("reading status: %w", err)
	}
	status := StatusFlags(statusBuf[0])

	// Read VarHeaderLength
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, fmt.Errorf("reading var header length: %w", err)
	}
	varHeaderLength := int32(binary.BigEndian.Uint32(buf[:]))

	// Read variable header
	varHeaderData := make([]byte, varHeaderLength)
	if _, err := io.ReadFull(r, varHeaderData); err != nil {
		return nil, fmt.Errorf("reading variable header: %w", err)
	}

	// Parse variable header
	stream := NewStreamInput(&bufferReader{buf: varHeaderData})
	var action string
	var parentTaskID string

	// Action is only present for requests
	if status.IsRequest() {
		var err error
		action, err = stream.ReadString()
		if err != nil {
			return nil, fmt.Errorf("reading action: %w", err)
		}
	}

	// ParentTaskID always present
	var err error
	parentTaskID, err = stream.ReadString()
	if err != nil {
		return nil, fmt.Errorf("reading parent task ID: %w", err)
	}

	return &Header{
		MessageLength:   messageLength,
		RequestID:       requestID,
		Status:          status,
		Action:          action,
		ParentTaskID:    parentTaskID,
		varHeaderLength: varHeaderLength,
	}, nil
}

// PayloadSize returns the size of the payload following this header.
// PayloadSize = MessageLength - requestID(8) - status(1) - varHeaderLen(4) - varHeaderLength
func (h *Header) PayloadSize() int {
	return int(h.MessageLength) - 8 - 1 - 4 - int(h.varHeaderLength)
}

// bufferWriter is a simple writer that appends to a byte slice.
type bufferWriter struct {
	buf *[]byte
}

func (w *bufferWriter) Write(p []byte) (n int, err error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}

// bufferReader is a simple reader that reads from a byte slice.
type bufferReader struct {
	buf []byte
	pos int
}

func (r *bufferReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.buf) {
		return 0, io.EOF
	}
	n = copy(p, r.buf[r.pos:])
	r.pos += n
	return n, nil
}
