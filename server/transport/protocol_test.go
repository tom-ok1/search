package transport

import (
	"bytes"
	"testing"
)

// TestStatusFlags tests setting and getting each flag.
func TestStatusFlags(t *testing.T) {
	var flags StatusFlags

	// Test IsRequest
	if flags.IsRequest() {
		t.Error("expected IsRequest to be false initially")
	}
	flags = flags.WithRequest(true)
	if !flags.IsRequest() {
		t.Error("expected IsRequest to be true after WithRequest(true)")
	}
	flags = flags.WithRequest(false)
	if flags.IsRequest() {
		t.Error("expected IsRequest to be false after WithRequest(false)")
	}

	// Test IsError
	if flags.IsError() {
		t.Error("expected IsError to be false initially")
	}
	flags = flags.WithError(true)
	if !flags.IsError() {
		t.Error("expected IsError to be true after WithError(true)")
	}
	flags = flags.WithError(false)
	if flags.IsError() {
		t.Error("expected IsError to be false after WithError(false)")
	}

	// Test IsHandshake
	if flags.IsHandshake() {
		t.Error("expected IsHandshake to be false initially")
	}
	flags = flags.WithHandshake(true)
	if !flags.IsHandshake() {
		t.Error("expected IsHandshake to be true after WithHandshake(true)")
	}
	flags = flags.WithHandshake(false)
	if flags.IsHandshake() {
		t.Error("expected IsHandshake to be false after WithHandshake(false)")
	}

	// Test multiple flags at once
	flags = StatusFlags(0).WithRequest(true).WithError(true)
	if !flags.IsRequest() {
		t.Error("expected IsRequest to be true")
	}
	if !flags.IsError() {
		t.Error("expected IsError to be true")
	}
	if flags.IsHandshake() {
		t.Error("expected IsHandshake to be false")
	}
}

// TestHeaderRequestRoundtrip tests roundtrip serialization of a request header.
func TestHeaderRequestRoundtrip(t *testing.T) {
	// Create a request header
	original := &Header{
		RequestID:    42,
		Status:       StatusFlags(0).WithRequest(true),
		Action:       "indices:data/write/index",
		ParentTaskID: "node1:5",
	}

	// Calculate variable header length for MessageLength
	var varBuf []byte
	varStream := NewStreamOutput(&bufferWriter{buf: &varBuf})
	if err := varStream.WriteString(original.Action); err != nil {
		t.Fatalf("failed to write action: %v", err)
	}
	if err := varStream.WriteString(original.ParentTaskID); err != nil {
		t.Fatalf("failed to write parent task ID: %v", err)
	}
	varHeaderLen := int32(len(varBuf))
	original.varHeaderLength = varHeaderLen
	original.MessageLength = 8 + 1 + 4 + varHeaderLen // requestID + status + varHeaderLen field + varHeader

	// Write to buffer
	var buf bytes.Buffer
	if err := original.Encode(&buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	// Read back
	parsed, err := ReadHeader(&buf)
	if err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	// Verify fields
	if parsed.MessageLength != original.MessageLength {
		t.Errorf("MessageLength mismatch: got %d, want %d", parsed.MessageLength, original.MessageLength)
	}
	if parsed.RequestID != original.RequestID {
		t.Errorf("RequestID mismatch: got %d, want %d", parsed.RequestID, original.RequestID)
	}
	if parsed.Status != original.Status {
		t.Errorf("Status mismatch: got %d, want %d", parsed.Status, original.Status)
	}
	if !parsed.Status.IsRequest() {
		t.Error("expected Status.IsRequest() to be true")
	}
	if parsed.Action != original.Action {
		t.Errorf("Action mismatch: got %q, want %q", parsed.Action, original.Action)
	}
	if parsed.ParentTaskID != original.ParentTaskID {
		t.Errorf("ParentTaskID mismatch: got %q, want %q", parsed.ParentTaskID, original.ParentTaskID)
	}
	if parsed.varHeaderLength != original.varHeaderLength {
		t.Errorf("varHeaderLength mismatch: got %d, want %d", parsed.varHeaderLength, original.varHeaderLength)
	}
}

// TestHeaderResponseRoundtrip tests roundtrip serialization of a response header.
func TestHeaderResponseRoundtrip(t *testing.T) {
	// Create a response header (no action)
	original := &Header{
		RequestID:    99,
		Status:       StatusFlags(0), // isRequest=false
		ParentTaskID: "",
	}

	// Calculate variable header length for MessageLength
	var varBuf []byte
	varStream := NewStreamOutput(&bufferWriter{buf: &varBuf})
	if err := varStream.WriteString(original.ParentTaskID); err != nil {
		t.Fatalf("failed to write parent task ID: %v", err)
	}
	varHeaderLen := int32(len(varBuf))
	original.varHeaderLength = varHeaderLen
	original.MessageLength = 8 + 1 + 4 + varHeaderLen // requestID + status + varHeaderLen field + varHeader

	// Write to buffer
	var buf bytes.Buffer
	if err := original.Encode(&buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	// Read back
	parsed, err := ReadHeader(&buf)
	if err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	// Verify fields
	if parsed.MessageLength != original.MessageLength {
		t.Errorf("MessageLength mismatch: got %d, want %d", parsed.MessageLength, original.MessageLength)
	}
	if parsed.RequestID != original.RequestID {
		t.Errorf("RequestID mismatch: got %d, want %d", parsed.RequestID, original.RequestID)
	}
	if parsed.Status != original.Status {
		t.Errorf("Status mismatch: got %d, want %d", parsed.Status, original.Status)
	}
	if parsed.Status.IsRequest() {
		t.Error("expected Status.IsRequest() to be false for response")
	}
	if parsed.Action != "" {
		t.Errorf("Action should be empty for response, got %q", parsed.Action)
	}
	if parsed.ParentTaskID != original.ParentTaskID {
		t.Errorf("ParentTaskID mismatch: got %q, want %q", parsed.ParentTaskID, original.ParentTaskID)
	}
	if parsed.varHeaderLength != original.varHeaderLength {
		t.Errorf("varHeaderLength mismatch: got %d, want %d", parsed.varHeaderLength, original.varHeaderLength)
	}
}

// TestHeaderMarkerVerification tests that the marker bytes are 'E', 'S'.
func TestHeaderMarkerVerification(t *testing.T) {
	// Create a simple header
	header := &Header{
		RequestID:    1,
		Status:       StatusFlags(0),
		ParentTaskID: "",
	}

	// Calculate MessageLength
	var varBuf []byte
	varStream := NewStreamOutput(&bufferWriter{buf: &varBuf})
	if err := varStream.WriteString(header.ParentTaskID); err != nil {
		t.Fatalf("failed to write parent task ID: %v", err)
	}
	varHeaderLen := int32(len(varBuf))
	header.varHeaderLength = varHeaderLen
	header.MessageLength = 8 + 1 + 4 + varHeaderLen

	// Write to buffer
	var buf bytes.Buffer
	if err := header.Encode(&buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	// Verify first two bytes are 'E', 'S'
	data := buf.Bytes()
	if len(data) < 2 {
		t.Fatal("buffer too short")
	}
	if data[0] != 'E' {
		t.Errorf("first marker byte: got %c, want E", data[0])
	}
	if data[1] != 'S' {
		t.Errorf("second marker byte: got %c, want S", data[1])
	}

	// Test invalid marker
	invalidBuf := bytes.NewBuffer([]byte("XX"))
	invalidBuf.Write(data[2:]) // rest of the header
	_, err := ReadHeader(invalidBuf)
	if err == nil {
		t.Error("expected error for invalid marker, got nil")
	}
}

// TestHeaderPayloadSize tests that PayloadSize is calculated correctly.
func TestHeaderPayloadSize(t *testing.T) {
	// Create a header with known sizes
	header := &Header{
		RequestID:    42,
		Status:       StatusFlags(0).WithRequest(true),
		Action:       "test:action",
		ParentTaskID: "task1",
	}

	// Calculate variable header length
	var varBuf []byte
	varStream := NewStreamOutput(&bufferWriter{buf: &varBuf})
	if err := varStream.WriteString(header.Action); err != nil {
		t.Fatalf("failed to write action: %v", err)
	}
	if err := varStream.WriteString(header.ParentTaskID); err != nil {
		t.Fatalf("failed to write parent task ID: %v", err)
	}
	varHeaderLen := int32(len(varBuf))
	header.varHeaderLength = varHeaderLen

	// Set MessageLength to include some payload
	payloadSize := 100
	header.MessageLength = 8 + 1 + 4 + varHeaderLen + int32(payloadSize)

	// Verify PayloadSize calculation
	calculatedPayloadSize := header.PayloadSize()
	if calculatedPayloadSize != payloadSize {
		t.Errorf("PayloadSize mismatch: got %d, want %d", calculatedPayloadSize, payloadSize)
	}

	// Test with zero payload
	header.MessageLength = 8 + 1 + 4 + varHeaderLen
	calculatedPayloadSize = header.PayloadSize()
	if calculatedPayloadSize != 0 {
		t.Errorf("PayloadSize for zero payload: got %d, want 0", calculatedPayloadSize)
	}
}
