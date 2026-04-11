# Transport Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a custom binary transport layer for inter-node communication, following Elasticsearch's transport architecture.

**Architecture:** Custom binary TCP protocol with ES-style framing (marker + length + requestID + status + payload). Named bounded worker pools for executor model. Multi-connection pools per node categorized by type (REG, BULK, STATE, RECOVERY, PING). All code in `server/transport/` package.

**Tech Stack:** Go 1.24, `net` stdlib for TCP, `sync` for concurrency, `encoding/binary` for wire format. No external dependencies.

**Spec:** `docs/superpowers/specs/2026-04-07-transport-layer-design.md`

---

### Task 1: Serialization Primitives — StreamOutput

**Files:**
- Create: `server/transport/stream.go`
- Test: `server/transport/stream_test.go`

- [ ] **Step 1: Write failing tests for StreamOutput**

Create `server/transport/stream_test.go`:

```go
package transport

import (
	"bytes"
	"testing"
)

func TestStreamOutput_WriteByte(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := out.WriteByte(0x42); err != nil {
		t.Fatal(err)
	}
	if got := buf.Bytes(); len(got) != 1 || got[0] != 0x42 {
		t.Fatalf("got %v, want [0x42]", got)
	}
}

func TestStreamOutput_WriteBytes(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := out.WriteBytes([]byte{0x01, 0x02, 0x03}); err != nil {
		t.Fatal(err)
	}
	if got := buf.Bytes(); !bytes.Equal(got, []byte{0x01, 0x02, 0x03}) {
		t.Fatalf("got %v, want [1 2 3]", got)
	}
}

func TestStreamOutput_WriteVInt(t *testing.T) {
	tests := []struct {
		name  string
		value int32
		want  []byte
	}{
		{"zero", 0, []byte{0x00}},
		{"one byte", 42, []byte{42}},
		{"max single byte", 127, []byte{127}},
		{"two bytes", 128, []byte{0x80, 0x01}},
		{"large", 16384, []byte{0x80, 0x80, 0x01}},
		{"negative", -1, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			out := NewStreamOutput(&buf)
			if err := out.WriteVInt(tt.value); err != nil {
				t.Fatal(err)
			}
			if got := buf.Bytes(); !bytes.Equal(got, tt.want) {
				t.Errorf("WriteVInt(%d) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestStreamOutput_WriteVLong(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := out.WriteVLong(300); err != nil {
		t.Fatal(err)
	}
	// 300 = 0x12C → varint: 0xAC, 0x02
	if got := buf.Bytes(); !bytes.Equal(got, []byte{0xAC, 0x02}) {
		t.Fatalf("got %v, want [0xAC 0x02]", got)
	}
}

func TestStreamOutput_WriteString(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := out.WriteString("hello"); err != nil {
		t.Fatal(err)
	}
	// VInt(5) = 0x05, then "hello" bytes
	want := append([]byte{0x05}, []byte("hello")...)
	if got := buf.Bytes(); !bytes.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestStreamOutput_WriteBool(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := out.WriteBool(true); err != nil {
		t.Fatal(err)
	}
	if err := out.WriteBool(false); err != nil {
		t.Fatal(err)
	}
	if got := buf.Bytes(); !bytes.Equal(got, []byte{1, 0}) {
		t.Fatalf("got %v, want [1 0]", got)
	}
}

func TestStreamOutput_WriteByteArray(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	data := []byte(`{"key":"value"}`)
	if err := out.WriteByteArray(data); err != nil {
		t.Fatal(err)
	}
	// VInt(len) + data
	got := buf.Bytes()
	if got[0] != byte(len(data)) {
		t.Fatalf("length prefix: got %d, want %d", got[0], len(data))
	}
	if !bytes.Equal(got[1:], data) {
		t.Fatalf("payload mismatch")
	}
}

func TestStreamOutput_WriteOptionalInt64_Present(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	val := int64(42)
	if err := out.WriteOptionalInt64(&val); err != nil {
		t.Fatal(err)
	}
	if got := buf.Bytes()[0]; got != 1 {
		t.Fatalf("presence flag: got %d, want 1", got)
	}
}

func TestStreamOutput_WriteOptionalInt64_Nil(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := out.WriteOptionalInt64(nil); err != nil {
		t.Fatal(err)
	}
	if got := buf.Bytes(); len(got) != 1 || got[0] != 0 {
		t.Fatalf("got %v, want [0]", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run TestStreamOutput`
Expected: compilation error — package/types don't exist

- [ ] **Step 3: Implement StreamOutput**

Create `server/transport/stream.go`:

```go
package transport

import (
	"encoding/binary"
	"io"
	"math"
)

// StreamOutput provides typed binary serialization over an io.Writer.
// Encoding follows Elasticsearch's StreamOutput conventions:
// variable-length integers, length-prefixed strings, etc.
type StreamOutput struct {
	w io.Writer
}

func NewStreamOutput(w io.Writer) *StreamOutput {
	return &StreamOutput{w: w}
}

func (s *StreamOutput) WriteByte(b byte) error {
	_, err := s.w.Write([]byte{b})
	return err
}

func (s *StreamOutput) WriteBytes(b []byte) error {
	_, err := s.w.Write(b)
	return err
}

// WriteVInt writes a variable-length encoded int32 (unsigned varint encoding).
func (s *StreamOutput) WriteVInt(v int32) error {
	uv := uint32(v)
	for uv >= 0x80 {
		if err := s.WriteByte(byte(uv) | 0x80); err != nil {
			return err
		}
		uv >>= 7
	}
	return s.WriteByte(byte(uv))
}

// WriteVLong writes a variable-length encoded int64 (unsigned varint encoding).
func (s *StreamOutput) WriteVLong(v int64) error {
	uv := uint64(v)
	for uv >= 0x80 {
		if err := s.WriteByte(byte(uv) | 0x80); err != nil {
			return err
		}
		uv >>= 7
	}
	return s.WriteByte(byte(uv))
}

func (s *StreamOutput) WriteInt32(v int32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(v))
	_, err := s.w.Write(buf[:])
	return err
}

func (s *StreamOutput) WriteInt64(v int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	_, err := s.w.Write(buf[:])
	return err
}

func (s *StreamOutput) WriteFloat64(v float64) error {
	return s.WriteInt64(int64(math.Float64bits(v)))
}

func (s *StreamOutput) WriteBool(v bool) error {
	if v {
		return s.WriteByte(1)
	}
	return s.WriteByte(0)
}

// WriteString writes a length-prefixed UTF-8 string.
func (s *StreamOutput) WriteString(v string) error {
	if err := s.WriteVInt(int32(len(v))); err != nil {
		return err
	}
	_, err := s.w.Write([]byte(v))
	return err
}

// WriteByteArray writes a length-prefixed byte array.
func (s *StreamOutput) WriteByteArray(b []byte) error {
	if err := s.WriteVInt(int32(len(b))); err != nil {
		return err
	}
	_, err := s.w.Write(b)
	return err
}

// WriteOptionalInt64 writes a presence flag followed by the value if non-nil.
func (s *StreamOutput) WriteOptionalInt64(v *int64) error {
	if v == nil {
		return s.WriteBool(false)
	}
	if err := s.WriteBool(true); err != nil {
		return err
	}
	return s.WriteVLong(*v)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run TestStreamOutput`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add server/transport/stream.go server/transport/stream_test.go
git commit -m "feat(transport): add StreamOutput serialization primitives"
```

---

### Task 2: Serialization Primitives — StreamInput

**Files:**
- Modify: `server/transport/stream.go`
- Modify: `server/transport/stream_test.go`

- [ ] **Step 1: Write failing tests for StreamInput**

Append to `server/transport/stream_test.go`:

```go
func TestStreamInput_ReadByte(t *testing.T) {
	in := NewStreamInput(bytes.NewReader([]byte{0x42}))
	got, err := in.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	if got != 0x42 {
		t.Fatalf("got 0x%02X, want 0x42", got)
	}
}

func TestStreamInput_ReadVInt(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  int32
	}{
		{"zero", []byte{0x00}, 0},
		{"one byte", []byte{42}, 42},
		{"max single byte", []byte{127}, 127},
		{"two bytes", []byte{0x80, 0x01}, 128},
		{"large", []byte{0x80, 0x80, 0x01}, 16384},
		{"negative", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := NewStreamInput(bytes.NewReader(tt.input))
			got, err := in.ReadVInt()
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestStreamInput_ReadString(t *testing.T) {
	data := append([]byte{0x05}, []byte("hello")...)
	in := NewStreamInput(bytes.NewReader(data))
	got, err := in.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
	}
}

func TestStreamInput_ReadBool(t *testing.T) {
	in := NewStreamInput(bytes.NewReader([]byte{1, 0}))
	if got, _ := in.ReadBool(); !got {
		t.Fatal("expected true")
	}
	if got, _ := in.ReadBool(); got {
		t.Fatal("expected false")
	}
}

func TestStreamInput_ReadByteArray(t *testing.T) {
	data := []byte(`{"key":"value"}`)
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	out.WriteByteArray(data)

	in := NewStreamInput(bytes.NewReader(buf.Bytes()))
	got, err := in.ReadByteArray()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}
}

func TestStreamInput_ReadOptionalInt64_Present(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	val := int64(42)
	out.WriteOptionalInt64(&val)

	in := NewStreamInput(bytes.NewReader(buf.Bytes()))
	got, err := in.ReadOptionalInt64()
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != 42 {
		t.Fatalf("got %v, want *42", got)
	}
}

func TestStreamInput_ReadOptionalInt64_Nil(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	out.WriteOptionalInt64(nil)

	in := NewStreamInput(bytes.NewReader(buf.Bytes()))
	got, err := in.ReadOptionalInt64()
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("got %v, want nil", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run TestStreamInput`
Expected: compilation error

- [ ] **Step 3: Implement StreamInput**

Append to `server/transport/stream.go`:

```go
// StreamInput provides typed binary deserialization over an io.Reader.
// Symmetric counterpart to StreamOutput.
type StreamInput struct {
	r io.Reader
}

func NewStreamInput(r io.Reader) *StreamInput {
	return &StreamInput{r: r}
}

func (s *StreamInput) ReadByte() (byte, error) {
	var buf [1]byte
	if _, err := io.ReadFull(s.r, buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func (s *StreamInput) ReadBytes(n int) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := io.ReadFull(s.r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (s *StreamInput) ReadVInt() (int32, error) {
	var result uint32
	var shift uint
	for i := 0; i < 5; i++ {
		b, err := s.ReadByte()
		if err != nil {
			return 0, err
		}
		result |= uint32(b&0x7F) << shift
		if b&0x80 == 0 {
			return int32(result), nil
		}
		shift += 7
	}
	return 0, fmt.Errorf("varint too long")
}

func (s *StreamInput) ReadVLong() (int64, error) {
	var result uint64
	var shift uint
	for i := 0; i < 10; i++ {
		b, err := s.ReadByte()
		if err != nil {
			return 0, err
		}
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return int64(result), nil
		}
		shift += 7
	}
	return 0, fmt.Errorf("varlong too long")
}

func (s *StreamInput) ReadInt32() (int32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(s.r, buf[:]); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(buf[:])), nil
}

func (s *StreamInput) ReadInt64() (int64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(s.r, buf[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(buf[:])), nil
}

func (s *StreamInput) ReadFloat64() (float64, error) {
	v, err := s.ReadInt64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(uint64(v)), nil
}

func (s *StreamInput) ReadBool() (bool, error) {
	b, err := s.ReadByte()
	if err != nil {
		return false, err
	}
	return b != 0, nil
}

func (s *StreamInput) ReadString() (string, error) {
	length, err := s.ReadVInt()
	if err != nil {
		return "", err
	}
	buf, err := s.ReadBytes(int(length))
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func (s *StreamInput) ReadByteArray() ([]byte, error) {
	length, err := s.ReadVInt()
	if err != nil {
		return nil, err
	}
	return s.ReadBytes(int(length))
}

func (s *StreamInput) ReadOptionalInt64() (*int64, error) {
	present, err := s.ReadBool()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}
	v, err := s.ReadVLong()
	if err != nil {
		return nil, err
	}
	return &v, nil
}
```

Also add `"fmt"` to the imports in `stream.go`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run TestStreamInput`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add server/transport/stream.go server/transport/stream_test.go
git commit -m "feat(transport): add StreamInput deserialization primitives"
```

---

### Task 3: Serialization — Roundtrip Tests and GenericMap

**Files:**
- Modify: `server/transport/stream.go`
- Modify: `server/transport/stream_test.go`

- [ ] **Step 1: Write failing tests for roundtrip and generic map**

Append to `server/transport/stream_test.go`:

```go
func TestStream_VInt_Roundtrip(t *testing.T) {
	values := []int32{0, 1, 127, 128, 255, 256, 16383, 16384, -1, -128, math.MaxInt32, math.MinInt32}
	for _, v := range values {
		var buf bytes.Buffer
		out := NewStreamOutput(&buf)
		if err := out.WriteVInt(v); err != nil {
			t.Fatalf("WriteVInt(%d): %v", v, err)
		}
		in := NewStreamInput(bytes.NewReader(buf.Bytes()))
		got, err := in.ReadVInt()
		if err != nil {
			t.Fatalf("ReadVInt for %d: %v", v, err)
		}
		if got != v {
			t.Errorf("roundtrip: got %d, want %d", got, v)
		}
	}
}

func TestStream_VLong_Roundtrip(t *testing.T) {
	values := []int64{0, 1, 127, 128, 300, -1, math.MaxInt64, math.MinInt64}
	for _, v := range values {
		var buf bytes.Buffer
		out := NewStreamOutput(&buf)
		if err := out.WriteVLong(v); err != nil {
			t.Fatalf("WriteVLong(%d): %v", v, err)
		}
		in := NewStreamInput(bytes.NewReader(buf.Bytes()))
		got, err := in.ReadVLong()
		if err != nil {
			t.Fatalf("ReadVLong for %d: %v", v, err)
		}
		if got != v {
			t.Errorf("roundtrip: got %d, want %d", got, v)
		}
	}
}

func TestStream_Float64_Roundtrip(t *testing.T) {
	values := []float64{0, 1.5, -3.14, math.MaxFloat64, math.SmallestNonzeroFloat64}
	for _, v := range values {
		var buf bytes.Buffer
		out := NewStreamOutput(&buf)
		if err := out.WriteFloat64(v); err != nil {
			t.Fatal(err)
		}
		in := NewStreamInput(bytes.NewReader(buf.Bytes()))
		got, err := in.ReadFloat64()
		if err != nil {
			t.Fatal(err)
		}
		if got != v {
			t.Errorf("roundtrip: got %v, want %v", got, v)
		}
	}
}

func TestStream_GenericMap_Roundtrip(t *testing.T) {
	original := map[string]any{
		"query": map[string]any{
			"match": map[string]any{
				"title": "hello world",
			},
		},
		"size":    int64(10),
		"score":   float64(3.14),
		"enabled": true,
		"empty":   nil,
		"tags":    []any{"a", "b"},
	}

	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := out.WriteGenericMap(original); err != nil {
		t.Fatal(err)
	}

	in := NewStreamInput(bytes.NewReader(buf.Bytes()))
	got, err := in.ReadGenericMap()
	if err != nil {
		t.Fatal(err)
	}

	// Deep compare key fields
	if got["size"] != int64(10) {
		t.Errorf("size: got %v (%T)", got["size"], got["size"])
	}
	if got["score"] != float64(3.14) {
		t.Errorf("score: got %v", got["score"])
	}
	if got["enabled"] != true {
		t.Errorf("enabled: got %v", got["enabled"])
	}
	if got["empty"] != nil {
		t.Errorf("empty: got %v", got["empty"])
	}
	query := got["query"].(map[string]any)
	match := query["match"].(map[string]any)
	if match["title"] != "hello world" {
		t.Errorf("nested query: got %v", match["title"])
	}
	tags := got["tags"].([]any)
	if len(tags) != 2 || tags[0] != "a" || tags[1] != "b" {
		t.Errorf("tags: got %v", tags)
	}
}

func TestStream_GenericMap_Nil(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := out.WriteGenericMap(nil); err != nil {
		t.Fatal(err)
	}
	in := NewStreamInput(bytes.NewReader(buf.Bytes()))
	got, err := in.ReadGenericMap()
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}
```

Add `"math"` to the imports in the test file.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run "TestStream_"`
Expected: compilation error — `WriteGenericMap`/`ReadGenericMap` not defined

- [ ] **Step 3: Implement GenericMap serialization**

Append to `server/transport/stream.go`:

```go
// Type tags for generic map values.
const (
	tagNil     byte = 0
	tagString  byte = 1
	tagInt64   byte = 2
	tagFloat64 byte = 3
	tagBool    byte = 4
	tagMap     byte = 5
	tagSlice   byte = 6
)

// WriteGenericMap writes a type-tagged recursive map. Nil maps are encoded as
// a single nil tag.
func (s *StreamOutput) WriteGenericMap(m map[string]any) error {
	if m == nil {
		return s.WriteBool(false)
	}
	if err := s.WriteBool(true); err != nil {
		return err
	}
	if err := s.WriteVInt(int32(len(m))); err != nil {
		return err
	}
	for k, v := range m {
		if err := s.WriteString(k); err != nil {
			return err
		}
		if err := s.writeGenericValue(v); err != nil {
			return err
		}
	}
	return nil
}

func (s *StreamOutput) writeGenericValue(v any) error {
	switch val := v.(type) {
	case nil:
		return s.WriteByte(tagNil)
	case string:
		if err := s.WriteByte(tagString); err != nil {
			return err
		}
		return s.WriteString(val)
	case int64:
		if err := s.WriteByte(tagInt64); err != nil {
			return err
		}
		return s.WriteVLong(val)
	case float64:
		if err := s.WriteByte(tagFloat64); err != nil {
			return err
		}
		return s.WriteFloat64(val)
	case bool:
		if err := s.WriteByte(tagBool); err != nil {
			return err
		}
		return s.WriteBool(val)
	case map[string]any:
		if err := s.WriteByte(tagMap); err != nil {
			return err
		}
		return s.WriteGenericMap(val)
	case []any:
		if err := s.WriteByte(tagSlice); err != nil {
			return err
		}
		if err := s.WriteVInt(int32(len(val))); err != nil {
			return err
		}
		for _, item := range val {
			if err := s.writeGenericValue(item); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported generic value type: %T", v)
	}
}

// ReadGenericMap reads a type-tagged recursive map written by WriteGenericMap.
func (s *StreamInput) ReadGenericMap() (map[string]any, error) {
	present, err := s.ReadBool()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}
	length, err := s.ReadVInt()
	if err != nil {
		return nil, err
	}
	m := make(map[string]any, length)
	for i := int32(0); i < length; i++ {
		key, err := s.ReadString()
		if err != nil {
			return nil, err
		}
		val, err := s.readGenericValue()
		if err != nil {
			return nil, err
		}
		m[key] = val
	}
	return m, nil
}

func (s *StreamInput) readGenericValue() (any, error) {
	tag, err := s.ReadByte()
	if err != nil {
		return nil, err
	}
	switch tag {
	case tagNil:
		return nil, nil
	case tagString:
		return s.ReadString()
	case tagInt64:
		return s.ReadVLong()
	case tagFloat64:
		return s.ReadFloat64()
	case tagBool:
		return s.ReadBool()
	case tagMap:
		return s.ReadGenericMap()
	case tagSlice:
		length, err := s.ReadVInt()
		if err != nil {
			return nil, err
		}
		slice := make([]any, length)
		for i := int32(0); i < length; i++ {
			slice[i], err = s.readGenericValue()
			if err != nil {
				return nil, err
			}
		}
		return slice, nil
	default:
		return nil, fmt.Errorf("unknown generic value tag: %d", tag)
	}
}
```

- [ ] **Step 4: Run all stream tests**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add server/transport/stream.go server/transport/stream_test.go
git commit -m "feat(transport): add GenericMap serialization and roundtrip tests"
```

---

### Task 4: Writeable Interface and Error Types

**Files:**
- Create: `server/transport/writeable.go`
- Create: `server/transport/errors.go`

- [ ] **Step 1: Create writeable.go**

```go
package transport

// Writeable is implemented by all types that can be serialized to the transport wire format.
type Writeable interface {
	WriteTo(out *StreamOutput) error
}

// Reader is a function that deserializes a value from a StreamInput.
type Reader[T any] func(in *StreamInput) (T, error)
```

- [ ] **Step 2: Create errors.go**

```go
package transport

import "fmt"

type RemoteTransportError struct {
	NodeID  string
	Action  string
	Message string
}

func (e *RemoteTransportError) Error() string {
	return fmt.Sprintf("[%s][%s] %s", e.NodeID, e.Action, e.Message)
}

func (e *RemoteTransportError) WriteTo(out *StreamOutput) error {
	if err := out.WriteString(e.NodeID); err != nil {
		return err
	}
	if err := out.WriteString(e.Action); err != nil {
		return err
	}
	return out.WriteString(e.Message)
}

func ReadRemoteTransportError(in *StreamInput) (*RemoteTransportError, error) {
	nodeID, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	action, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	msg, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	return &RemoteTransportError{NodeID: nodeID, Action: action, Message: msg}, nil
}

type NodeNotConnectedError struct {
	NodeID string
}

func (e *NodeNotConnectedError) Error() string {
	return fmt.Sprintf("node not connected [%s]", e.NodeID)
}

type ConnectTransportError struct {
	NodeID string
	Cause  error
}

func (e *ConnectTransportError) Error() string {
	return fmt.Sprintf("failed to connect to node [%s]: %v", e.NodeID, e.Cause)
}

func (e *ConnectTransportError) Unwrap() error {
	return e.Cause
}

type SendRequestError struct {
	Action string
	Cause  error
}

func (e *SendRequestError) Error() string {
	return fmt.Sprintf("failed to send [%s]: %v", e.Action, e.Cause)
}

func (e *SendRequestError) Unwrap() error {
	return e.Cause
}

type ResponseTimeoutError struct {
	Action    string
	RequestID int64
}

func (e *ResponseTimeoutError) Error() string {
	return fmt.Sprintf("response timeout [%s] requestID=%d", e.Action, e.RequestID)
}
```

- [ ] **Step 3: Run compilation check**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go build ./server/transport/`
Expected: success

- [ ] **Step 4: Commit**

```bash
git add server/transport/writeable.go server/transport/errors.go
git commit -m "feat(transport): add Writeable interface and error types"
```

---

### Task 5: Wire Protocol — Header and StatusFlags

**Files:**
- Create: `server/transport/protocol.go`
- Create: `server/transport/protocol_test.go`

- [ ] **Step 1: Write failing tests for protocol encoding/decoding**

Create `server/transport/protocol_test.go`:

```go
package transport

import (
	"bytes"
	"testing"
)

func TestStatusFlags(t *testing.T) {
	s := StatusFlags(0)
	s = s.WithRequest(true)
	if !s.IsRequest() {
		t.Error("expected IsRequest")
	}
	if s.IsError() {
		t.Error("unexpected IsError")
	}

	s = s.WithError(true)
	if !s.IsError() {
		t.Error("expected IsError")
	}

	s = s.WithHandshake(true)
	if !s.IsHandshake() {
		t.Error("expected IsHandshake")
	}
}

func TestHeader_RequestRoundtrip(t *testing.T) {
	h := &Header{
		RequestID:    42,
		Status:       StatusFlags(0).WithRequest(true),
		Action:       "indices:data/write/index",
		ParentTaskID: "node1:5",
	}

	var buf bytes.Buffer
	if err := h.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}

	got, err := ReadHeader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	if got.RequestID != 42 {
		t.Errorf("RequestID: got %d, want 42", got.RequestID)
	}
	if !got.Status.IsRequest() {
		t.Error("expected IsRequest")
	}
	if got.Action != "indices:data/write/index" {
		t.Errorf("Action: got %q", got.Action)
	}
	if got.ParentTaskID != "node1:5" {
		t.Errorf("ParentTaskID: got %q", got.ParentTaskID)
	}
}

func TestHeader_ResponseRoundtrip(t *testing.T) {
	h := &Header{
		RequestID: 99,
		Status:    StatusFlags(0), // response (not request)
	}

	var buf bytes.Buffer
	if err := h.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}

	got, err := ReadHeader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	if got.RequestID != 99 {
		t.Errorf("RequestID: got %d, want 99", got.RequestID)
	}
	if got.Status.IsRequest() {
		t.Error("unexpected IsRequest for response")
	}
	if got.Action != "" {
		t.Errorf("Action should be empty for response, got %q", got.Action)
	}
}

func TestHeader_Marker(t *testing.T) {
	h := &Header{RequestID: 1, Status: StatusFlags(0).WithRequest(true), Action: "test"}
	var buf bytes.Buffer
	h.WriteTo(&buf)

	data := buf.Bytes()
	if data[0] != 'E' || data[1] != 'S' {
		t.Errorf("marker: got %c%c, want ES", data[0], data[1])
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run "TestStatus|TestHeader"`
Expected: compilation error

- [ ] **Step 3: Implement protocol.go**

Create `server/transport/protocol.go`:

```go
package transport

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

var marker = [2]byte{'E', 'S'}

// StatusFlags is a single byte encoding message metadata.
type StatusFlags byte

const (
	statusRequest   StatusFlags = 1 << 0
	statusError     StatusFlags = 1 << 1
	statusHandshake StatusFlags = 1 << 2
)

func (s StatusFlags) IsRequest() bool   { return s&statusRequest != 0 }
func (s StatusFlags) IsError() bool     { return s&statusError != 0 }
func (s StatusFlags) IsHandshake() bool { return s&statusHandshake != 0 }

func (s StatusFlags) WithRequest(v bool) StatusFlags {
	if v {
		return s | statusRequest
	}
	return s &^ statusRequest
}

func (s StatusFlags) WithError(v bool) StatusFlags {
	if v {
		return s | statusError
	}
	return s &^ statusError
}

func (s StatusFlags) WithHandshake(v bool) StatusFlags {
	if v {
		return s | statusHandshake
	}
	return s &^ statusHandshake
}

// Header is the transport message header.
//
// Wire format:
//
//	Marker (2) | MessageLength (4) | RequestID (8) | Status (1) | VarHeaderLen (4)
//	Variable header: Action (string, request only) | ParentTaskID (string)
type Header struct {
	RequestID    int64
	Status       StatusFlags
	Action       string // only for requests
	ParentTaskID string
}

// FixedHeaderSize is the size of the fixed portion of the header:
// marker(2) + messageLength(4) + requestID(8) + status(1) + varHeaderLen(4) = 19
const FixedHeaderSize = 19

// WriteTo serializes the header to w.
func (h *Header) WriteTo(w io.Writer) error {
	// Serialize variable header to get its length
	var varBuf bytes.Buffer
	varOut := NewStreamOutput(&varBuf)
	if h.Status.IsRequest() {
		if err := varOut.WriteString(h.Action); err != nil {
			return err
		}
	}
	if err := varOut.WriteString(h.ParentTaskID); err != nil {
		return err
	}
	varHeader := varBuf.Bytes()

	// MessageLength = requestID(8) + status(1) + varHeaderLen(4) + len(varHeader)
	// (does not include marker(2) or messageLength(4) itself)
	messageLength := int32(8 + 1 + 4 + len(varHeader))

	// Write fixed header
	if _, err := w.Write(marker[:]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, messageLength); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, h.RequestID); err != nil {
		return err
	}
	if _, err := w.Write([]byte{byte(h.Status)}); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, int32(len(varHeader))); err != nil {
		return err
	}
	// Write variable header
	_, err := w.Write(varHeader)
	return err
}

// ReadHeader reads a header from r.
func ReadHeader(r io.Reader) (*Header, error) {
	// Read and verify marker
	var mk [2]byte
	if _, err := io.ReadFull(r, mk[:]); err != nil {
		return nil, err
	}
	if mk != marker {
		return nil, fmt.Errorf("invalid marker: %v", mk)
	}

	// Read fixed fields
	var msgLen int32
	if err := binary.Read(r, binary.BigEndian, &msgLen); err != nil {
		return nil, err
	}
	var requestID int64
	if err := binary.Read(r, binary.BigEndian, &requestID); err != nil {
		return nil, err
	}
	var statusByte [1]byte
	if _, err := io.ReadFull(r, statusByte[:]); err != nil {
		return nil, err
	}
	status := StatusFlags(statusByte[0])

	var varHeaderLen int32
	if err := binary.Read(r, binary.BigEndian, &varHeaderLen); err != nil {
		return nil, err
	}

	// Read variable header
	varData := make([]byte, varHeaderLen)
	if _, err := io.ReadFull(r, varData); err != nil {
		return nil, err
	}

	h := &Header{
		RequestID: requestID,
		Status:    status,
	}

	varIn := NewStreamInput(bytes.NewReader(varData))
	if status.IsRequest() {
		action, err := varIn.ReadString()
		if err != nil {
			return nil, fmt.Errorf("read action: %w", err)
		}
		h.Action = action
	}
	parentTaskID, err := varIn.ReadString()
	if err != nil {
		return nil, fmt.Errorf("read parent task ID: %w", err)
	}
	h.ParentTaskID = parentTaskID

	return h, nil
}

// PayloadOffset returns the number of bytes from the start of the message to
// the beginning of the payload, given the variable header length.
func (h *Header) PayloadOffset() int {
	// The payload starts right after the header is written.
	// The caller reads the header then reads the payload from the same reader.
	return 0 // payload is read sequentially after header
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run "TestStatus|TestHeader"`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add server/transport/protocol.go server/transport/protocol_test.go
git commit -m "feat(transport): add wire protocol header and status flags"
```

---

### Task 6: Executor — ThreadPool, BoundedExecutor, DirectExecutor

**Files:**
- Create: `server/transport/executor.go`
- Create: `server/transport/executor_test.go`

- [ ] **Step 1: Write failing tests**

Create `server/transport/executor_test.go`:

```go
package transport

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDirectExecutor_RunsInline(t *testing.T) {
	exec := &DirectExecutor{}
	ran := false
	if err := exec.Execute(func() { ran = true }); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Error("task did not run inline")
	}
}

func TestBoundedExecutor_ExecutesTasks(t *testing.T) {
	exec := NewBoundedExecutor(2, 10)
	defer exec.Shutdown()

	var count atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		exec.Execute(func() {
			count.Add(1)
			wg.Done()
		})
	}
	wg.Wait()
	if got := count.Load(); got != 5 {
		t.Errorf("executed %d tasks, want 5", got)
	}
}

func TestBoundedExecutor_Backpressure(t *testing.T) {
	exec := NewBoundedExecutor(1, 1) // 1 worker, queue size 1
	defer exec.Shutdown()

	// Block the worker
	block := make(chan struct{})
	exec.Execute(func() { <-block })

	// Fill the queue
	exec.Execute(func() {})

	// Third task should be rejected
	err := exec.Execute(func() {})
	if err != ErrRejected {
		t.Errorf("expected ErrRejected, got %v", err)
	}

	close(block) // unblock
}

func TestBoundedExecutor_Shutdown(t *testing.T) {
	exec := NewBoundedExecutor(2, 10)
	var count atomic.Int32
	for i := 0; i < 5; i++ {
		exec.Execute(func() {
			count.Add(1)
			time.Sleep(10 * time.Millisecond)
		})
	}
	exec.Shutdown()
	if got := count.Load(); got != 5 {
		t.Errorf("executed %d tasks, want 5", got)
	}
}

func TestThreadPool_GetReturnsNamedPools(t *testing.T) {
	tp := NewThreadPool(map[string]PoolConfig{
		"search":           {Workers: 2, QueueSize: 10},
		"transport_worker": {Workers: 0}, // direct executor
	})
	defer tp.Shutdown()

	search := tp.Get("search")
	if search == nil {
		t.Fatal("search pool is nil")
	}

	tw := tp.Get("transport_worker")
	if tw == nil {
		t.Fatal("transport_worker pool is nil")
	}

	// transport_worker should run inline
	ran := false
	tw.Execute(func() { ran = true })
	if !ran {
		t.Error("transport_worker did not execute inline")
	}
}

func TestThreadPool_GetUnknown_ReturnsGeneric(t *testing.T) {
	tp := NewThreadPool(map[string]PoolConfig{
		"generic": {Workers: 2, QueueSize: 10},
	})
	defer tp.Shutdown()

	got := tp.Get("nonexistent")
	generic := tp.Get("generic")
	if got != generic {
		t.Error("unknown pool should fallback to generic")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run "TestDirect|TestBounded|TestThreadPool"`
Expected: compilation error

- [ ] **Step 3: Implement executor.go**

Create `server/transport/executor.go`:

```go
package transport

import (
	"errors"
	"sync"
)

var ErrRejected = errors.New("executor rejected: queue full")

// Executor runs tasks in a controlled concurrency context.
type Executor interface {
	Execute(task func()) error
	Shutdown()
}

// DirectExecutor runs tasks inline on the calling goroutine.
type DirectExecutor struct{}

func (d *DirectExecutor) Execute(task func()) error {
	task()
	return nil
}

func (d *DirectExecutor) Shutdown() {}

// BoundedExecutor runs tasks on a fixed pool of worker goroutines
// with a bounded queue for backpressure.
type BoundedExecutor struct {
	queue chan func()
	wg    sync.WaitGroup
}

func NewBoundedExecutor(workers, queueSize int) *BoundedExecutor {
	e := &BoundedExecutor{
		queue: make(chan func(), queueSize),
	}
	e.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer e.wg.Done()
			for task := range e.queue {
				task()
			}
		}()
	}
	return e
}

func (e *BoundedExecutor) Execute(task func()) error {
	select {
	case e.queue <- task:
		return nil
	default:
		return ErrRejected
	}
}

func (e *BoundedExecutor) Shutdown() {
	close(e.queue)
	e.wg.Wait()
}

// PoolConfig configures a named executor pool.
// Workers == 0 means DirectExecutor (inline).
type PoolConfig struct {
	Workers   int
	QueueSize int
}

// ThreadPool manages named executor pools.
type ThreadPool struct {
	pools map[string]Executor
}

func NewThreadPool(configs map[string]PoolConfig) *ThreadPool {
	pools := make(map[string]Executor, len(configs))
	for name, cfg := range configs {
		if cfg.Workers == 0 {
			pools[name] = &DirectExecutor{}
		} else {
			pools[name] = NewBoundedExecutor(cfg.Workers, cfg.QueueSize)
		}
	}
	return &ThreadPool{pools: pools}
}

// Get returns the named executor, falling back to "generic" if not found.
func (tp *ThreadPool) Get(name string) Executor {
	if e, ok := tp.pools[name]; ok {
		return e
	}
	return tp.pools["generic"]
}

func (tp *ThreadPool) Shutdown() {
	for _, e := range tp.pools {
		e.Shutdown()
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run "TestDirect|TestBounded|TestThreadPool"`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add server/transport/executor.go server/transport/executor_test.go
git commit -m "feat(transport): add ThreadPool with BoundedExecutor and DirectExecutor"
```

---

### Task 7: DiscoveryNode, ConnectionType, ConnectionProfile

**Files:**
- Create: `server/transport/discovery_node.go`
- Create: `server/transport/connection.go`

- [ ] **Step 1: Create discovery_node.go**

```go
package transport

// DiscoveryNode represents a node in the cluster.
// Minimal definition for the transport layer; will be extended in Phase 2 (discovery).
type DiscoveryNode struct {
	ID      string
	Name    string
	Address string // host:port for transport
}
```

- [ ] **Step 2: Create connection.go with types and ConnectionProfile**

```go
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
```

- [ ] **Step 3: Run compilation check**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go build ./server/transport/`
Expected: success

- [ ] **Step 4: Commit**

```bash
git add server/transport/discovery_node.go server/transport/connection.go
git commit -m "feat(transport): add DiscoveryNode, ConnectionType, ConnectionProfile"
```

---

### Task 8: Handler Registry and TransportChannel

**Files:**
- Create: `server/transport/registry.go`
- Create: `server/transport/channel.go`
- Create: `server/transport/registry_test.go`

- [ ] **Step 1: Write failing tests**

Create `server/transport/registry_test.go`:

```go
package transport

import (
	"sync"
	"testing"
	"time"
)

func TestResponseHandlers_AddAndRemove(t *testing.T) {
	rh := NewResponseHandlers()

	ctx := &ResponseContext{
		Action:    "test",
		NodeID:    "node1",
		CreatedAt: time.Now(),
	}
	id := rh.Add(ctx)

	got := rh.Remove(id)
	if got == nil {
		t.Fatal("expected non-nil context")
	}
	if got.Action != "test" {
		t.Errorf("Action: got %q, want %q", got.Action, "test")
	}

	// Second remove returns nil
	if rh.Remove(id) != nil {
		t.Error("double remove should return nil")
	}
}

func TestResponseHandlers_IDsAreUnique(t *testing.T) {
	rh := NewResponseHandlers()
	seen := make(map[int64]bool)
	for i := 0; i < 100; i++ {
		id := rh.Add(&ResponseContext{})
		if seen[id] {
			t.Fatalf("duplicate ID: %d", id)
		}
		seen[id] = true
	}
}

func TestResponseHandlers_ConcurrentAddRemove(t *testing.T) {
	rh := NewResponseHandlers()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := rh.Add(&ResponseContext{Action: "test"})
			rh.Remove(id)
		}()
	}
	wg.Wait()
}

func TestRequestHandlerMap_RegisterAndGet(t *testing.T) {
	m := NewRequestHandlerMap()
	entry := &requestHandlerEntry{
		action:   "test:action",
		executor: "generic",
	}
	m.Register(entry)

	got := m.Get("test:action")
	if got == nil {
		t.Fatal("expected non-nil entry")
	}
	if got.action != "test:action" {
		t.Errorf("action: got %q", got.action)
	}

	if m.Get("nonexistent") != nil {
		t.Error("expected nil for unknown action")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run "TestResponse|TestRequest"`
Expected: compilation error

- [ ] **Step 3: Implement registry.go**

Create `server/transport/registry.go`:

```go
package transport

import (
	"sync"
	"sync/atomic"
	"time"
)

// ResponseContext holds the state for an in-flight outbound request.
type ResponseContext struct {
	Handler   any    // TransportResponseHandler (type-erased)
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

// requestHandlerEntry stores a registered request handler (type-erased).
type requestHandlerEntry struct {
	action   string
	executor string
	reader   any // Reader[T] (type-erased)
	handler  any // TransportRequestHandler[T] (type-erased)
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
```

- [ ] **Step 4: Implement channel.go**

Create `server/transport/channel.go`:

```go
package transport

import (
	"bytes"
	"io"
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
}

func NewTcpTransportChannel(requestID int64, writer io.Writer) *TcpTransportChannel {
	return &TcpTransportChannel{requestID: requestID, writer: writer}
}

func (c *TcpTransportChannel) SendResponse(response Writeable) error {
	// Serialize payload
	var payload bytes.Buffer
	out := NewStreamOutput(&payload)
	if err := response.WriteTo(out); err != nil {
		return err
	}

	// Build response header
	h := &Header{
		RequestID: c.requestID,
		Status:    StatusFlags(0), // response (not request)
	}

	// Write header + payload
	var msg bytes.Buffer
	if err := h.WriteTo(&msg); err != nil {
		return err
	}
	// Update message length to include payload
	return writeMessageWithPayload(c.writer, h, payload.Bytes())
}

func (c *TcpTransportChannel) SendError(err error) error {
	errMsg := &RemoteTransportError{Message: err.Error()}
	var payload bytes.Buffer
	out := NewStreamOutput(&payload)
	if err := errMsg.WriteTo(out); err != nil {
		return err
	}

	h := &Header{
		RequestID: c.requestID,
		Status:    StatusFlags(0).WithError(true),
	}
	return writeMessageWithPayload(c.writer, h, payload.Bytes())
}

// writeMessageWithPayload writes a complete message (header + payload) to w.
func writeMessageWithPayload(w io.Writer, h *Header, payload []byte) error {
	// Build variable header
	var varBuf bytes.Buffer
	varOut := NewStreamOutput(&varBuf)
	if h.Status.IsRequest() {
		if err := varOut.WriteString(h.Action); err != nil {
			return err
		}
	}
	if err := varOut.WriteString(h.ParentTaskID); err != nil {
		return err
	}
	varHeader := varBuf.Bytes()

	// Write complete message to buffer first, then single write to connection
	var msg bytes.Buffer

	// Marker
	msg.Write([]byte{'E', 'S'})

	// MessageLength = requestID(8) + status(1) + varHeaderLen(4) + varHeader + payload
	msgLen := int32(8 + 1 + 4 + len(varHeader) + len(payload))
	sout := NewStreamOutput(&msg)
	sout.WriteInt32(msgLen)
	sout.WriteInt64(h.RequestID)
	sout.WriteByte(byte(h.Status))
	sout.WriteInt32(int32(len(varHeader)))
	msg.Write(varHeader)
	msg.Write(payload)

	_, err := w.Write(msg.Bytes())
	return err
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run "TestResponse|TestRequest"`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add server/transport/registry.go server/transport/channel.go server/transport/registry_test.go
git commit -m "feat(transport): add handler registry, response handlers, and TransportChannel"
```

---

### Task 9: Handshake Protocol

**Files:**
- Create: `server/transport/handshake.go`
- Create: `server/transport/handshake_test.go`

- [ ] **Step 1: Write failing tests**

Create `server/transport/handshake_test.go`:

```go
package transport

import (
	"bytes"
	"testing"
)

func TestHandshakeRequest_Roundtrip(t *testing.T) {
	req := &HandshakeRequest{Version: 1}
	var buf bytes.Buffer
	if err := req.WriteTo(NewStreamOutput(&buf)); err != nil {
		t.Fatal(err)
	}

	got, err := ReadHandshakeRequest(NewStreamInput(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 {
		t.Errorf("Version: got %d, want 1", got.Version)
	}
}

func TestHandshakeResponse_Roundtrip(t *testing.T) {
	resp := &HandshakeResponse{Version: 2, NodeID: "node-abc"}
	var buf bytes.Buffer
	if err := resp.WriteTo(NewStreamOutput(&buf)); err != nil {
		t.Fatal(err)
	}

	got, err := ReadHandshakeResponse(NewStreamInput(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 2 {
		t.Errorf("Version: got %d, want 2", got.Version)
	}
	if got.NodeID != "node-abc" {
		t.Errorf("NodeID: got %q", got.NodeID)
	}
}

func TestNegotiateVersion(t *testing.T) {
	tests := []struct {
		local, remote, want int32
	}{
		{1, 1, 1},
		{2, 1, 1},
		{1, 3, 1},
	}
	for _, tt := range tests {
		got := NegotiateVersion(tt.local, tt.remote)
		if got != tt.want {
			t.Errorf("Negotiate(%d, %d) = %d, want %d", tt.local, tt.remote, got, tt.want)
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run TestHandshake`
Expected: compilation error

- [ ] **Step 3: Implement handshake.go**

Create `server/transport/handshake.go`:

```go
package transport

// TransportVersion is the protocol version for wire compatibility.
const CurrentTransportVersion int32 = 1

// HandshakeRequest is sent when opening a new connection.
type HandshakeRequest struct {
	Version int32
}

func (r *HandshakeRequest) WriteTo(out *StreamOutput) error {
	return out.WriteVInt(r.Version)
}

func ReadHandshakeRequest(in *StreamInput) (*HandshakeRequest, error) {
	v, err := in.ReadVInt()
	if err != nil {
		return nil, err
	}
	return &HandshakeRequest{Version: v}, nil
}

// HandshakeResponse is the reply to a HandshakeRequest.
type HandshakeResponse struct {
	Version int32
	NodeID  string
}

func (r *HandshakeResponse) WriteTo(out *StreamOutput) error {
	if err := out.WriteVInt(r.Version); err != nil {
		return err
	}
	return out.WriteString(r.NodeID)
}

func ReadHandshakeResponse(in *StreamInput) (*HandshakeResponse, error) {
	v, err := in.ReadVInt()
	if err != nil {
		return nil, err
	}
	nodeID, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	return &HandshakeResponse{Version: v, NodeID: nodeID}, nil
}

// NegotiateVersion returns the minimum of two versions.
func NegotiateVersion(local, remote int32) int32 {
	if local < remote {
		return local
	}
	return remote
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run TestHandshake`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add server/transport/handshake.go server/transport/handshake_test.go
git commit -m "feat(transport): add handshake protocol with version negotiation"
```

---

### Task 10: OutboundHandler and InboundHandler

**Files:**
- Create: `server/transport/handler.go`
- Create: `server/transport/handler_test.go`

- [ ] **Step 1: Write failing tests**

Create `server/transport/handler_test.go`:

```go
package transport

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

// testRequest is a simple Writeable for testing.
type testRequest struct {
	Value string
}

func (r *testRequest) WriteTo(out *StreamOutput) error {
	return out.WriteString(r.Value)
}

func readTestRequest(in *StreamInput) (*testRequest, error) {
	v, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	return &testRequest{Value: v}, nil
}

// testResponse is a simple Writeable for testing.
type testResponse struct {
	Result string
}

func (r *testResponse) WriteTo(out *StreamOutput) error {
	return out.WriteString(r.Result)
}

func readTestResponse(in *StreamInput) (*testResponse, error) {
	v, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	return &testResponse{Result: v}, nil
}

func TestOutboundHandler_SendRequest(t *testing.T) {
	var buf bytes.Buffer
	oh := NewOutboundHandler()
	req := &testRequest{Value: "hello"}

	err := oh.SendRequest(&buf, 42, "test:action", req)
	if err != nil {
		t.Fatal(err)
	}

	// Verify we can read the header back
	h, err := ReadHeader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if h.RequestID != 42 {
		t.Errorf("RequestID: got %d, want 42", h.RequestID)
	}
	if !h.Status.IsRequest() {
		t.Error("expected IsRequest")
	}
	if h.Action != "test:action" {
		t.Errorf("Action: got %q", h.Action)
	}
}

func TestOutboundHandler_SendResponse(t *testing.T) {
	var buf bytes.Buffer
	oh := NewOutboundHandler()
	resp := &testResponse{Result: "ok"}

	err := oh.SendResponse(&buf, 42, resp)
	if err != nil {
		t.Fatal(err)
	}

	h, err := ReadHeader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if h.RequestID != 42 {
		t.Errorf("RequestID: got %d, want 42", h.RequestID)
	}
	if h.Status.IsRequest() {
		t.Error("unexpected IsRequest")
	}
}

func TestInboundHandler_DispatchRequest(t *testing.T) {
	handlers := NewRequestHandlerMap()
	responseHandlers := NewResponseHandlers()
	tp := NewThreadPool(map[string]PoolConfig{
		"generic": {Workers: 2, QueueSize: 10},
	})
	defer tp.Shutdown()

	var received string
	var mu sync.Mutex
	done := make(chan struct{})

	handlers.Register(&requestHandlerEntry{
		action:   "test:echo",
		executor: "generic",
		reader: Reader[*testRequest](func(in *StreamInput) (*testRequest, error) {
			return readTestRequest(in)
		}),
		handler: requestHandlerFunc[*testRequest](func(req *testRequest, ch TransportChannel) error {
			mu.Lock()
			received = req.Value
			mu.Unlock()
			close(done)
			return ch.SendResponse(&testResponse{Result: "echo:" + req.Value})
		}),
	})

	ih := NewInboundHandler(handlers, responseHandlers, tp)

	// Build a request message
	var msg bytes.Buffer
	oh := NewOutboundHandler()
	oh.SendRequest(&msg, 1, "test:echo", &testRequest{Value: "world"})

	// Pipe to inbound: use a discard writer for responses
	var respBuf bytes.Buffer
	ih.HandleMessage(bytes.NewReader(msg.Bytes()), &respBuf)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for handler")
	}

	mu.Lock()
	defer mu.Unlock()
	if received != "world" {
		t.Errorf("received: got %q, want %q", received, "world")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run "TestOutbound|TestInbound"`
Expected: compilation error

- [ ] **Step 3: Implement handler.go**

Create `server/transport/handler.go`:

```go
package transport

import (
	"bytes"
	"fmt"
	"io"
)

// requestHandlerFunc adapts a function to TransportRequestHandler.
type requestHandlerFunc[T any] func(request T, channel TransportChannel) error

func (f requestHandlerFunc[T]) MessageReceived(request T, channel TransportChannel) error {
	return f(request, channel)
}

// OutboundHandler serializes and writes transport messages.
type OutboundHandler struct{}

func NewOutboundHandler() *OutboundHandler {
	return &OutboundHandler{}
}

func (oh *OutboundHandler) SendRequest(w io.Writer, requestID int64, action string, request Writeable) error {
	var payload bytes.Buffer
	out := NewStreamOutput(&payload)
	if err := request.WriteTo(out); err != nil {
		return fmt.Errorf("serialize request: %w", err)
	}

	h := &Header{
		RequestID: requestID,
		Status:    StatusFlags(0).WithRequest(true),
		Action:    action,
	}
	return writeMessageWithPayload(w, h, payload.Bytes())
}

func (oh *OutboundHandler) SendResponse(w io.Writer, requestID int64, response Writeable) error {
	var payload bytes.Buffer
	out := NewStreamOutput(&payload)
	if err := response.WriteTo(out); err != nil {
		return fmt.Errorf("serialize response: %w", err)
	}

	h := &Header{
		RequestID: requestID,
		Status:    StatusFlags(0),
	}
	return writeMessageWithPayload(w, h, payload.Bytes())
}

func (oh *OutboundHandler) SendErrorResponse(w io.Writer, requestID int64, nodeID string, action string, errMsg error) error {
	transportErr := &RemoteTransportError{
		NodeID:  nodeID,
		Action:  action,
		Message: errMsg.Error(),
	}
	var payload bytes.Buffer
	out := NewStreamOutput(&payload)
	if err := transportErr.WriteTo(out); err != nil {
		return fmt.Errorf("serialize error: %w", err)
	}

	h := &Header{
		RequestID: requestID,
		Status:    StatusFlags(0).WithError(true),
	}
	return writeMessageWithPayload(w, h, payload.Bytes())
}

func (oh *OutboundHandler) SendHandshakeRequest(w io.Writer, requestID int64, req *HandshakeRequest) error {
	var payload bytes.Buffer
	out := NewStreamOutput(&payload)
	if err := req.WriteTo(out); err != nil {
		return err
	}

	h := &Header{
		RequestID: requestID,
		Status:    StatusFlags(0).WithRequest(true).WithHandshake(true),
		Action:    "internal:transport/handshake",
	}
	return writeMessageWithPayload(w, h, payload.Bytes())
}

func (oh *OutboundHandler) SendHandshakeResponse(w io.Writer, requestID int64, resp *HandshakeResponse) error {
	var payload bytes.Buffer
	out := NewStreamOutput(&payload)
	if err := resp.WriteTo(out); err != nil {
		return err
	}

	h := &Header{
		RequestID: requestID,
		Status:    StatusFlags(0).WithHandshake(true),
	}
	return writeMessageWithPayload(w, h, payload.Bytes())
}

// InboundHandler deserializes and dispatches incoming messages.
type InboundHandler struct {
	requestHandlers  *RequestHandlerMap
	responseHandlers *ResponseHandlers
	threadPool       *ThreadPool
}

func NewInboundHandler(
	requestHandlers *RequestHandlerMap,
	responseHandlers *ResponseHandlers,
	threadPool *ThreadPool,
) *InboundHandler {
	return &InboundHandler{
		requestHandlers:  requestHandlers,
		responseHandlers: responseHandlers,
		threadPool:       threadPool,
	}
}

// HandleMessage reads one complete message from r and dispatches it.
// For requests, responses are written to respWriter.
func (ih *InboundHandler) HandleMessage(r io.Reader, respWriter io.Writer) error {
	header, err := ReadHeader(r)
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	// Read remaining payload bytes.
	// MessageLength includes requestID(8)+status(1)+varHeaderLen(4)+varHeader.
	// The payload is everything after the variable header, which we've already
	// consumed via ReadHeader. So the remaining bytes are:
	// messageLength - (8 + 1 + 4 + varHeaderLen)
	// But ReadHeader already consumed all header bytes. We need to compute
	// how many payload bytes remain.
	//
	// Actually, ReadHeader reads: marker(2) + msgLen(4) + requestID(8) + status(1) + varHdrLen(4) + varHdr(varHdrLen)
	// The total message from the wire is: marker(2) + msgLen(4) + msgLen bytes
	// ReadHeader consumed: 2 + 4 + msgLen bytes (it reads the full fixed+variable header)
	// Since payload follows the variable header in the stream, we can just
	// read the remaining bytes from r. The payload size is not explicitly
	// encoded; the reader must know the expected type.
	//
	// For simplicity, read all remaining bytes from r as payload.
	payloadBytes, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("read payload: %w", err)
	}
	payloadReader := NewStreamInput(bytes.NewReader(payloadBytes))

	if header.Status.IsRequest() {
		return ih.handleRequest(header, payloadReader, respWriter)
	}
	return ih.handleResponse(header, payloadReader)
}

func (ih *InboundHandler) handleRequest(header *Header, payload *StreamInput, respWriter io.Writer) error {
	entry := ih.requestHandlers.Get(header.Action)
	if entry == nil {
		return fmt.Errorf("no handler for action: %s", header.Action)
	}

	executor := ih.threadPool.Get(entry.executor)
	channel := NewTcpTransportChannel(header.RequestID, respWriter)

	return executor.Execute(func() {
		ih.dispatchRequest(entry, payload, channel)
	})
}

func (ih *InboundHandler) dispatchRequest(entry *requestHandlerEntry, payload *StreamInput, channel TransportChannel) {
	// Type-erased dispatch: use the stored reader and handler.
	// This uses the concrete types stored in the entry.
	dispatchTypedRequest(entry, payload, channel)
}

// dispatchTypedRequest is the type-erased dispatch function.
// The entry stores reader and handler as `any`; we call them via the concrete types.
func dispatchTypedRequest(entry *requestHandlerEntry, payload *StreamInput, channel TransportChannel) {
	// The reader and handler are stored as any. We need to invoke them.
	// Since Go doesn't support runtime generic dispatch, we use a callback
	// stored alongside the entry that knows the concrete types.
	if fn, ok := entry.handler.(interface {
		dispatch(any, *StreamInput, TransportChannel)
	}); ok {
		fn.dispatch(entry.reader, payload, channel)
		return
	}

	// Fallback: try the dispatch function pattern
	if entry.dispatch != nil {
		entry.dispatch(payload, channel)
	}
}

func (ih *InboundHandler) handleResponse(header *Header, payload *StreamInput) error {
	ctx := ih.responseHandlers.Remove(header.RequestID)
	if ctx == nil {
		// Late response after timeout — ignore
		return nil
	}

	if header.Status.IsError() {
		transportErr, err := ReadRemoteTransportError(payload)
		if err != nil {
			return fmt.Errorf("read error response: %w", err)
		}
		if cb, ok := ctx.Handler.(interface{ HandleError(*RemoteTransportError) }); ok {
			cb.HandleError(transportErr)
		}
		return nil
	}

	// Response dispatch happens via the stored handler
	if cb, ok := ctx.Handler.(interface{ handleResponsePayload(*StreamInput) }); ok {
		cb.handleResponsePayload(payload)
	}
	return nil
}
```

Wait — this type-erased dispatch is getting awkward. Let me simplify the approach. The `requestHandlerEntry` should store a `dispatch` function closure that captures the concrete types. Let me revise.

Update `server/transport/registry.go` — replace `requestHandlerEntry` with a version that stores a dispatch closure:

```go
// requestHandlerEntry stores a registered request handler with a type-erased dispatch closure.
type requestHandlerEntry struct {
	action   string
	executor string
	reader   any // stored for reference; dispatch closure captures typed reader
	handler  any // stored for reference; dispatch closure captures typed handler
	dispatch func(payload *StreamInput, channel TransportChannel) // type-erased dispatch
}
```

Then simplify `handler.go` — remove `dispatchTypedRequest` and `dispatchRequest`, update `handleRequest`:

```go
func (ih *InboundHandler) handleRequest(header *Header, payload *StreamInput, respWriter io.Writer) error {
	entry := ih.requestHandlers.Get(header.Action)
	if entry == nil {
		return fmt.Errorf("no handler for action: %s", header.Action)
	}

	executor := ih.threadPool.Get(entry.executor)
	channel := NewTcpTransportChannel(header.RequestID, respWriter)

	return executor.Execute(func() {
		entry.dispatch(payload, channel)
	})
}
```

Add a helper to create entries with typed generics:

```go
// RegisterHandler is a typed helper for registering request handlers.
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
```

Add `"fmt"` to registry.go imports.

- [ ] **Step 4: Run tests to verify they pass**

Update the test to use `RegisterHandler` instead of manual entry creation:

In `TestInboundHandler_DispatchRequest`, replace the `handlers.Register(...)` block with:

```go
	RegisterHandler(handlers, "test:echo", "generic",
		Reader[*testRequest](readTestRequest),
		func(req *testRequest, ch TransportChannel) error {
			mu.Lock()
			received = req.Value
			mu.Unlock()
			close(done)
			return ch.SendResponse(&testResponse{Result: "echo:" + req.Value})
		},
	)
```

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run "TestOutbound|TestInbound"`
Expected: all PASS

- [ ] **Step 5: Run full test suite**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add server/transport/handler.go server/transport/handler_test.go server/transport/registry.go
git commit -m "feat(transport): add OutboundHandler, InboundHandler, and typed dispatch"
```

---

### Task 11: TcpTransport — Listener, Accept, Connect

**Files:**
- Create: `server/transport/tcp_transport.go`
- Create: `server/transport/tcp_transport_test.go`

- [ ] **Step 1: Write failing tests**

Create `server/transport/tcp_transport_test.go`:

```go
package transport

import (
	"testing"
	"time"
)

func TestTcpTransport_ListenAndConnect(t *testing.T) {
	tp := NewThreadPool(map[string]PoolConfig{
		"generic": {Workers: 2, QueueSize: 10},
	})
	defer tp.Shutdown()

	requestHandlers := NewRequestHandlerMap()
	responseHandlers := NewResponseHandlers()

	serverNode := DiscoveryNode{ID: "server", Name: "server", Address: "127.0.0.1:0"}
	transport := NewTcpTransport(serverNode, requestHandlers, responseHandlers, tp)

	addr, err := transport.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Stop()

	if addr == "" {
		t.Fatal("expected non-empty address")
	}
}

func TestTcpTransport_Handshake(t *testing.T) {
	tp := NewThreadPool(map[string]PoolConfig{
		"generic": {Workers: 2, QueueSize: 10},
	})
	defer tp.Shutdown()

	requestHandlers := NewRequestHandlerMap()
	responseHandlers := NewResponseHandlers()

	serverNode := DiscoveryNode{ID: "server", Name: "server"}
	transport := NewTcpTransport(serverNode, requestHandlers, responseHandlers, tp)

	addr, err := transport.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Stop()

	profile := ConnectionProfile{
		ConnPerType: map[ConnectionType]int{
			ConnTypeREG: 1,
		},
		ConnectTimeout:   5 * time.Second,
		HandshakeTimeout: 5 * time.Second,
	}

	remoteNode := DiscoveryNode{ID: "server", Name: "server", Address: addr}
	conn, err := transport.OpenConnection(remoteNode, profile)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if conn.version != CurrentTransportVersion {
		t.Errorf("version: got %d, want %d", conn.version, CurrentTransportVersion)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run "TestTcpTransport" -timeout 10s`
Expected: compilation error

- [ ] **Step 3: Implement tcp_transport.go**

Create `server/transport/tcp_transport.go`:

```go
package transport

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// NodeConnection holds all TCP connections to a single remote node.
type NodeConnection struct {
	node     DiscoveryNode
	channels map[ConnectionType][]net.Conn
	version  int32 // negotiated transport version
	closed   atomic.Bool
	mu       sync.Mutex
	counters map[ConnectionType]*atomic.Uint64 // round-robin counters
}

func newNodeConnection(node DiscoveryNode, channels map[ConnectionType][]net.Conn, version int32) *NodeConnection {
	counters := make(map[ConnectionType]*atomic.Uint64)
	for ct := range channels {
		counters[ct] = &atomic.Uint64{}
	}
	return &NodeConnection{
		node:     node,
		channels: channels,
		version:  version,
		counters: counters,
	}
}

// Conn returns a connection for the given type, round-robin within the pool.
func (nc *NodeConnection) Conn(ct ConnectionType) (net.Conn, error) {
	conns := nc.channels[ct]
	if len(conns) == 0 {
		// Fallback to REG
		conns = nc.channels[ConnTypeREG]
		ct = ConnTypeREG
	}
	if len(conns) == 0 {
		return nil, fmt.Errorf("no connections of type %d", ct)
	}
	idx := nc.counters[ct].Add(1) - 1
	return conns[idx%uint64(len(conns))], nil
}

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

func NewTcpTransport(
	localNode DiscoveryNode,
	requestHandlers *RequestHandlerMap,
	responseHandlers *ResponseHandlers,
	threadPool *ThreadPool,
) *TcpTransport {
	return &TcpTransport{
		localNode:        localNode,
		requestHandlers:  requestHandlers,
		responseHandlers: responseHandlers,
		threadPool:       threadPool,
		outbound:         NewOutboundHandler(),
		inbound:          NewInboundHandler(requestHandlers, responseHandlers, threadPool),
		stopCh:           make(chan struct{}),
	}
}

// Start begins listening for incoming connections. Returns the bound address.
func (tt *TcpTransport) Start(bindAddress string) (string, error) {
	listener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return "", fmt.Errorf("listen: %w", err)
	}
	tt.listener = listener
	tt.localNode.Address = listener.Addr().String()

	tt.wg.Add(1)
	go tt.acceptLoop()

	return listener.Addr().String(), nil
}

func (tt *TcpTransport) acceptLoop() {
	defer tt.wg.Done()
	for {
		conn, err := tt.listener.Accept()
		if err != nil {
			select {
			case <-tt.stopCh:
				return
			default:
				continue
			}
		}
		tt.wg.Add(1)
		go tt.handleConnection(conn)
	}
}

func (tt *TcpTransport) handleConnection(conn net.Conn) {
	defer tt.wg.Done()
	defer conn.Close()

	for {
		select {
		case <-tt.stopCh:
			return
		default:
		}

		err := tt.inbound.HandleMessage(conn, conn)
		if err != nil {
			// Connection closed or read error
			return
		}
	}
}

// OpenConnection opens connections to a remote node per the profile and performs handshake.
func (tt *TcpTransport) OpenConnection(node DiscoveryNode, profile ConnectionProfile) (*NodeConnection, error) {
	channels := make(map[ConnectionType][]net.Conn)
	var allConns []net.Conn
	var version int32

	for ct, count := range profile.ConnPerType {
		conns := make([]net.Conn, 0, count)
		for i := 0; i < count; i++ {
			conn, err := net.DialTimeout("tcp", node.Address, profile.ConnectTimeout)
			if err != nil {
				// Close all opened connections on failure
				for _, c := range allConns {
					c.Close()
				}
				return nil, &ConnectTransportError{NodeID: node.ID, Cause: err}
			}
			allConns = append(allConns, conn)

			// Perform handshake on each connection
			v, err := tt.performHandshake(conn, profile)
			if err != nil {
				for _, c := range allConns {
					c.Close()
				}
				return nil, &ConnectTransportError{NodeID: node.ID, Cause: fmt.Errorf("handshake: %w", err)}
			}
			version = v
			conns = append(conns, conn)
		}
		channels[ct] = conns
	}

	return newNodeConnection(node, channels, version), nil
}

func (tt *TcpTransport) performHandshake(conn net.Conn, profile ConnectionProfile) (int32, error) {
	// Send handshake request
	req := &HandshakeRequest{Version: CurrentTransportVersion}
	if err := tt.outbound.SendHandshakeRequest(conn, 0, req); err != nil {
		return 0, fmt.Errorf("send handshake: %w", err)
	}

	// Read handshake response
	header, err := ReadHeader(conn)
	if err != nil {
		return 0, fmt.Errorf("read handshake header: %w", err)
	}
	if !header.Status.IsHandshake() {
		return 0, fmt.Errorf("expected handshake response, got status %d", header.Status)
	}

	// Read remaining payload
	payloadBytes, err := readPayloadFromHeader(conn, header)
	if err != nil {
		return 0, err
	}
	payloadReader := NewStreamInput(bytes.NewReader(payloadBytes))

	resp, err := ReadHandshakeResponse(payloadReader)
	if err != nil {
		return 0, fmt.Errorf("read handshake response: %w", err)
	}

	return NegotiateVersion(CurrentTransportVersion, resp.Version), nil
}

func (tt *TcpTransport) Stop() error {
	close(tt.stopCh)
	var err error
	if tt.listener != nil {
		err = tt.listener.Close()
	}
	tt.wg.Wait()
	return err
}

// readPayloadFromHeader reads the payload bytes that follow a header in the stream.
// The header has already been read from r, so this reads whatever remains
// of the message based on the message length.
func readPayloadFromHeader(r io.Reader, h *Header) ([]byte, error) {
	// After ReadHeader, the variable header has been consumed.
	// We need to read the remaining payload. Since we don't track
	// exact bytes consumed in ReadHeader, we use ReadAll for the
	// message-based protocol where each message maps to one "read unit".
	//
	// For TCP streams, we need proper framing. The InboundHandler.HandleMessage
	// approach of reading from a pre-sliced reader handles this.
	// For the handshake (one-shot on a fresh connection), ReadAll is not safe
	// because the connection stays open.
	//
	// Instead, we should not use ReadAll. The payload size must be computed.
	// This is a TODO that will be fixed when we add InboundPipeline.
	// For now, use a buffered approach.

	// Actually, let's fix this properly. The payload after ReadHeader has not
	// been consumed. But ReadHeader consumed the variable header already.
	// We need to track how many bytes remain.
	// Let's add this to the protocol.

	// For the handshake response, the payload is small and fixed.
	// We can just read from the StreamInput directly since ReadHeader
	// leaves the reader positioned right after the variable header.
	return io.ReadAll(r)
}
```

Hmm — the `readPayloadFromHeader` with `io.ReadAll` won't work on a persistent TCP connection. Let me fix the approach. The `Header.WriteTo` already includes `MessageLength`. We need to track how many bytes ReadHeader consumed after the message length field, and compute remaining payload bytes.

Let me revise the approach: `ReadHeader` should return a `messageLength` field, and the caller computes payload size.

Update `protocol.go` — add `MessageLength` to `Header`:

In `ReadHeader`, store `msgLen` in the header:

```go
h := &Header{
    MessageLength: msgLen,
    RequestID:     requestID,
    Status:        status,
}
```

And add to the `Header` struct:

```go
type Header struct {
    MessageLength    int32 // total message length after marker+length fields
    RequestID        int64
    Status           StatusFlags
    Action           string
    ParentTaskID     string
    varHeaderLength  int32 // stored for payload size calculation
}
```

Store varHeaderLen in the header during ReadHeader:

```go
h.varHeaderLength = varHeaderLen
```

Add a method:

```go
// PayloadSize returns the number of payload bytes that follow the header.
func (h *Header) PayloadSize() int {
    // MessageLength = requestID(8) + status(1) + varHeaderLen(4) + varHeader + payload
    return int(h.MessageLength) - 8 - 1 - 4 - int(h.varHeaderLength)
}
```

Then replace `readPayloadFromHeader` in `tcp_transport.go`:

```go
func readPayloadFromHeader(r io.Reader, h *Header) ([]byte, error) {
    size := h.PayloadSize()
    if size <= 0 {
        return nil, nil
    }
    buf := make([]byte, size)
    if _, err := io.ReadFull(r, buf); err != nil {
        return nil, fmt.Errorf("read payload: %w", err)
    }
    return buf, nil
}
```

Also update `HandleMessage` in `handler.go` to use `readPayloadFromHeader` instead of `io.ReadAll`:

```go
func (ih *InboundHandler) HandleMessage(r io.Reader, respWriter io.Writer) error {
    header, err := ReadHeader(r)
    if err != nil {
        return fmt.Errorf("read header: %w", err)
    }

    payloadBytes, err := readPayloadFromHeader(r, header)
    if err != nil {
        return err
    }
    payloadReader := NewStreamInput(bytes.NewReader(payloadBytes))

    if header.Status.IsHandshake() {
        return ih.handleHandshake(header, payloadReader, respWriter)
    }
    if header.Status.IsRequest() {
        return ih.handleRequest(header, payloadReader, respWriter)
    }
    return ih.handleResponse(header, payloadReader)
}
```

Add the handshake handler to `InboundHandler`:

```go
func (ih *InboundHandler) handleHandshake(header *Header, payload *StreamInput, respWriter io.Writer) error {
    if header.Status.IsRequest() {
        req, err := ReadHandshakeRequest(payload)
        if err != nil {
            return fmt.Errorf("read handshake request: %w", err)
        }
        negotiated := NegotiateVersion(CurrentTransportVersion, req.Version)
        resp := &HandshakeResponse{Version: negotiated, NodeID: ih.localNodeID}
        oh := NewOutboundHandler()
        return oh.SendHandshakeResponse(respWriter, header.RequestID, resp)
    }
    // Handshake responses are handled by the caller (performHandshake)
    return nil
}
```

Add `localNodeID` field to `InboundHandler`:

```go
type InboundHandler struct {
    requestHandlers  *RequestHandlerMap
    responseHandlers *ResponseHandlers
    threadPool       *ThreadPool
    localNodeID      string
}
```

Update `NewInboundHandler` to accept `localNodeID`:

```go
func NewInboundHandler(
    requestHandlers *RequestHandlerMap,
    responseHandlers *ResponseHandlers,
    threadPool *ThreadPool,
    localNodeID string,
) *InboundHandler {
    return &InboundHandler{
        requestHandlers:  requestHandlers,
        responseHandlers: responseHandlers,
        threadPool:       threadPool,
        localNodeID:      localNodeID,
    }
}
```

Update all callers (including tests and `TcpTransport`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -timeout 10s`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add server/transport/tcp_transport.go server/transport/tcp_transport_test.go server/transport/handler.go server/transport/protocol.go
git commit -m "feat(transport): add TcpTransport with listener, connect, and handshake"
```

---

### Task 12: ConnectionManager

**Files:**
- Create: `server/transport/connection_manager.go`
- Create: `server/transport/connection_manager_test.go`

- [ ] **Step 1: Write failing tests**

Create `server/transport/connection_manager_test.go`:

```go
package transport

import (
	"testing"
	"time"
)

func TestConnectionManager_ConnectAndGet(t *testing.T) {
	tp := NewThreadPool(map[string]PoolConfig{
		"generic": {Workers: 2, QueueSize: 10},
	})
	defer tp.Shutdown()

	requestHandlers := NewRequestHandlerMap()
	responseHandlers := NewResponseHandlers()

	serverNode := DiscoveryNode{ID: "server", Name: "server"}
	transport := NewTcpTransport(serverNode, requestHandlers, responseHandlers, tp)
	addr, err := transport.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Stop()

	profile := ConnectionProfile{
		ConnPerType: map[ConnectionType]int{
			ConnTypeREG: 2,
		},
		ConnectTimeout:   5 * time.Second,
		HandshakeTimeout: 5 * time.Second,
	}

	cm := NewConnectionManager(transport, profile)
	defer cm.Close()

	remoteNode := DiscoveryNode{ID: "server", Name: "server", Address: addr}
	if err := cm.Connect(remoteNode); err != nil {
		t.Fatal(err)
	}

	conn, err := cm.GetConnection(remoteNode.ID)
	if err != nil {
		t.Fatal(err)
	}
	if conn == nil {
		t.Fatal("expected non-nil connection")
	}
}

func TestConnectionManager_GetConnection_NotConnected(t *testing.T) {
	tp := NewThreadPool(map[string]PoolConfig{
		"generic": {Workers: 1, QueueSize: 1},
	})
	defer tp.Shutdown()

	requestHandlers := NewRequestHandlerMap()
	responseHandlers := NewResponseHandlers()
	serverNode := DiscoveryNode{ID: "server", Name: "server"}
	transport := NewTcpTransport(serverNode, requestHandlers, responseHandlers, tp)

	profile := DefaultConnectionProfile()
	cm := NewConnectionManager(transport, profile)
	defer cm.Close()

	_, err := cm.GetConnection("nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
	if _, ok := err.(*NodeNotConnectedError); !ok {
		t.Fatalf("expected NodeNotConnectedError, got %T: %v", err, err)
	}
}

func TestConnectionManager_Disconnect(t *testing.T) {
	tp := NewThreadPool(map[string]PoolConfig{
		"generic": {Workers: 2, QueueSize: 10},
	})
	defer tp.Shutdown()

	requestHandlers := NewRequestHandlerMap()
	responseHandlers := NewResponseHandlers()

	serverNode := DiscoveryNode{ID: "server", Name: "server"}
	transport := NewTcpTransport(serverNode, requestHandlers, responseHandlers, tp)
	addr, err := transport.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Stop()

	profile := ConnectionProfile{
		ConnPerType: map[ConnectionType]int{
			ConnTypeREG: 1,
		},
		ConnectTimeout:   5 * time.Second,
		HandshakeTimeout: 5 * time.Second,
	}

	cm := NewConnectionManager(transport, profile)
	defer cm.Close()

	remoteNode := DiscoveryNode{ID: "server", Name: "server", Address: addr}
	cm.Connect(remoteNode)
	cm.DisconnectFromNode(remoteNode.ID)

	_, err = cm.GetConnection(remoteNode.ID)
	if err == nil {
		t.Fatal("expected error after disconnect")
	}
}

func TestConnectionManager_ConnectedNodes(t *testing.T) {
	tp := NewThreadPool(map[string]PoolConfig{
		"generic": {Workers: 2, QueueSize: 10},
	})
	defer tp.Shutdown()

	requestHandlers := NewRequestHandlerMap()
	responseHandlers := NewResponseHandlers()

	serverNode := DiscoveryNode{ID: "server", Name: "server"}
	transport := NewTcpTransport(serverNode, requestHandlers, responseHandlers, tp)
	addr, err := transport.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Stop()

	profile := ConnectionProfile{
		ConnPerType: map[ConnectionType]int{
			ConnTypeREG: 1,
		},
		ConnectTimeout:   5 * time.Second,
		HandshakeTimeout: 5 * time.Second,
	}

	cm := NewConnectionManager(transport, profile)
	defer cm.Close()

	if len(cm.ConnectedNodes()) != 0 {
		t.Fatal("expected empty")
	}

	remoteNode := DiscoveryNode{ID: "server", Name: "server", Address: addr}
	cm.Connect(remoteNode)

	nodes := cm.ConnectedNodes()
	if len(nodes) != 1 || nodes[0].ID != "server" {
		t.Fatalf("unexpected nodes: %v", nodes)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run TestConnectionManager -timeout 10s`
Expected: compilation error

- [ ] **Step 3: Implement connection_manager.go**

Create `server/transport/connection_manager.go`:

```go
package transport

import "sync"

// ConnectionManager manages connections to remote nodes.
type ConnectionManager struct {
	transport   *TcpTransport
	profile     ConnectionProfile
	connections map[string]*NodeConnection // nodeID → connection
	mu          sync.RWMutex
}

func NewConnectionManager(transport *TcpTransport, profile ConnectionProfile) *ConnectionManager {
	return &ConnectionManager{
		transport:   transport,
		profile:     profile,
		connections: make(map[string]*NodeConnection),
	}
}

func (cm *ConnectionManager) Connect(node DiscoveryNode) error {
	conn, err := cm.transport.OpenConnection(node, cm.profile)
	if err != nil {
		return err
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Close existing connection if any
	if old, ok := cm.connections[node.ID]; ok {
		old.Close()
	}
	cm.connections[node.ID] = conn
	return nil
}

func (cm *ConnectionManager) GetConnection(nodeID string) (*NodeConnection, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	conn, ok := cm.connections[nodeID]
	if !ok {
		return nil, &NodeNotConnectedError{NodeID: nodeID}
	}
	return conn, nil
}

func (cm *ConnectionManager) DisconnectFromNode(nodeID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if conn, ok := cm.connections[nodeID]; ok {
		conn.Close()
		delete(cm.connections, nodeID)
	}
}

func (cm *ConnectionManager) ConnectedNodes() []DiscoveryNode {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	nodes := make([]DiscoveryNode, 0, len(cm.connections))
	for _, conn := range cm.connections {
		nodes = append(nodes, conn.node)
	}
	return nodes
}

func (cm *ConnectionManager) Close() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for id, conn := range cm.connections {
		conn.Close()
		delete(cm.connections, id)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run TestConnectionManager -timeout 10s`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add server/transport/connection_manager.go server/transport/connection_manager_test.go
git commit -m "feat(transport): add ConnectionManager with connect, disconnect, and lookup"
```

---

### Task 13: TransportService

**Files:**
- Create: `server/transport/service.go`
- Create: `server/transport/service_test.go`

- [ ] **Step 1: Write failing tests**

Create `server/transport/service_test.go`:

```go
package transport

import (
	"sync"
	"testing"
	"time"
)

func TestTransportService_LocalRequest(t *testing.T) {
	ts := newTestTransportService(t)
	defer ts.Stop()

	var received string
	var mu sync.Mutex
	done := make(chan struct{})

	ts.RegisterHandler("test:echo", "generic",
		Reader[*testRequest](readTestRequest),
		func(req *testRequest, ch TransportChannel) error {
			mu.Lock()
			received = req.Value
			mu.Unlock()
			close(done)
			return ch.SendResponse(&testResponse{Result: "echo:" + req.Value})
		},
	)

	var respResult string
	var respMu sync.Mutex
	respDone := make(chan struct{})

	ts.SendRequest(
		ts.LocalNode(),
		"test:echo",
		&testRequest{Value: "local"},
		TransportRequestOptions{ConnType: ConnTypeREG},
		&testResponseCallback{
			onResponse: func(resp *testResponse) {
				respMu.Lock()
				respResult = resp.Result
				respMu.Unlock()
				close(respDone)
			},
		},
	)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler timeout")
	}

	select {
	case <-respDone:
	case <-time.After(2 * time.Second):
		t.Fatal("response timeout")
	}

	mu.Lock()
	defer mu.Unlock()
	if received != "local" {
		t.Errorf("received: got %q, want %q", received, "local")
	}
	respMu.Lock()
	defer respMu.Unlock()
	if respResult != "echo:local" {
		t.Errorf("response: got %q, want %q", respResult, "echo:local")
	}
}

func TestTransportService_RemoteRequest(t *testing.T) {
	server := newTestTransportService(t)
	defer server.Stop()

	done := make(chan string, 1)
	server.RegisterHandler("test:echo", "generic",
		Reader[*testRequest](readTestRequest),
		func(req *testRequest, ch TransportChannel) error {
			done <- req.Value
			return ch.SendResponse(&testResponse{Result: "echo:" + req.Value})
		},
	)

	client := newTestTransportService(t)
	defer client.Stop()

	// Connect client to server
	serverNode := DiscoveryNode{
		ID:      server.LocalNode().ID,
		Name:    server.LocalNode().Name,
		Address: server.LocalNode().Address,
	}
	if err := client.ConnectToNode(serverNode); err != nil {
		t.Fatal(err)
	}

	respDone := make(chan string, 1)
	client.SendRequest(
		serverNode,
		"test:echo",
		&testRequest{Value: "remote"},
		TransportRequestOptions{ConnType: ConnTypeREG},
		&testResponseCallback{
			onResponse: func(resp *testResponse) {
				respDone <- resp.Result
			},
		},
	)

	select {
	case v := <-done:
		if v != "remote" {
			t.Errorf("handler received: %q", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler timeout")
	}

	select {
	case v := <-respDone:
		if v != "echo:remote" {
			t.Errorf("response: %q", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("response timeout")
	}
}

// --- test helpers ---

type testResponseCallback struct {
	onResponse func(*testResponse)
	onError    func(*RemoteTransportError)
}

func (c *testResponseCallback) HandleResponse(resp *testResponse)      { c.onResponse(resp) }
func (c *testResponseCallback) HandleError(err *RemoteTransportError)   { if c.onError != nil { c.onError(err) } }
func (c *testResponseCallback) ReadResponse(in *StreamInput) (*testResponse, error) { return readTestResponse(in) }
func (c *testResponseCallback) ExecutorName() string                    { return "generic" }

func newTestTransportService(t *testing.T) *TransportService {
	t.Helper()
	ts, err := NewTransportService(TransportServiceConfig{
		BindAddress: "127.0.0.1:0",
		NodeName:    t.Name(),
		PoolConfigs: map[string]PoolConfig{
			"generic": {Workers: 4, QueueSize: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ts
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run TestTransportService -timeout 15s`
Expected: compilation error

- [ ] **Step 3: Implement service.go**

Create `server/transport/service.go`:

```go
package transport

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// TransportServiceConfig holds configuration for creating a TransportService.
type TransportServiceConfig struct {
	BindAddress string
	NodeName    string
	PoolConfigs map[string]PoolConfig
}

// TransportService is the top-level transport API.
type TransportService struct {
	localNode        DiscoveryNode
	transport        *TcpTransport
	connectionMgr    *ConnectionManager
	requestHandlers  *RequestHandlerMap
	responseHandlers *ResponseHandlers
	threadPool       *ThreadPool
	outbound         *OutboundHandler
}

func NewTransportService(config TransportServiceConfig) (*TransportService, error) {
	nodeID := uuid.New().String()
	localNode := DiscoveryNode{
		ID:   nodeID,
		Name: config.NodeName,
	}

	requestHandlers := NewRequestHandlerMap()
	responseHandlers := NewResponseHandlers()
	threadPool := NewThreadPool(config.PoolConfigs)

	transport := NewTcpTransport(localNode, requestHandlers, responseHandlers, threadPool)

	addr, err := transport.Start(config.BindAddress)
	if err != nil {
		threadPool.Shutdown()
		return nil, err
	}
	localNode.Address = addr

	profile := ConnectionProfile{
		ConnPerType: map[ConnectionType]int{
			ConnTypeREG: 1,
		},
		ConnectTimeout:   5 * time.Second,
		HandshakeTimeout: 5 * time.Second,
	}
	connectionMgr := NewConnectionManager(transport, profile)

	return &TransportService{
		localNode:        localNode,
		transport:        transport,
		connectionMgr:    connectionMgr,
		requestHandlers:  requestHandlers,
		responseHandlers: responseHandlers,
		threadPool:       threadPool,
		outbound:         NewOutboundHandler(),
	}, nil
}

func (ts *TransportService) LocalNode() DiscoveryNode {
	return ts.localNode
}

// RegisterHandler registers a typed request handler for an action.
func (ts *TransportService) RegisterHandler(
	action string,
	executor string,
	reader any,
	handler any,
) {
	// Use the RegisterHandler helper that creates the dispatch closure
	RegisterHandlerFn(ts.requestHandlers, action, executor, reader, handler)
}

// RegisterHandlerFn is the typed version that creates a dispatch closure.
func RegisterHandlerFn[T any](
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

// ResponseHandler is the interface for handling async responses.
type ResponseHandler[T any] interface {
	HandleResponse(resp T)
	HandleError(err *RemoteTransportError)
	ReadResponse(in *StreamInput) (T, error)
	ExecutorName() string
}

// SendRequest sends a request to a node (local or remote).
func (ts *TransportService) SendRequest(
	node DiscoveryNode,
	action string,
	request Writeable,
	options TransportRequestOptions,
	handler any, // ResponseHandler[T]
) error {
	if node.ID == ts.localNode.ID {
		return ts.sendLocalRequest(action, request, handler)
	}
	return ts.sendRemoteRequest(node, action, request, options, handler)
}

func (ts *TransportService) sendLocalRequest(action string, request Writeable, handler any) error {
	entry := ts.requestHandlers.Get(action)
	if entry == nil {
		return fmt.Errorf("no handler for action: %s", action)
	}

	// Serialize and deserialize to match remote behavior
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := request.WriteTo(out); err != nil {
		return err
	}

	payload := NewStreamInput(bytes.NewReader(buf.Bytes()))

	// Create a local channel that delivers response to the handler
	ch := &localTransportChannel{
		handler:    handler,
		threadPool: ts.threadPool,
	}

	executor := ts.threadPool.Get(entry.executor)
	return executor.Execute(func() {
		entry.dispatch(payload, ch)
	})
}

func (ts *TransportService) sendRemoteRequest(
	node DiscoveryNode,
	action string,
	request Writeable,
	options TransportRequestOptions,
	handler any,
) error {
	nodeConn, err := ts.connectionMgr.GetConnection(node.ID)
	if err != nil {
		return err
	}

	ctx := &ResponseContext{
		Handler:   handler,
		Action:    action,
		NodeID:    node.ID,
		Timeout:   options.Timeout,
		CreatedAt: time.Now(),
	}
	requestID := ts.responseHandlers.Add(ctx)

	conn, err := nodeConn.Conn(options.ConnType)
	if err != nil {
		ts.responseHandlers.Remove(requestID)
		return err
	}

	if err := ts.outbound.SendRequest(conn, requestID, action, request); err != nil {
		ts.responseHandlers.Remove(requestID)
		return &SendRequestError{Action: action, Cause: err}
	}

	return nil
}

func (ts *TransportService) ConnectToNode(node DiscoveryNode) error {
	return ts.connectionMgr.Connect(node)
}

func (ts *TransportService) DisconnectFromNode(nodeID string) {
	ts.connectionMgr.DisconnectFromNode(nodeID)
}

func (ts *TransportService) Stop() error {
	ts.connectionMgr.Close()
	err := ts.transport.Stop()
	ts.threadPool.Shutdown()
	return err
}

// localTransportChannel delivers responses directly to the handler in-process.
type localTransportChannel struct {
	handler    any
	threadPool *ThreadPool
	mu         sync.Mutex
	responded  bool
}

func (c *localTransportChannel) SendResponse(response Writeable) error {
	c.mu.Lock()
	if c.responded {
		c.mu.Unlock()
		return nil
	}
	c.responded = true
	c.mu.Unlock()

	// Serialize and deserialize to match remote behavior
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := response.WriteTo(out); err != nil {
		return err
	}

	payload := NewStreamInput(bytes.NewReader(buf.Bytes()))

	// Call handler's ReadResponse and HandleResponse via reflection-free interface check
	if h, ok := c.handler.(interface {
		ReadResponse(*StreamInput) (any, error)
		HandleResponse(any)
		ExecutorName() string
	}); ok {
		resp, err := h.ReadResponse(payload)
		if err != nil {
			return err
		}
		executor := c.threadPool.Get(h.ExecutorName())
		executor.Execute(func() { h.HandleResponse(resp) })
		return nil
	}

	// Try concrete type callback for test helpers
	return deliverLocalResponse(c.handler, payload, c.threadPool)
}

func (c *localTransportChannel) SendError(err error) error {
	c.mu.Lock()
	if c.responded {
		c.mu.Unlock()
		return nil
	}
	c.responded = true
	c.mu.Unlock()

	transportErr := &RemoteTransportError{Message: err.Error()}
	if h, ok := c.handler.(interface{ HandleError(*RemoteTransportError) }); ok {
		h.HandleError(transportErr)
	}
	return nil
}

// deliverLocalResponse handles the type-erased local response delivery.
func deliverLocalResponse(handler any, payload *StreamInput, tp *ThreadPool) error {
	// This is called when the handler doesn't implement the generic interface.
	// The test callback types implement specific methods.
	type responseReader interface {
		ReadResponse(*StreamInput) (any, error)
	}
	type responseReceiver interface {
		HandleResponse(any)
	}

	// For the concrete test type, use direct interface assertions
	// In production, handlers will implement ResponseHandler[T]
	return fmt.Errorf("unsupported handler type: %T", handler)
}
```

This approach with type-erased handlers is getting complex. Let me simplify — since Go generics can't be stored in maps easily, let's use a concrete dispatch pattern where the `ResponseHandler` stores its own read+deliver closure, similar to how `requestHandlerEntry` works.

Add to `service.go`:

```go
// TypedResponseHandler wraps a ResponseHandler[T] into a type-erased callback.
func TypedResponseHandler[T any](
	reader Reader[T],
	executor string,
	onResponse func(T),
	onError func(*RemoteTransportError),
) *responseHandlerWrapper {
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

type responseHandlerWrapper struct {
	executorName  string
	readAndHandle func(in *StreamInput) error
	onError       func(*RemoteTransportError)
}
```

Update `sendLocalRequest` to use `localTransportChannel` that works with `responseHandlerWrapper`:

The `localTransportChannel.SendResponse` should:
1. Serialize the response
2. Call `handler.readAndHandle(payload)`

Update the `InboundHandler.handleResponse` to also use `responseHandlerWrapper`:

```go
func (ih *InboundHandler) handleResponse(header *Header, payload *StreamInput) error {
    ctx := ih.responseHandlers.Remove(header.RequestID)
    if ctx == nil {
        return nil
    }

    wrapper, ok := ctx.Handler.(*responseHandlerWrapper)
    if !ok {
        return fmt.Errorf("unexpected handler type: %T", ctx.Handler)
    }

    if header.Status.IsError() {
        transportErr, err := ReadRemoteTransportError(payload)
        if err != nil {
            return err
        }
        if wrapper.onError != nil {
            executor := ih.threadPool.Get(wrapper.executorName)
            executor.Execute(func() { wrapper.onError(transportErr) })
        }
        return nil
    }

    executor := ih.threadPool.Get(wrapper.executorName)
    return executor.Execute(func() {
        wrapper.readAndHandle(payload)
    })
}
```

Then update `SendRequest` to accept `*responseHandlerWrapper` instead of `any`:

```go
func (ts *TransportService) SendRequest(
    node DiscoveryNode,
    action string,
    request Writeable,
    options TransportRequestOptions,
    handler *responseHandlerWrapper,
) error
```

And update the test's `testResponseCallback` to use `TypedResponseHandler`:

```go
// In the test, replace testResponseCallback with:
handler := TypedResponseHandler(
    Reader[*testResponse](readTestResponse),
    "generic",
    func(resp *testResponse) {
        respMu.Lock()
        respResult = resp.Result
        respMu.Unlock()
        close(respDone)
    },
    nil,
)
```

This is cleaner. The final `service.go` should use `*responseHandlerWrapper` throughout.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run TestTransportService -timeout 15s`
Expected: all PASS

- [ ] **Step 5: Run full test suite**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -timeout 30s`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add server/transport/service.go server/transport/service_test.go
git commit -m "feat(transport): add TransportService with local and remote request dispatch"
```

---

### Task 14: Representative Writeable — IndexDocument

**Files:**
- Create: `server/transport/action_writeable.go`
- Create: `server/transport/action_writeable_test.go`

- [ ] **Step 1: Write failing tests**

Create `server/transport/action_writeable_test.go`:

```go
package transport

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestIndexDocumentRequest_Roundtrip(t *testing.T) {
	seqNo := int64(5)
	primTerm := int64(1)
	req := &IndexDocumentRequest{
		Index:         "products",
		ID:            "doc-1",
		Source:        json.RawMessage(`{"title":"hello"}`),
		IfSeqNo:       &seqNo,
		IfPrimaryTerm: &primTerm,
	}

	var buf bytes.Buffer
	if err := req.WriteTo(NewStreamOutput(&buf)); err != nil {
		t.Fatal(err)
	}

	got, err := ReadIndexDocumentRequest(NewStreamInput(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}

	if got.Index != "products" {
		t.Errorf("Index: got %q", got.Index)
	}
	if got.ID != "doc-1" {
		t.Errorf("ID: got %q", got.ID)
	}
	if !bytes.Equal(got.Source, req.Source) {
		t.Errorf("Source: got %q", got.Source)
	}
	if got.IfSeqNo == nil || *got.IfSeqNo != 5 {
		t.Errorf("IfSeqNo: got %v", got.IfSeqNo)
	}
	if got.IfPrimaryTerm == nil || *got.IfPrimaryTerm != 1 {
		t.Errorf("IfPrimaryTerm: got %v", got.IfPrimaryTerm)
	}
}

func TestIndexDocumentRequest_Roundtrip_NilOptionals(t *testing.T) {
	req := &IndexDocumentRequest{
		Index:  "products",
		ID:     "doc-2",
		Source: json.RawMessage(`{}`),
	}

	var buf bytes.Buffer
	req.WriteTo(NewStreamOutput(&buf))

	got, err := ReadIndexDocumentRequest(NewStreamInput(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if got.IfSeqNo != nil {
		t.Errorf("IfSeqNo should be nil, got %v", got.IfSeqNo)
	}
	if got.IfPrimaryTerm != nil {
		t.Errorf("IfPrimaryTerm should be nil, got %v", got.IfPrimaryTerm)
	}
}

func TestIndexDocumentResponse_Roundtrip(t *testing.T) {
	resp := &IndexDocumentResponse{
		Index:       "products",
		ID:          "doc-1",
		SeqNo:       10,
		PrimaryTerm: 1,
		Result:      "created",
	}

	var buf bytes.Buffer
	resp.WriteTo(NewStreamOutput(&buf))

	got, err := ReadIndexDocumentResponse(NewStreamInput(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}

	if got.Index != "products" || got.ID != "doc-1" {
		t.Errorf("identity: %+v", got)
	}
	if got.SeqNo != 10 || got.PrimaryTerm != 1 {
		t.Errorf("version: seqNo=%d, primTerm=%d", got.SeqNo, got.PrimaryTerm)
	}
	if got.Result != "created" {
		t.Errorf("Result: %q", got.Result)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run TestIndexDocument`
Expected: compilation error

- [ ] **Step 3: Implement IndexDocument Writeable**

Create `server/transport/action_writeable.go`:

```go
package transport

import "encoding/json"

// --- IndexDocument ---

type IndexDocumentRequest struct {
	Index         string
	ID            string
	Source        json.RawMessage
	IfSeqNo       *int64
	IfPrimaryTerm *int64
}

func (r *IndexDocumentRequest) WriteTo(out *StreamOutput) error {
	if err := out.WriteString(r.Index); err != nil {
		return err
	}
	if err := out.WriteString(r.ID); err != nil {
		return err
	}
	if err := out.WriteByteArray(r.Source); err != nil {
		return err
	}
	if err := out.WriteOptionalInt64(r.IfSeqNo); err != nil {
		return err
	}
	return out.WriteOptionalInt64(r.IfPrimaryTerm)
}

func ReadIndexDocumentRequest(in *StreamInput) (*IndexDocumentRequest, error) {
	index, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	id, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	source, err := in.ReadByteArray()
	if err != nil {
		return nil, err
	}
	ifSeqNo, err := in.ReadOptionalInt64()
	if err != nil {
		return nil, err
	}
	ifPrimaryTerm, err := in.ReadOptionalInt64()
	if err != nil {
		return nil, err
	}
	return &IndexDocumentRequest{
		Index:         index,
		ID:            id,
		Source:        json.RawMessage(source),
		IfSeqNo:       ifSeqNo,
		IfPrimaryTerm: ifPrimaryTerm,
	}, nil
}

type IndexDocumentResponse struct {
	Index       string
	ID          string
	SeqNo       int64
	PrimaryTerm int64
	Result      string
}

func (r *IndexDocumentResponse) WriteTo(out *StreamOutput) error {
	if err := out.WriteString(r.Index); err != nil {
		return err
	}
	if err := out.WriteString(r.ID); err != nil {
		return err
	}
	if err := out.WriteVLong(r.SeqNo); err != nil {
		return err
	}
	if err := out.WriteVLong(r.PrimaryTerm); err != nil {
		return err
	}
	return out.WriteString(r.Result)
}

func ReadIndexDocumentResponse(in *StreamInput) (*IndexDocumentResponse, error) {
	index, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	id, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	seqNo, err := in.ReadVLong()
	if err != nil {
		return nil, err
	}
	primaryTerm, err := in.ReadVLong()
	if err != nil {
		return nil, err
	}
	result, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	return &IndexDocumentResponse{
		Index:       index,
		ID:          id,
		SeqNo:       seqNo,
		PrimaryTerm: primaryTerm,
		Result:      result,
	}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run TestIndexDocument`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add server/transport/action_writeable.go server/transport/action_writeable_test.go
git commit -m "feat(transport): add Writeable for IndexDocument request/response"
```

---

### Task 15: Representative Writeable — GetDocument and Search

**Files:**
- Modify: `server/transport/action_writeable.go`
- Modify: `server/transport/action_writeable_test.go`

- [ ] **Step 1: Write failing tests for GetDocument and Search**

Append to `server/transport/action_writeable_test.go`:

```go
func TestGetDocumentRequest_Roundtrip(t *testing.T) {
	req := &GetDocumentRequest{Index: "products", ID: "doc-1"}
	var buf bytes.Buffer
	req.WriteTo(NewStreamOutput(&buf))

	got, err := ReadGetDocumentRequest(NewStreamInput(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if got.Index != "products" || got.ID != "doc-1" {
		t.Errorf("got %+v", got)
	}
}

func TestGetDocumentResponse_Roundtrip_Found(t *testing.T) {
	resp := &GetDocumentResponse{
		Index:       "products",
		ID:          "doc-1",
		SeqNo:       3,
		PrimaryTerm: 1,
		Found:       true,
		Source:      json.RawMessage(`{"title":"test"}`),
	}
	var buf bytes.Buffer
	resp.WriteTo(NewStreamOutput(&buf))

	got, err := ReadGetDocumentResponse(NewStreamInput(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Found {
		t.Error("expected Found=true")
	}
	if !bytes.Equal(got.Source, resp.Source) {
		t.Errorf("Source: got %q", got.Source)
	}
}

func TestGetDocumentResponse_Roundtrip_NotFound(t *testing.T) {
	resp := &GetDocumentResponse{
		Index: "products",
		ID:    "missing",
		Found: false,
	}
	var buf bytes.Buffer
	resp.WriteTo(NewStreamOutput(&buf))

	got, err := ReadGetDocumentResponse(NewStreamInput(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if got.Found {
		t.Error("expected Found=false")
	}
	if got.Source != nil {
		t.Errorf("Source should be nil, got %q", got.Source)
	}
}

func TestSearchRequest_Roundtrip(t *testing.T) {
	req := &SearchRequestMsg{
		Index: "products",
		QueryJSON: map[string]any{
			"match": map[string]any{"title": "hello"},
		},
		AggsJSON: map[string]any{
			"by_status": map[string]any{
				"terms": map[string]any{"field": "status"},
			},
		},
		Size: 20,
	}
	var buf bytes.Buffer
	req.WriteTo(NewStreamOutput(&buf))

	got, err := ReadSearchRequestMsg(NewStreamInput(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if got.Index != "products" {
		t.Errorf("Index: got %q", got.Index)
	}
	if got.Size != 20 {
		t.Errorf("Size: got %d", got.Size)
	}
	match := got.QueryJSON["match"].(map[string]any)
	if match["title"] != "hello" {
		t.Errorf("QueryJSON: got %v", got.QueryJSON)
	}
}

func TestSearchResponse_Roundtrip(t *testing.T) {
	resp := &SearchResponseMsg{
		Took: 15,
		TotalHits:    100,
		TotalRelation: "eq",
		MaxScore:     2.5,
		Hits: []SearchHitMsg{
			{Index: "products", ID: "1", Score: 2.5, Source: json.RawMessage(`{"title":"a"}`)},
			{Index: "products", ID: "2", Score: 1.2, Source: json.RawMessage(`{"title":"b"}`)},
		},
		Aggregations: map[string]any{
			"by_status": map[string]any{"buckets": []any{}},
		},
	}
	var buf bytes.Buffer
	resp.WriteTo(NewStreamOutput(&buf))

	got, err := ReadSearchResponseMsg(NewStreamInput(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if got.Took != 15 {
		t.Errorf("Took: got %d", got.Took)
	}
	if got.TotalHits != 100 {
		t.Errorf("TotalHits: got %d", got.TotalHits)
	}
	if len(got.Hits) != 2 {
		t.Fatalf("Hits: got %d", len(got.Hits))
	}
	if got.Hits[0].ID != "1" || got.Hits[1].ID != "2" {
		t.Errorf("Hits: %+v", got.Hits)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run "TestGetDocument|TestSearch"`
Expected: compilation error

- [ ] **Step 3: Implement GetDocument and Search Writeable**

Append to `server/transport/action_writeable.go`:

```go
// --- GetDocument ---

type GetDocumentRequest struct {
	Index string
	ID    string
}

func (r *GetDocumentRequest) WriteTo(out *StreamOutput) error {
	if err := out.WriteString(r.Index); err != nil {
		return err
	}
	return out.WriteString(r.ID)
}

func ReadGetDocumentRequest(in *StreamInput) (*GetDocumentRequest, error) {
	index, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	id, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	return &GetDocumentRequest{Index: index, ID: id}, nil
}

type GetDocumentResponse struct {
	Index       string
	ID          string
	SeqNo       int64
	PrimaryTerm int64
	Found       bool
	Source      json.RawMessage // nil if not found
}

func (r *GetDocumentResponse) WriteTo(out *StreamOutput) error {
	if err := out.WriteString(r.Index); err != nil {
		return err
	}
	if err := out.WriteString(r.ID); err != nil {
		return err
	}
	if err := out.WriteVLong(r.SeqNo); err != nil {
		return err
	}
	if err := out.WriteVLong(r.PrimaryTerm); err != nil {
		return err
	}
	if err := out.WriteBool(r.Found); err != nil {
		return err
	}
	if r.Found {
		return out.WriteByteArray(r.Source)
	}
	return nil
}

func ReadGetDocumentResponse(in *StreamInput) (*GetDocumentResponse, error) {
	index, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	id, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	seqNo, err := in.ReadVLong()
	if err != nil {
		return nil, err
	}
	primaryTerm, err := in.ReadVLong()
	if err != nil {
		return nil, err
	}
	found, err := in.ReadBool()
	if err != nil {
		return nil, err
	}
	var source json.RawMessage
	if found {
		src, err := in.ReadByteArray()
		if err != nil {
			return nil, err
		}
		source = json.RawMessage(src)
	}
	return &GetDocumentResponse{
		Index:       index,
		ID:          id,
		SeqNo:       seqNo,
		PrimaryTerm: primaryTerm,
		Found:       found,
		Source:      source,
	}, nil
}

// --- Search ---
// Uses "Msg" suffix to avoid collision with server/action types.

type SearchRequestMsg struct {
	Index     string
	QueryJSON map[string]any
	AggsJSON  map[string]any
	Size      int
}

func (r *SearchRequestMsg) WriteTo(out *StreamOutput) error {
	if err := out.WriteString(r.Index); err != nil {
		return err
	}
	if err := out.WriteGenericMap(r.QueryJSON); err != nil {
		return err
	}
	if err := out.WriteGenericMap(r.AggsJSON); err != nil {
		return err
	}
	return out.WriteVInt(int32(r.Size))
}

func ReadSearchRequestMsg(in *StreamInput) (*SearchRequestMsg, error) {
	index, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	queryJSON, err := in.ReadGenericMap()
	if err != nil {
		return nil, err
	}
	aggsJSON, err := in.ReadGenericMap()
	if err != nil {
		return nil, err
	}
	size, err := in.ReadVInt()
	if err != nil {
		return nil, err
	}
	return &SearchRequestMsg{
		Index:     index,
		QueryJSON: queryJSON,
		AggsJSON:  aggsJSON,
		Size:      int(size),
	}, nil
}

type SearchHitMsg struct {
	Index  string
	ID     string
	Score  float64
	Source json.RawMessage
}

type SearchResponseMsg struct {
	Took          int64
	TotalHits     int
	TotalRelation string
	MaxScore      float64
	Hits          []SearchHitMsg
	Aggregations  map[string]any
}

func (r *SearchResponseMsg) WriteTo(out *StreamOutput) error {
	if err := out.WriteVLong(r.Took); err != nil {
		return err
	}
	if err := out.WriteVInt(int32(r.TotalHits)); err != nil {
		return err
	}
	if err := out.WriteString(r.TotalRelation); err != nil {
		return err
	}
	if err := out.WriteFloat64(r.MaxScore); err != nil {
		return err
	}
	if err := out.WriteVInt(int32(len(r.Hits))); err != nil {
		return err
	}
	for _, hit := range r.Hits {
		if err := out.WriteString(hit.Index); err != nil {
			return err
		}
		if err := out.WriteString(hit.ID); err != nil {
			return err
		}
		if err := out.WriteFloat64(hit.Score); err != nil {
			return err
		}
		if err := out.WriteByteArray(hit.Source); err != nil {
			return err
		}
	}
	return out.WriteGenericMap(r.Aggregations)
}

func ReadSearchResponseMsg(in *StreamInput) (*SearchResponseMsg, error) {
	took, err := in.ReadVLong()
	if err != nil {
		return nil, err
	}
	totalHits, err := in.ReadVInt()
	if err != nil {
		return nil, err
	}
	totalRelation, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	maxScore, err := in.ReadFloat64()
	if err != nil {
		return nil, err
	}
	hitCount, err := in.ReadVInt()
	if err != nil {
		return nil, err
	}
	hits := make([]SearchHitMsg, hitCount)
	for i := range hits {
		index, err := in.ReadString()
		if err != nil {
			return nil, err
		}
		id, err := in.ReadString()
		if err != nil {
			return nil, err
		}
		score, err := in.ReadFloat64()
		if err != nil {
			return nil, err
		}
		source, err := in.ReadByteArray()
		if err != nil {
			return nil, err
		}
		hits[i] = SearchHitMsg{Index: index, ID: id, Score: score, Source: json.RawMessage(source)}
	}
	aggs, err := in.ReadGenericMap()
	if err != nil {
		return nil, err
	}
	return &SearchResponseMsg{
		Took:          took,
		TotalHits:     int(totalHits),
		TotalRelation: totalRelation,
		MaxScore:      maxScore,
		Hits:          hits,
		Aggregations:  aggs,
	}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run "TestGetDocument|TestSearch"`
Expected: all PASS

- [ ] **Step 5: Run full test suite**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add server/transport/action_writeable.go server/transport/action_writeable_test.go
git commit -m "feat(transport): add Writeable for GetDocument and Search request/response"
```

---

### Task 16: Integration — Wire into Node

**Files:**
- Modify: `server/node/node.go`
- Modify: `server/node/node_test.go`

- [ ] **Step 1: Write a test that verifies TransportService starts with Node**

Add to `server/node/node_test.go`:

```go
func TestNode_TransportServiceStarts(t *testing.T) {
	dir := t.TempDir()
	node, err := NewNode(NodeConfig{
		DataPath:      dir,
		HTTPPort:      0,
		TransportPort: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()

	_, err = node.Start()
	if err != nil {
		t.Fatal(err)
	}

	if node.TransportService() == nil {
		t.Error("TransportService should not be nil")
	}
	if node.TransportService().LocalNode().Address == "" {
		t.Error("transport address should be set")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/node/ -v -run TestNode_TransportService`
Expected: compilation error — `TransportPort` field, `TransportService()` method don't exist

- [ ] **Step 3: Add TransportService to Node**

Update `server/node/node.go`:

Add to imports: `"gosearch/server/transport"` and `"runtime"`

Add `TransportPort` to `NodeConfig`:
```go
type NodeConfig struct {
	DataPath      string
	HTTPPort      int
	TransportPort int
}
```

Add `transportService` field to `Node`:
```go
type Node struct {
	config           NodeConfig
	clusterState     *cluster.ClusterState
	indexServices    map[string]*index.IndexService
	router           chi.Router
	registry         *analysis.AnalyzerRegistry
	httpServer       *http.Server
	listener         net.Listener
	transportService *transport.TransportService
	stopped          bool
}
```

In `NewNode`, after creating the handler and before returning, add:

```go
	numCPU := runtime.NumCPU()
	ts, err := transport.NewTransportService(transport.TransportServiceConfig{
		BindAddress: fmt.Sprintf(":%d", config.TransportPort),
		NodeName:    fmt.Sprintf("gosearch-%d", config.HTTPPort),
		PoolConfigs: map[string]transport.PoolConfig{
			"generic":          {Workers: numCPU * 4, QueueSize: 1000},
			"search":           {Workers: numCPU + 1, QueueSize: 1000},
			"index":            {Workers: numCPU, QueueSize: 200},
			"transport_worker": {Workers: 0},
			"cluster_state":    {Workers: 1, QueueSize: 10},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create transport service: %w", err)
	}
	n.transportService = ts
```

Add accessor:
```go
func (n *Node) TransportService() *transport.TransportService {
	return n.transportService
}
```

In `Stop()`, add before closing index services:
```go
	if n.transportService != nil {
		n.transportService.Stop()
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/node/ -v -run TestNode_TransportService -timeout 10s`
Expected: PASS

- [ ] **Step 5: Run full project test suite**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./... -timeout 60s`
Expected: all PASS (existing tests unaffected)

- [ ] **Step 6: Commit**

```bash
git add server/node/node.go server/node/node_test.go
git commit -m "feat(transport): wire TransportService into Node with default thread pools"
```

---

### Task 17: End-to-End Integration Test

**Files:**
- Create: `server/transport/integration_test.go`

- [ ] **Step 1: Write an end-to-end test**

Create `server/transport/integration_test.go`:

```go
package transport

import (
	"bytes"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestIntegration_IndexDocumentOverTransport(t *testing.T) {
	// Start server
	server, err := NewTransportService(TransportServiceConfig{
		BindAddress: "127.0.0.1:0",
		NodeName:    "server",
		PoolConfigs: map[string]PoolConfig{
			"generic": {Workers: 4, QueueSize: 100},
			"index":   {Workers: 2, QueueSize: 50},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	// Register IndexDocument handler on server
	RegisterHandlerFn(server.requestHandlers, "indices:data/write/index", "index",
		Reader[*IndexDocumentRequest](ReadIndexDocumentRequest),
		func(req *IndexDocumentRequest, ch TransportChannel) error {
			resp := &IndexDocumentResponse{
				Index:       req.Index,
				ID:          req.ID,
				SeqNo:       1,
				PrimaryTerm: 1,
				Result:      "created",
			}
			return ch.SendResponse(resp)
		},
	)

	// Start client
	client, err := NewTransportService(TransportServiceConfig{
		BindAddress: "127.0.0.1:0",
		NodeName:    "client",
		PoolConfigs: map[string]PoolConfig{
			"generic": {Workers: 4, QueueSize: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()

	// Connect
	serverNode := server.LocalNode()
	if err := client.ConnectToNode(serverNode); err != nil {
		t.Fatal(err)
	}

	// Send IndexDocument request
	var mu sync.Mutex
	var result *IndexDocumentResponse
	done := make(chan struct{})

	req := &IndexDocumentRequest{
		Index:  "products",
		ID:     "doc-1",
		Source: json.RawMessage(`{"title":"widget"}`),
	}

	handler := TypedResponseHandler(
		Reader[*IndexDocumentResponse](ReadIndexDocumentResponse),
		"generic",
		func(resp *IndexDocumentResponse) {
			mu.Lock()
			result = resp
			mu.Unlock()
			close(done)
		},
		func(err *RemoteTransportError) {
			t.Errorf("unexpected error: %v", err)
			close(done)
		},
	)

	if err := client.SendRequest(serverNode, "indices:data/write/index", req,
		TransportRequestOptions{ConnType: ConnTypeREG}, handler); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	mu.Lock()
	defer mu.Unlock()
	if result == nil {
		t.Fatal("no response received")
	}
	if result.Index != "products" {
		t.Errorf("Index: got %q", result.Index)
	}
	if result.ID != "doc-1" {
		t.Errorf("ID: got %q", result.ID)
	}
	if result.Result != "created" {
		t.Errorf("Result: got %q", result.Result)
	}
}

func TestIntegration_SearchOverTransport(t *testing.T) {
	server, err := NewTransportService(TransportServiceConfig{
		BindAddress: "127.0.0.1:0",
		NodeName:    "server",
		PoolConfigs: map[string]PoolConfig{
			"generic": {Workers: 4, QueueSize: 100},
			"search":  {Workers: 2, QueueSize: 50},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	RegisterHandlerFn(server.requestHandlers, "indices:data/read/search", "search",
		Reader[*SearchRequestMsg](ReadSearchRequestMsg),
		func(req *SearchRequestMsg, ch TransportChannel) error {
			resp := &SearchResponseMsg{
				Took:          5,
				TotalHits:     1,
				TotalRelation: "eq",
				MaxScore:      1.0,
				Hits: []SearchHitMsg{
					{Index: req.Index, ID: "1", Score: 1.0, Source: json.RawMessage(`{"title":"result"}`)},
				},
			}
			return ch.SendResponse(resp)
		},
	)

	client, err := NewTransportService(TransportServiceConfig{
		BindAddress: "127.0.0.1:0",
		NodeName:    "client",
		PoolConfigs: map[string]PoolConfig{
			"generic": {Workers: 4, QueueSize: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()

	serverNode := server.LocalNode()
	client.ConnectToNode(serverNode)

	var result *SearchResponseMsg
	done := make(chan struct{})

	req := &SearchRequestMsg{
		Index:     "products",
		QueryJSON: map[string]any{"match_all": map[string]any{}},
		Size:      10,
	}

	handler := TypedResponseHandler(
		Reader[*SearchResponseMsg](ReadSearchResponseMsg),
		"generic",
		func(resp *SearchResponseMsg) {
			result = resp
			close(done)
		},
		nil,
	)

	client.SendRequest(serverNode, "indices:data/read/search", req,
		TransportRequestOptions{ConnType: ConnTypeREG}, handler)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	if result.TotalHits != 1 {
		t.Errorf("TotalHits: got %d", result.TotalHits)
	}
	if len(result.Hits) != 1 || result.Hits[0].ID != "1" {
		t.Errorf("Hits: %+v", result.Hits)
	}
	if !bytes.Equal(result.Hits[0].Source, json.RawMessage(`{"title":"result"}`)) {
		t.Errorf("Source: got %q", result.Hits[0].Source)
	}
}
```

- [ ] **Step 2: Run integration tests**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/transport/ -v -run TestIntegration -timeout 30s`
Expected: all PASS

- [ ] **Step 3: Run full project test suite**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./... -timeout 60s`
Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git add server/transport/integration_test.go
git commit -m "test(transport): add end-to-end integration tests for IndexDocument and Search"
```
