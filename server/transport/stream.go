package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// StreamOutput wraps an io.Writer and provides methods for writing
// primitive types in a binary format compatible with Elasticsearch's
// StreamOutput serialization.
type StreamOutput struct {
	w io.Writer
}

// NewStreamOutput creates a new StreamOutput wrapping the given writer.
func NewStreamOutput(w io.Writer) *StreamOutput {
	return &StreamOutput{w: w}
}

// WriteByte writes a single byte.
func (s *StreamOutput) WriteByte(b byte) error {
	_, err := s.w.Write([]byte{b})
	return err
}

// WriteBytes writes a raw byte slice without any length prefix.
func (s *StreamOutput) WriteBytes(b []byte) error {
	_, err := s.w.Write(b)
	return err
}

// WriteVInt writes an int32 as an unsigned varint (MSB continuation bit).
// The value is treated as uint32 for encoding purposes.
func (s *StreamOutput) WriteVInt(v int32) error {
	return s.writeUvarint(uint64(uint32(v)))
}

// WriteVLong writes an int64 as an unsigned varint (MSB continuation bit).
// The value is treated as uint64 for encoding purposes.
func (s *StreamOutput) WriteVLong(v int64) error {
	return s.writeUvarint(uint64(v))
}

func (s *StreamOutput) writeUvarint(v uint64) error {
	for v >= 0x80 {
		if err := s.WriteByte(byte(v&0x7F) | 0x80); err != nil {
			return err
		}
		v >>= 7
	}
	return s.WriteByte(byte(v))
}

// WriteInt32 writes an int32 in big-endian fixed format.
func (s *StreamOutput) WriteInt32(v int32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(v))
	_, err := s.w.Write(buf[:])
	return err
}

// WriteInt64 writes an int64 in big-endian fixed format.
func (s *StreamOutput) WriteInt64(v int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	_, err := s.w.Write(buf[:])
	return err
}

// WriteFloat64 writes a float64 by converting it to uint64 bits and writing as int64.
func (s *StreamOutput) WriteFloat64(v float64) error {
	return s.WriteInt64(int64(math.Float64bits(v)))
}

// WriteBool writes a boolean as a single byte (1=true, 0=false).
func (s *StreamOutput) WriteBool(v bool) error {
	if v {
		return s.WriteByte(1)
	}
	return s.WriteByte(0)
}

// WriteString writes a string as a VInt length prefix followed by UTF-8 bytes.
func (s *StreamOutput) WriteString(v string) error {
	b := []byte(v)
	if err := s.WriteVInt(int32(len(b))); err != nil {
		return err
	}
	return s.WriteBytes(b)
}

// WriteByteArray writes a byte slice as a VInt length prefix followed by raw bytes.
func (s *StreamOutput) WriteByteArray(b []byte) error {
	if err := s.WriteVInt(int32(len(b))); err != nil {
		return err
	}
	return s.WriteBytes(b)
}

// WriteOptionalInt64 writes an optional int64 with a 1-byte presence flag.
// If v is nil, writes false. Otherwise writes true followed by the VLong value.
func (s *StreamOutput) WriteOptionalInt64(v *int64) error {
	if v == nil {
		return s.WriteBool(false)
	}
	if err := s.WriteBool(true); err != nil {
		return err
	}
	return s.WriteVLong(*v)
}

// GenericMap type tags.
const (
	tagNil     byte = 0
	tagString  byte = 1
	tagInt64   byte = 2
	tagFloat64 byte = 3
	tagBool    byte = 4
	tagMap     byte = 5
	tagSlice   byte = 6
)

// WriteGenericMap writes a map[string]any with type-tagged values.
// Nil maps are encoded with presence=false.
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
	if v == nil {
		return s.WriteByte(tagNil)
	}
	switch val := v.(type) {
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
		for _, elem := range val {
			if err := s.writeGenericValue(elem); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported generic value type: %T", v)
	}
}

// StreamInput wraps an io.Reader and provides methods for reading
// primitive types serialized by StreamOutput.
type StreamInput struct {
	r io.Reader
}

// NewStreamInput creates a new StreamInput wrapping the given reader.
func NewStreamInput(r io.Reader) *StreamInput {
	return &StreamInput{r: r}
}

// ReadByte reads a single byte.
func (s *StreamInput) ReadByte() (byte, error) {
	var buf [1]byte
	_, err := io.ReadFull(s.r, buf[:])
	return buf[0], err
}

// ReadBytes reads exactly n bytes.
func (s *StreamInput) ReadBytes(n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(s.r, buf)
	return buf, err
}

// ReadVInt reads an unsigned varint and returns it as int32.
func (s *StreamInput) ReadVInt() (int32, error) {
	v, err := s.readUvarint()
	if err != nil {
		return 0, err
	}
	return int32(uint32(v)), nil
}

// ReadVLong reads an unsigned varint and returns it as int64.
func (s *StreamInput) ReadVLong() (int64, error) {
	v, err := s.readUvarint()
	if err != nil {
		return 0, err
	}
	return int64(v), nil
}

func (s *StreamInput) readUvarint() (uint64, error) {
	var result uint64
	var shift uint
	for {
		b, err := s.ReadByte()
		if err != nil {
			return 0, err
		}
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("varint overflow")
		}
	}
	return result, nil
}

// ReadInt32 reads a big-endian fixed int32.
func (s *StreamInput) ReadInt32() (int32, error) {
	buf, err := s.ReadBytes(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(buf)), nil
}

// ReadInt64 reads a big-endian fixed int64.
func (s *StreamInput) ReadInt64() (int64, error) {
	buf, err := s.ReadBytes(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(buf)), nil
}

// ReadFloat64 reads a float64 (stored as int64 bits).
func (s *StreamInput) ReadFloat64() (float64, error) {
	v, err := s.ReadInt64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(uint64(v)), nil
}

// ReadBool reads a boolean (1 byte: 1=true, 0=false).
func (s *StreamInput) ReadBool() (bool, error) {
	b, err := s.ReadByte()
	if err != nil {
		return false, err
	}
	return b != 0, nil
}

// ReadString reads a VInt length-prefixed UTF-8 string.
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

// ReadByteArray reads a VInt length-prefixed byte array.
func (s *StreamInput) ReadByteArray() ([]byte, error) {
	length, err := s.ReadVInt()
	if err != nil {
		return nil, err
	}
	return s.ReadBytes(int(length))
}

// ReadOptionalInt64 reads an optional int64 with a presence flag.
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

// ReadGenericMap reads a type-tagged map[string]any.
func (s *StreamInput) ReadGenericMap() (map[string]any, error) {
	present, err := s.ReadBool()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}
	count, err := s.ReadVInt()
	if err != nil {
		return nil, err
	}
	m := make(map[string]any, count)
	for range count {
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
		for i := range length {
			v, err := s.readGenericValue()
			if err != nil {
				return nil, err
			}
			slice[i] = v
		}
		return slice, nil
	default:
		return nil, fmt.Errorf("unknown generic value tag: %d", tag)
	}
}
