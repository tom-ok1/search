package transport

import (
	"bytes"
	"math"
	"testing"
)

func TestWriteReadByte(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := out.WriteByte(0xAB); err != nil {
		t.Fatal(err)
	}
	in := NewStreamInput(&buf)
	got, err := in.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	if got != 0xAB {
		t.Fatalf("expected 0xAB, got 0x%02X", got)
	}
}

func TestWriteReadBytes(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	data := []byte{1, 2, 3, 4, 5}
	if err := out.WriteBytes(data); err != nil {
		t.Fatal(err)
	}
	in := NewStreamInput(&buf)
	got, err := in.ReadBytes(5)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("expected %v, got %v", data, got)
	}
}

func TestWriteVIntEncoding(t *testing.T) {
	tests := []struct {
		name  string
		value int32
		want  []byte
	}{
		{"zero", 0, []byte{0x00}},
		{"42", 42, []byte{42}},
		{"127", 127, []byte{127}},
		{"128", 128, []byte{0x80, 0x01}},
		{"16384", 16384, []byte{0x80, 0x80, 0x01}},
		{"-1", -1, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			out := NewStreamOutput(&buf)
			if err := out.WriteVInt(tt.value); err != nil {
				t.Fatal(err)
			}
			got := buf.Bytes()
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("WriteVInt(%d): got %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestWriteVLongEncoding(t *testing.T) {
	tests := []struct {
		name  string
		value int64
		want  []byte
	}{
		{"300", 300, []byte{0xAC, 0x02}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			out := NewStreamOutput(&buf)
			if err := out.WriteVLong(tt.value); err != nil {
				t.Fatal(err)
			}
			got := buf.Bytes()
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("WriteVLong(%d): got %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestWriteReadString(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := out.WriteString("hello"); err != nil {
		t.Fatal(err)
	}
	in := NewStreamInput(&buf)
	got, err := in.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Fatalf("expected %q, got %q", "hello", got)
	}
}

func TestWriteReadBool(t *testing.T) {
	for _, v := range []bool{true, false} {
		var buf bytes.Buffer
		out := NewStreamOutput(&buf)
		if err := out.WriteBool(v); err != nil {
			t.Fatal(err)
		}
		in := NewStreamInput(&buf)
		got, err := in.ReadBool()
		if err != nil {
			t.Fatal(err)
		}
		if got != v {
			t.Fatalf("expected %v, got %v", v, got)
		}
	}
}

func TestWriteReadByteArray(t *testing.T) {
	data := []byte(`{"key":"value"}`)
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := out.WriteByteArray(data); err != nil {
		t.Fatal(err)
	}
	in := NewStreamInput(&buf)
	got, err := in.ReadByteArray()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("expected %v, got %v", data, got)
	}
}

func TestWriteReadOptionalInt64(t *testing.T) {
	t.Run("present", func(t *testing.T) {
		var buf bytes.Buffer
		out := NewStreamOutput(&buf)
		v := int64(42)
		if err := out.WriteOptionalInt64(&v); err != nil {
			t.Fatal(err)
		}
		in := NewStreamInput(&buf)
		got, err := in.ReadOptionalInt64()
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || *got != 42 {
			t.Fatalf("expected 42, got %v", got)
		}
	})
	t.Run("nil", func(t *testing.T) {
		var buf bytes.Buffer
		out := NewStreamOutput(&buf)
		if err := out.WriteOptionalInt64(nil); err != nil {
			t.Fatal(err)
		}
		in := NewStreamInput(&buf)
		got, err := in.ReadOptionalInt64()
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Fatalf("expected nil, got %v", *got)
		}
	})
}

func TestVIntRoundtrip(t *testing.T) {
	values := []int32{0, 1, 127, 128, 255, 256, 16383, 16384, -1, -128, math.MaxInt32, math.MinInt32}
	for _, v := range values {
		var buf bytes.Buffer
		out := NewStreamOutput(&buf)
		if err := out.WriteVInt(v); err != nil {
			t.Fatalf("WriteVInt(%d): %v", v, err)
		}
		in := NewStreamInput(&buf)
		got, err := in.ReadVInt()
		if err != nil {
			t.Fatalf("ReadVInt for %d: %v", v, err)
		}
		if got != v {
			t.Fatalf("VInt roundtrip: wrote %d, got %d", v, got)
		}
	}
}

func TestVLongRoundtrip(t *testing.T) {
	values := []int64{0, 1, 127, 128, 300, -1, math.MaxInt64, math.MinInt64}
	for _, v := range values {
		var buf bytes.Buffer
		out := NewStreamOutput(&buf)
		if err := out.WriteVLong(v); err != nil {
			t.Fatalf("WriteVLong(%d): %v", v, err)
		}
		in := NewStreamInput(&buf)
		got, err := in.ReadVLong()
		if err != nil {
			t.Fatalf("ReadVLong for %d: %v", v, err)
		}
		if got != v {
			t.Fatalf("VLong roundtrip: wrote %d, got %d", v, got)
		}
	}
}

func TestFloat64Roundtrip(t *testing.T) {
	values := []float64{0, 1.5, -3.14, math.MaxFloat64, math.SmallestNonzeroFloat64}
	for _, v := range values {
		var buf bytes.Buffer
		out := NewStreamOutput(&buf)
		if err := out.WriteFloat64(v); err != nil {
			t.Fatalf("WriteFloat64(%v): %v", v, err)
		}
		in := NewStreamInput(&buf)
		got, err := in.ReadFloat64()
		if err != nil {
			t.Fatalf("ReadFloat64 for %v: %v", v, err)
		}
		if got != v {
			t.Fatalf("Float64 roundtrip: wrote %v, got %v", v, got)
		}
	}
}

func TestGenericMapRoundtrip(t *testing.T) {
	m := map[string]any{
		"name":    "test",
		"count":   int64(42),
		"score":   float64(3.14),
		"active":  true,
		"nothing": nil,
		"tags":    []any{"a", "b", int64(3)},
		"nested": map[string]any{
			"inner": "value",
			"deep": map[string]any{
				"level": int64(2),
			},
		},
	}

	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := out.WriteGenericMap(m); err != nil {
		t.Fatal(err)
	}

	in := NewStreamInput(&buf)
	got, err := in.ReadGenericMap()
	if err != nil {
		t.Fatal(err)
	}

	// Verify each field
	if got["name"] != "test" {
		t.Fatalf("name: expected %q, got %v", "test", got["name"])
	}
	if got["count"] != int64(42) {
		t.Fatalf("count: expected 42, got %v", got["count"])
	}
	if got["score"] != float64(3.14) {
		t.Fatalf("score: expected 3.14, got %v", got["score"])
	}
	if got["active"] != true {
		t.Fatalf("active: expected true, got %v", got["active"])
	}
	if got["nothing"] != nil {
		t.Fatalf("nothing: expected nil, got %v", got["nothing"])
	}

	tags, ok := got["tags"].([]any)
	if !ok {
		t.Fatalf("tags: expected []any, got %T", got["tags"])
	}
	if len(tags) != 3 || tags[0] != "a" || tags[1] != "b" || tags[2] != int64(3) {
		t.Fatalf("tags: unexpected value %v", tags)
	}

	nested, ok := got["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested: expected map[string]any, got %T", got["nested"])
	}
	if nested["inner"] != "value" {
		t.Fatalf("nested.inner: expected %q, got %v", "value", nested["inner"])
	}
	deep, ok := nested["deep"].(map[string]any)
	if !ok {
		t.Fatalf("nested.deep: expected map[string]any, got %T", nested["deep"])
	}
	if deep["level"] != int64(2) {
		t.Fatalf("nested.deep.level: expected 2, got %v", deep["level"])
	}
}

func TestGenericMapNilRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := out.WriteGenericMap(nil); err != nil {
		t.Fatal(err)
	}

	in := NewStreamInput(&buf)
	got, err := in.ReadGenericMap()
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil map, got %v", got)
	}
}
