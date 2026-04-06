package document

import (
	"math"
	"testing"
)

func TestDoubleToSortableLong(t *testing.T) {
	tests := []struct {
		name  string
		value float64
		want  int64
	}{
		// Test basic values - verify ordering property
		{"zero", 0.0, 0},
		{"positive small", 1.0, 4607182418800017408},
		{"negative small", -1.0, -4607182418800017409},

		// Special values
		{"positive infinity", math.Inf(1), 9218868437227405312},
		{"negative infinity", math.Inf(-1), -9218868437227405313},
		{"NaN", math.NaN(), 9221120237041090561},

		// Edge cases
		{"max float64", math.MaxFloat64, 9218868437227405311},
		{"smallest positive", math.SmallestNonzeroFloat64, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DoubleToSortableLong(tt.value)

			// For NaN, we only verify it's a valid int64, not exact value
			// since NaN representations can vary
			if math.IsNaN(tt.value) {
				if got < 0 {
					t.Errorf("NaN encoded to negative value: %d", got)
				}
				return
			}

			if got != tt.want {
				t.Errorf("DoubleToSortableLong(%v) = %d, want %d", tt.value, got, tt.want)
			}
		})
	}
}

func TestDoubleEncodingRoundTrip(t *testing.T) {
	tests := []float64{
		0.0,
		1.0,
		-1.0,
		42.5,
		-42.5,
		math.MaxFloat64,
		-math.MaxFloat64,
		math.SmallestNonzeroFloat64,
		math.Inf(1),
		math.Inf(-1),
	}

	for _, val := range tests {
		t.Run("", func(t *testing.T) {
			encoded := DoubleToSortableLong(val)
			decoded := SortableLongToDouble(encoded)

			if val != decoded {
				t.Errorf("round trip failed: %v -> %d -> %v", val, encoded, decoded)
			}
		})
	}
}

func TestDoubleEncodingOrdering(t *testing.T) {
	// The key property: if a < b, then encoded(a) < encoded(b)
	values := []float64{
		math.Inf(-1),
		-math.MaxFloat64,
		-1000.0,
		-1.0,
		-0.5,
		0.0,
		0.5,
		1.0,
		1000.0,
		math.MaxFloat64,
		math.Inf(1),
	}

	for i := 0; i < len(values)-1; i++ {
		a := values[i]
		b := values[i+1]

		encodedA := DoubleToSortableLong(a)
		encodedB := DoubleToSortableLong(b)

		if encodedA >= encodedB {
			t.Errorf("ordering violated: %v (encoded: %d) should be < %v (encoded: %d)",
				a, encodedA, b, encodedB)
		}
	}
}

func TestDocument_SetSeqNoFields(t *testing.T) {
	doc := NewDocument()
	doc.AddField("_id", "1", FieldTypeKeyword)
	doc.AddNumericDocValuesField("_seq_no", 0)
	doc.AddNumericDocValuesField("_primary_term", 0)

	doc.SetSeqNoFields(42, 3)

	var seqNo, primaryTerm int64
	for _, f := range doc.Fields {
		if f.Name == "_seq_no" {
			seqNo = f.Value.(int64)
		}
		if f.Name == "_primary_term" {
			primaryTerm = f.Value.(int64)
		}
	}
	if seqNo != 42 {
		t.Fatalf("expected _seq_no=42, got %d", seqNo)
	}
	if primaryTerm != 3 {
		t.Fatalf("expected _primary_term=3, got %d", primaryTerm)
	}
}

func TestAddLongPoint(t *testing.T) {
	doc := NewDocument()
	doc.AddLongPoint("price", 42)

	if len(doc.Fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(doc.Fields))
	}

	field := doc.Fields[0]
	if field.Name != "price" {
		t.Errorf("field name = %q, want \"price\"", field.Name)
	}
	if field.Type != FieldTypeLongPoint {
		t.Errorf("field type = %v, want FieldTypeLongPoint", field.Type)
	}
	if field.Value.(int64) != 42 {
		t.Errorf("numeric value = %d, want 42", field.Value.(int64))
	}
}

func TestAddDoublePoint(t *testing.T) {
	doc := NewDocument()
	doc.AddDoublePoint("score", 3.14)

	if len(doc.Fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(doc.Fields))
	}

	field := doc.Fields[0]
	if field.Name != "score" {
		t.Errorf("field name = %q, want \"score\"", field.Name)
	}
	if field.Type != FieldTypeDoublePoint {
		t.Errorf("field type = %v, want FieldTypeDoublePoint", field.Type)
	}

	// Verify the stored value can be decoded back to original
	decoded := SortableLongToDouble(field.Value.(int64))
	if decoded != 3.14 {
		t.Errorf("decoded value = %v, want 3.14", decoded)
	}
}
