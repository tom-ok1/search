package document

import "math"

// FieldType represents the type of a field.
type FieldType int

const (
	// FieldTypeText is a field that is analyzed and added to the inverted index.
	FieldTypeText FieldType = iota
	// FieldTypeKeyword is a field that is indexed as a single term without analysis.
	FieldTypeKeyword
	// FieldTypeStored is a field that is only stored and returned with search results.
	FieldTypeStored
	// FieldTypeNumericDocValues stores a per-document int64 value for sorting.
	FieldTypeNumericDocValues
	// FieldTypeSortedDocValues stores a per-document string value with ordinal deduplication for sorting.
	FieldTypeSortedDocValues
	// FieldTypeLongPoint is indexed as a numeric point for range queries + numeric doc values for sorting.
	FieldTypeLongPoint
	// FieldTypeDoublePoint is indexed as a numeric point for range queries + numeric doc values for sorting.
	FieldTypeDoublePoint
)

// Field represents a single field within a document.
// Value holds the field's data and its concrete type depends on Type:
//   - FieldTypeText, FieldTypeKeyword, FieldTypeSortedDocValues: string
//   - FieldTypeStored: string or []byte
//   - FieldTypeNumericDocValues, FieldTypeLongPoint, FieldTypeDoublePoint: int64
type Field struct {
	Name  string
	Value any
	Type  FieldType
}

// Document represents a single document to be indexed.
type Document struct {
	Fields []Field
}

func NewDocument() *Document {
	return &Document{}
}

func (d *Document) AddField(name, value string, fieldType FieldType) {
	d.Fields = append(d.Fields, Field{
		Name:  name,
		Value: value,
		Type:  fieldType,
	})
}

func (d *Document) AddNumericDocValuesField(name string, value int64) {
	d.Fields = append(d.Fields, Field{
		Name:  name,
		Type:  FieldTypeNumericDocValues,
		Value: value,
	})
}

func (d *Document) AddBytesField(name string, value []byte, fieldType FieldType) {
	d.Fields = append(d.Fields, Field{
		Name:  name,
		Value: value,
		Type:  fieldType,
	})
}

func (d *Document) AddSortedDocValuesField(name string, value string) {
	d.Fields = append(d.Fields, Field{
		Name:  name,
		Value: value,
		Type:  FieldTypeSortedDocValues,
	})
}

// AddLongPoint adds a long field indexed as a point for range queries
// and as numeric doc values for sorting. Mirrors Lucene's LongPoint + SortedNumericDocValuesField.
func (d *Document) AddLongPoint(name string, value int64) {
	d.Fields = append(d.Fields, Field{
		Name:  name,
		Type:  FieldTypeLongPoint,
		Value: value,
	})
}

// AddDoublePoint adds a double field indexed as a point for range queries
// and as numeric doc values for sorting. The value is stored as a sortable long
// using the same encoding as Lucene's NumericUtils.doubleToSortableLong.
func (d *Document) AddDoublePoint(name string, value float64) {
	d.Fields = append(d.Fields, Field{
		Name:  name,
		Type:  FieldTypeDoublePoint,
		Value: DoubleToSortableLong(value),
	})
}

// SetSeqNoFields updates the _seq_no and _primary_term numeric doc values
// fields in the document. These fields must have been added previously
// (e.g., by ParseDocument). This mirrors Elasticsearch's
// ParsedDocument.updateSeqID().
func (d *Document) SetSeqNoFields(seqNo, primaryTerm int64) {
	for i := range d.Fields {
		switch d.Fields[i].Name {
		case "_seq_no":
			d.Fields[i].Value = seqNo
		case "_primary_term":
			d.Fields[i].Value = primaryTerm
		}
	}
}

// DoubleToSortableLong converts a float64 to an int64 that sorts in the same order.
// This is equivalent to Lucene's NumericUtils.doubleToSortableLong.
func DoubleToSortableLong(value float64) int64 {
	bits := int64(math.Float64bits(value))
	return bits ^ (bits>>63)&0x7fffffffffffffff
}

// SortableLongToDouble reverses DoubleToSortableLong.
func SortableLongToDouble(encoded int64) float64 {
	bits := encoded ^ ((encoded >> 63) & 0x7fffffffffffffff)
	return math.Float64frombits(uint64(bits))
}
