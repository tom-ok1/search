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
type Field struct {
	Name         string
	Value        string
	BytesValue   []byte // used for binary stored fields (e.g. _source)
	Type         FieldType
	NumericValue int64 // used when Type == FieldTypeNumericDocValues
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
		Name:         name,
		Type:         FieldTypeNumericDocValues,
		NumericValue: value,
	})
}

func (d *Document) AddBytesField(name string, value []byte, fieldType FieldType) {
	d.Fields = append(d.Fields, Field{
		Name:       name,
		BytesValue: value,
		Type:       fieldType,
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
		Name:         name,
		Type:         FieldTypeLongPoint,
		NumericValue: value,
	})
}

// AddDoublePoint adds a double field indexed as a point for range queries
// and as numeric doc values for sorting. The value is stored as a sortable long
// using the same encoding as Lucene's NumericUtils.doubleToSortableLong.
func (d *Document) AddDoublePoint(name string, value float64) {
	d.Fields = append(d.Fields, Field{
		Name:         name,
		Type:         FieldTypeDoublePoint,
		NumericValue: doubleToSortableLong(value),
	})
}

// doubleToSortableLong converts a float64 to an int64 that sorts in the same order.
// This is equivalent to Lucene's NumericUtils.doubleToSortableLong.
func doubleToSortableLong(value float64) int64 {
	bits := int64(math.Float64bits(value))
	return bits ^ (bits>>63)&0x7fffffffffffffff
}

// sortableLongToDouble reverses doubleToSortableLong.
func sortableLongToDouble(encoded int64) float64 {
	bits := encoded ^ ((encoded >> 63) & 0x7fffffffffffffff)
	return math.Float64frombits(uint64(bits))
}
