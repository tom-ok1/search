package document

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
)

// Field represents a single field within a document.
type Field struct {
	Name         string
	Value        string
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

func (d *Document) AddSortedDocValuesField(name string, value string) {
	d.Fields = append(d.Fields, Field{
		Name:  name,
		Value: value,
		Type:  FieldTypeSortedDocValues,
	})
}
