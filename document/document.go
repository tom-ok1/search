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
)

// Field represents a single field within a document.
type Field struct {
	Name  string
	Value string
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
