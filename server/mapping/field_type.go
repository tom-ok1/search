package mapping

// FieldType represents an Elasticsearch field type.
type FieldType string

const (
	FieldTypeText    FieldType = "text"
	FieldTypeKeyword FieldType = "keyword"
	FieldTypeLong    FieldType = "long"
	FieldTypeDouble  FieldType = "double"
	FieldTypeBoolean FieldType = "boolean"
)
