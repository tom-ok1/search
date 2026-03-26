package mapping

// MappingDefinition defines the field mappings for an index.
type MappingDefinition struct {
	Properties map[string]FieldMapping
}

// FieldMapping defines the type and configuration of a single field.
type FieldMapping struct {
	Type     FieldType
	Analyzer string // for text fields, defaults to standard
}
