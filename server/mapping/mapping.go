package mapping

// MappingDefinition defines the field mappings for an index.
type MappingDefinition struct {
	Properties map[string]FieldMapping `json:"properties"`
}

// FieldMapping defines the type and configuration of a single field.
type FieldMapping struct {
	Type     FieldType `json:"type"`
	Analyzer string    `json:"analyzer,omitempty"`
}
