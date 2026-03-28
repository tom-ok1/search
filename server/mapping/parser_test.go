package mapping

import (
	"bytes"
	"testing"

	"gosearch/document"
)

func TestParseDocument_IDAndSource(t *testing.T) {
	m := &MappingDefinition{Properties: map[string]FieldMapping{}}
	source := []byte(`{"title": "hello"}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "_id", "doc1", document.FieldTypeKeyword)
	assertHasBytesField(t, doc, "_source", source, document.FieldTypeStored)
}

func TestParseDocument_TextField(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"title": {Type: FieldTypeText},
		},
	}
	source := []byte(`{"title": "hello world"}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "title", "hello world", document.FieldTypeText)
}

func TestParseDocument_KeywordField(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"status": {Type: FieldTypeKeyword},
		},
	}
	source := []byte(`{"status": "published"}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "status", "published", document.FieldTypeKeyword)
}

func TestParseDocument_LongField(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"count": {Type: FieldTypeLong},
		},
	}
	source := []byte(`{"count": 42}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "count", "42", document.FieldTypeKeyword)
	assertHasNumericField(t, doc, "count", 42)
}

func TestParseDocument_DoubleField(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"score": {Type: FieldTypeDouble},
		},
	}
	source := []byte(`{"score": 3.14}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "score", "3.14", document.FieldTypeKeyword)
	assertHasNumericDocValues(t, doc, "score")
}

func TestParseDocument_BooleanFieldTrue(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"active": {Type: FieldTypeBoolean},
		},
	}
	source := []byte(`{"active": true}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "active", "true", document.FieldTypeKeyword)
}

func TestParseDocument_BooleanFieldFalse(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"active": {Type: FieldTypeBoolean},
		},
	}
	source := []byte(`{"active": false}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "active", "false", document.FieldTypeKeyword)
}

func TestParseDocument_UnmappedFieldsIgnored(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"title": {Type: FieldTypeText},
		},
	}
	source := []byte(`{"title": "hello", "unmapped": "ignored"}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	for _, f := range doc.Fields {
		if f.Name == "unmapped" {
			t.Error("unmapped field should not be present in document")
		}
	}
}

func TestParseDocument_MultipleFields(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"title":  {Type: FieldTypeText},
			"status": {Type: FieldTypeKeyword},
			"count":  {Type: FieldTypeLong},
			"active": {Type: FieldTypeBoolean},
		},
	}
	source := []byte(`{"title": "hello", "status": "draft", "count": 5, "active": true}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "title", "hello", document.FieldTypeText)
	assertHasField(t, doc, "status", "draft", document.FieldTypeKeyword)
	assertHasField(t, doc, "count", "5", document.FieldTypeKeyword)
	assertHasField(t, doc, "active", "true", document.FieldTypeKeyword)
}

func TestParseDocument_InvalidJSON(t *testing.T) {
	m := &MappingDefinition{Properties: map[string]FieldMapping{}}
	_, err := ParseDocument("doc1", []byte(`not json`), m)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParseDocument_LongFieldWithFloatValue(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"count": {Type: FieldTypeLong},
		},
	}
	source := []byte(`{"count": 42.0}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "count", "42", document.FieldTypeKeyword)
	assertHasNumericField(t, doc, "count", 42)
}

func TestParseDocument_TextFieldJapanese(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"title": {Type: FieldTypeText},
		},
	}
	source := []byte(`{"title": "東京タワー"}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "title", "東京タワー", document.FieldTypeText)
}

func TestParseDocument_KeywordFieldJapanese(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"category": {Type: FieldTypeKeyword},
		},
	}
	source := []byte(`{"category": "観光地"}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "category", "観光地", document.FieldTypeKeyword)
}

func TestParseDocument_MultipleFieldsJapanese(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"title":    {Type: FieldTypeText},
			"category": {Type: FieldTypeKeyword},
		},
	}
	source := []byte(`{"title": "東京 大阪 名古屋", "category": "都市"}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "title", "東京 大阪 名古屋", document.FieldTypeText)
	assertHasField(t, doc, "category", "都市", document.FieldTypeKeyword)
}

func TestParseDocument_MissingMappedField(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"title": {Type: FieldTypeText},
		},
	}
	source := []byte(`{"other": "value"}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	for _, f := range doc.Fields {
		if f.Name == "title" {
			t.Error("missing mapped field should not produce a document field")
		}
	}
}

// --- test helpers ---

func assertHasField(t *testing.T, doc *document.Document, name, value string, ft document.FieldType) {
	t.Helper()
	for _, f := range doc.Fields {
		if f.Name == name && f.Value == value && f.Type == ft {
			return
		}
	}
	t.Errorf("expected field {Name:%q, Value:%q, Type:%v} not found in document", name, value, ft)
}

func assertHasBytesField(t *testing.T, doc *document.Document, name string, value []byte, ft document.FieldType) {
	t.Helper()
	for _, f := range doc.Fields {
		if f.Name == name && bytes.Equal(f.BytesValue, value) && f.Type == ft {
			return
		}
	}
	t.Errorf("expected bytes field {Name:%q, BytesValue:%q, Type:%v} not found in document", name, value, ft)
}

func assertHasNumericField(t *testing.T, doc *document.Document, name string, value int64) {
	t.Helper()
	for _, f := range doc.Fields {
		if f.Name == name && f.Type == document.FieldTypeNumericDocValues && f.NumericValue == value {
			return
		}
	}
	t.Errorf("expected numeric doc values field {Name:%q, NumericValue:%d} not found", name, value)
}

func assertHasNumericDocValues(t *testing.T, doc *document.Document, name string) {
	t.Helper()
	for _, f := range doc.Fields {
		if f.Name == name && f.Type == document.FieldTypeNumericDocValues {
			return
		}
	}
	t.Errorf("expected numeric doc values field {Name:%q} not found", name)
}
