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

func TestParseDocument_TextFieldEmoji(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"title": {Type: FieldTypeText},
		},
	}
	source := []byte(`{"title": "hello 🔍 world"}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "title", "hello 🔍 world", document.FieldTypeText)
}

func TestParseDocument_KeywordWithSpaces(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"city": {Type: FieldTypeKeyword},
		},
	}
	source := []byte(`{"city": "New York"}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "city", "New York", document.FieldTypeKeyword)
}

func TestParseDocument_KeywordSpecialChars(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"tag": {Type: FieldTypeKeyword},
		},
	}
	tests := []struct {
		json     string // JSON value (with JSON escaping)
		expected string // Expected Go string value (after JSON parsing)
	}{
		{`"C++"`, "C++"},
		{`"user@example.com"`, "user@example.com"},
		{`"path\\to\\file"`, `path\to\file`},
		{`"007"`, "007"},
	}

	for _, tt := range tests {
		source := []byte(`{"tag": ` + tt.json + `}`)
		doc, err := ParseDocument("doc1", source, m)
		if err != nil {
			t.Fatalf("ParseDocument(%s): %v", tt.json, err)
		}
		assertHasField(t, doc, "tag", tt.expected, document.FieldTypeKeyword)
	}
}

func TestParseDocument_TextFieldAccented(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"body": {Type: FieldTypeText},
		},
	}
	source := []byte(`{"body": "café résumé naïve"}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "body", "café résumé naïve", document.FieldTypeText)
}

func TestParseDocument_TextFieldCJKExtensionB(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"body": {Type: FieldTypeText},
		},
	}
	source := []byte(`{"body": "𠮷野家 テスト"}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "body", "𠮷野家 テスト", document.FieldTypeText)
}

func TestParseDocument_SpecialCharsInID(t *testing.T) {
	m := &MappingDefinition{Properties: map[string]FieldMapping{}}

	// IDs with special characters
	ids := []string{"user@example.com", "path/to/doc", "doc#1", "doc with spaces"}
	for _, id := range ids {
		source := []byte(`{"title": "test"}`)
		doc, err := ParseDocument(id, source, m)
		if err != nil {
			t.Fatalf("ParseDocument(id=%q): %v", id, err)
		}
		assertHasField(t, doc, "_id", id, document.FieldTypeKeyword)
	}
}

func TestParseDocument_SourcePreservesSpecialChars(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"title": {Type: FieldTypeText},
		},
	}
	source := []byte(`{"title": "café 🔍 𠮷野家"}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	// _source should preserve the raw JSON including special chars
	assertHasBytesField(t, doc, "_source", source, document.FieldTypeStored)
}

func TestParseDocument_EscapedJSONInField(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"data": {Type: FieldTypeText},
		},
	}
	// JSON with escaped quotes
	source := []byte(`{"data": "he said \"hello\""}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "data", `he said "hello"`, document.FieldTypeText)
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

func TestParseDocument_KeywordArrayField(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"tags": {Type: FieldTypeKeyword},
		},
	}
	source := []byte(`{"tags": ["search", "information retrieval", "nlp"]}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "tags", "search", document.FieldTypeKeyword)
	assertHasField(t, doc, "tags", "information retrieval", document.FieldTypeKeyword)
	assertHasField(t, doc, "tags", "nlp", document.FieldTypeKeyword)
	assertFieldCount(t, doc, "tags", 3)
}

func TestParseDocument_TextArrayField(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"comments": {Type: FieldTypeText},
		},
	}
	source := []byte(`{"comments": ["great book", "must read"]}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "comments", "great book", document.FieldTypeText)
	assertHasField(t, doc, "comments", "must read", document.FieldTypeText)
	assertFieldCount(t, doc, "comments", 2)
}

func TestParseDocument_EmptyArrayField(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"tags": {Type: FieldTypeKeyword},
		},
	}
	source := []byte(`{"tags": []}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertFieldCount(t, doc, "tags", 0)
}

func TestParseDocument_SingleElementArrayField(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"tags": {Type: FieldTypeKeyword},
		},
	}
	source := []byte(`{"tags": ["only-one"]}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "tags", "only-one", document.FieldTypeKeyword)
	assertFieldCount(t, doc, "tags", 1)
}

func TestParseDocument_LongArrayField(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"scores": {Type: FieldTypeLong},
		},
	}
	source := []byte(`{"scores": [10, 20, 30]}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "scores", "10", document.FieldTypeKeyword)
	assertHasField(t, doc, "scores", "20", document.FieldTypeKeyword)
	assertHasField(t, doc, "scores", "30", document.FieldTypeKeyword)
}

func TestParseDocument_BooleanArrayField(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"flags": {Type: FieldTypeBoolean},
		},
	}
	source := []byte(`{"flags": [true, false]}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "flags", "true", document.FieldTypeKeyword)
	assertHasField(t, doc, "flags", "false", document.FieldTypeKeyword)
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

func assertFieldCount(t *testing.T, doc *document.Document, name string, expected int) {
	t.Helper()
	count := 0
	for _, f := range doc.Fields {
		if f.Name == name {
			count++
		}
	}
	if count != expected {
		t.Errorf("expected %d fields named %q, got %d", expected, name, count)
	}
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
