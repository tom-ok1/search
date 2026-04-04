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

	assertHasLongPoint(t, doc, "count", 42)
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

	assertHasDoublePoint(t, doc, "score", 3.14)
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
	assertHasLongPoint(t, doc, "count", 5)
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

	assertHasLongPoint(t, doc, "count", 42)
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
	// 3 keyword fields + 3 sorted doc values fields = 6 total
	assertFieldCount(t, doc, "tags", 6)
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
	// 1 keyword field + 1 sorted doc values field = 2 total
	assertFieldCount(t, doc, "tags", 2)
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

	assertHasLongPoint(t, doc, "scores", 10)
	assertHasLongPoint(t, doc, "scores", 20)
	assertHasLongPoint(t, doc, "scores", 30)
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

func TestParseDocument_LargeIntegerPrecision(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"big_id": {Type: FieldTypeLong},
		},
	}

	// 2^53 + 1 = 9007199254740993 — this value loses precision with float64
	source := []byte(`{"big_id": 9007199254740993}`)
	doc, err := ParseDocument("1", source, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Find the long point field for big_id
	for _, f := range doc.Fields {
		if f.Name == "big_id" && f.Type == document.FieldTypeLongPoint {
			var want int64 = 9007199254740993
			if f.Value.(int64) != want {
				t.Errorf("big_id numeric = %d, want %d (precision lost)", f.Value.(int64), want)
			}
			return
		}
	}
	t.Fatal("big_id long point field not found")
}

func TestParseDocument_LargeIntegerDocValues(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"big_id": {Type: FieldTypeLong},
		},
	}

	source := []byte(`{"big_id": 9007199254740993}`)
	doc, err := ParseDocument("1", source, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Point fields include both indexing and doc values in a single field
	assertHasLongPoint(t, doc, "big_id", 9007199254740993)
}

func TestParseDocument_KeywordHasSortedDocValues(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"status": {Type: FieldTypeKeyword},
		},
	}

	source := []byte(`{"status": "active"}`)
	doc, err := ParseDocument("1", source, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, f := range doc.Fields {
		if f.Name == "status" && f.Type == document.FieldTypeSortedDocValues {
			if f.Value.(string) != "active" {
				t.Errorf("sorted doc value = %q, want %q", f.Value.(string), "active")
			}
			return
		}
	}
	t.Fatal("keyword field 'status' missing FieldTypeSortedDocValues")
}

func TestParseDocument_BooleanHasSortedDocValues(t *testing.T) {
	m := &MappingDefinition{
		Properties: map[string]FieldMapping{
			"active": {Type: FieldTypeBoolean},
		},
	}

	source := []byte(`{"active": true}`)
	doc, err := ParseDocument("1", source, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, f := range doc.Fields {
		if f.Name == "active" && f.Type == document.FieldTypeSortedDocValues {
			if f.Value.(string) != "true" {
				t.Errorf("sorted doc value = %q, want %q", f.Value.(string), "true")
			}
			return
		}
	}
	t.Fatal("boolean field 'active' missing FieldTypeSortedDocValues")
}

// --- test helpers ---

func assertHasField(t *testing.T, doc *document.Document, name, value string, ft document.FieldType) {
	t.Helper()
	for _, f := range doc.Fields {
		if f.Name == name && f.Type == ft {
			if s, ok := f.Value.(string); ok && s == value {
				return
			}
		}
	}
	t.Errorf("expected field {Name:%q, Value:%q, Type:%v} not found in document", name, value, ft)
}

func assertHasBytesField(t *testing.T, doc *document.Document, name string, value []byte, ft document.FieldType) {
	t.Helper()
	for _, f := range doc.Fields {
		if f.Name == name && f.Type == ft {
			if b, ok := f.Value.([]byte); ok && bytes.Equal(b, value) {
				return
			}
		}
	}
	t.Errorf("expected bytes field {Name:%q, Value:%q, Type:%v} not found in document", name, value, ft)
}

func assertHasNumericField(t *testing.T, doc *document.Document, name string, value int64) {
	t.Helper()
	for _, f := range doc.Fields {
		if f.Name == name && f.Type == document.FieldTypeNumericDocValues {
			if n, ok := f.Value.(int64); ok && n == value {
				return
			}
		}
	}
	t.Errorf("expected numeric doc values field {Name:%q, Value:%d} not found", name, value)
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

func assertHasLongPoint(t *testing.T, doc *document.Document, name string, value int64) {
	t.Helper()
	for _, f := range doc.Fields {
		if f.Name == name && f.Type == document.FieldTypeLongPoint {
			if n, ok := f.Value.(int64); ok && n == value {
				return
			}
		}
	}
	t.Errorf("expected long point field {Name:%q, Value:%d, Type:FieldTypeLongPoint} not found", name, value)
}

func assertHasDoublePoint(t *testing.T, doc *document.Document, name string, value float64) {
	t.Helper()
	sortableLong := document.DoubleToSortableLong(value)

	for _, f := range doc.Fields {
		if f.Name == name && f.Type == document.FieldTypeDoublePoint {
			if n, ok := f.Value.(int64); ok && n == sortableLong {
				return
			}
		}
	}
	t.Errorf("expected double point field {Name:%q, Value:%f, Type:FieldTypeDoublePoint} not found", name, value)
}
