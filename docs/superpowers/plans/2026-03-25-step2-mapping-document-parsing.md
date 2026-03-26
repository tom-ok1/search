# Step 2: Mapping & Document Parsing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the schema system that translates JSON documents into Lucene documents. Define field type constants, mapping definitions, and a `ParseDocument` function that converts JSON source + mapping into `document.Document`.

**Architecture:** The `server/mapping/` package sits between the REST/action layer and GoSearch's `document` package. It defines how Elasticsearch field types map to Lucene field types.

**Tech Stack:** Go 1.23, existing `gosearch/document` package

---

### Task 1: Field Type Constants & Mapping Types

**Files:**
- Create: `server/mapping/field_type.go`
- Create: `server/mapping/mapping.go`

- [ ] **Step 1: Define FieldType constants**

```go
// server/mapping/field_type.go
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
```

- [ ] **Step 2: Define MappingDefinition and FieldMapping**

```go
// server/mapping/mapping.go
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
```

- [ ] **Step 3: Verify compilation**

Run: `go build ./server/mapping/`
Expected: SUCCESS — no errors

---

### Task 2: ParseDocument Implementation

**Files:**
- Create: `server/mapping/parser.go`
- Test: `server/mapping/parser_test.go`

- [ ] **Step 1: Write failing tests for ParseDocument**

```go
// server/mapping/parser_test.go
package mapping

import (
	"encoding/json"
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
	assertHasField(t, doc, "_source", string(source), document.FieldTypeStored)
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

	// Long fields produce a keyword field (for term queries) and a numeric doc values field (for sorting)
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

	// Double fields produce a keyword field (for term queries) and a numeric doc values field (for sorting)
	assertHasField(t, doc, "score", "3.14", document.FieldTypeKeyword)
	// Numeric doc values stores float64 bits as int64
	assertHasNumericDocValues(t, doc, "score")
}

func TestParseDocument_BooleanField(t *testing.T) {
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
	// JSON numbers are always float64 when unmarshalled to interface{}
	source := []byte(`{"count": 42.0}`)

	doc, err := ParseDocument("doc1", source, m)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}

	assertHasField(t, doc, "count", "42", document.FieldTypeKeyword)
	assertHasNumericField(t, doc, "count", 42)
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

// suppress unused import
var _ = json.Marshal
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./server/mapping/ -v`
Expected: FAIL — ParseDocument does not exist

- [ ] **Step 3: Implement ParseDocument**

```go
// server/mapping/parser.go
package mapping

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"gosearch/document"
)

// ParseDocument converts a JSON source document into a Lucene document
// according to the given mapping definition.
func ParseDocument(id string, source []byte, m *MappingDefinition) (*document.Document, error) {
	var fields map[string]any
	if err := json.Unmarshal(source, &fields); err != nil {
		return nil, fmt.Errorf("invalid JSON source: %w", err)
	}

	doc := document.NewDocument()

	// Add _id as keyword field
	doc.AddField("_id", id, document.FieldTypeKeyword)

	// Add _source as stored field (raw JSON bytes)
	doc.AddField("_source", string(source), document.FieldTypeStored)

	// Process each mapped field
	for fieldName, fieldMapping := range m.Properties {
		value, ok := fields[fieldName]
		if !ok {
			continue
		}

		if err := addField(doc, fieldName, value, fieldMapping); err != nil {
			return nil, fmt.Errorf("field %q: %w", fieldName, err)
		}
	}

	return doc, nil
}

func addField(doc *document.Document, name string, value any, fm FieldMapping) error {
	switch fm.Type {
	case FieldTypeText:
		s, err := toString(value)
		if err != nil {
			return err
		}
		doc.AddField(name, s, document.FieldTypeText)

	case FieldTypeKeyword:
		s, err := toString(value)
		if err != nil {
			return err
		}
		doc.AddField(name, s, document.FieldTypeKeyword)

	case FieldTypeLong:
		n, err := toInt64(value)
		if err != nil {
			return err
		}
		doc.AddField(name, strconv.FormatInt(n, 10), document.FieldTypeKeyword)
		doc.AddNumericDocValuesField(name, n)

	case FieldTypeDouble:
		f, err := toFloat64(value)
		if err != nil {
			return err
		}
		doc.AddField(name, strconv.FormatFloat(f, 'f', -1, 64), document.FieldTypeKeyword)
		doc.AddNumericDocValuesField(name, int64(math.Float64bits(f)))

	case FieldTypeBoolean:
		b, err := toBool(value)
		if err != nil {
			return err
		}
		doc.AddField(name, strconv.FormatBool(b), document.FieldTypeKeyword)

	default:
		return fmt.Errorf("unsupported field type: %s", fm.Type)
	}

	return nil
}

func toString(v any) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(val), nil
	case nil:
		return "", fmt.Errorf("null value")
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

func toInt64(v any) (int64, error) {
	switch val := v.(type) {
	case float64:
		return int64(val), nil
	case string:
		return strconv.ParseInt(val, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case string:
		return strconv.ParseFloat(val, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func toBool(v any) (bool, error) {
	switch val := v.(type) {
	case bool:
		return val, nil
	case string:
		return strconv.ParseBool(val)
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./server/mapping/ -v`
Expected: PASS — all tests pass

- [ ] **Step 5: Run all server tests**

Run: `go test ./server/... -v`
Expected: PASS — all tests across all server packages pass

---

### Summary of files created

| File | Responsibility |
|---|---|
| `server/mapping/field_type.go` | FieldType constants (text, keyword, long, double, boolean) |
| `server/mapping/mapping.go` | MappingDefinition and FieldMapping types |
| `server/mapping/parser.go` | ParseDocument: JSON source + mapping → document.Document |
| `server/mapping/parser_test.go` | Unit tests for all field types, edge cases |
