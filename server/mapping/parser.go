package mapping

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"gosearch/document"
)

// ParseDocument converts a JSON source document into a Lucene document
// according to the given mapping definition.
func ParseDocument(id string, source []byte, m *MappingDefinition) (*document.Document, error) {
	var fields map[string]any
	decoder := json.NewDecoder(bytes.NewReader(source))
	decoder.UseNumber()
	if err := decoder.Decode(&fields); err != nil {
		return nil, fmt.Errorf("invalid JSON source: %w", err)
	}

	doc := document.NewDocument()

	// Add _id as keyword field
	doc.AddField("_id", id, document.FieldTypeKeyword)

	// Add _source as stored field (raw JSON bytes)
	doc.AddBytesField("_source", source, document.FieldTypeStored)

	// Add _seq_no and _primary_term as numeric doc values fields.
	// The actual values are set by the engine after seqNo assignment;
	// these are placeholders that will be overwritten.
	doc.AddNumericDocValuesField("_seq_no", 0)
	doc.AddNumericDocValuesField("_primary_term", 0)

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
	// Handle array values: index each element as a separate field (same as Elasticsearch).
	if arr, ok := value.([]any); ok {
		for _, elem := range arr {
			if err := addScalarField(doc, name, elem, fm); err != nil {
				return err
			}
		}
		return nil
	}
	return addScalarField(doc, name, value, fm)
}

func addScalarField(doc *document.Document, name string, value any, fm FieldMapping) error {
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
		doc.AddSortedDocValuesField(name, s)

	case FieldTypeLong:
		n, err := toInt64(value)
		if err != nil {
			return err
		}
		doc.AddLongPoint(name, n)

	case FieldTypeDouble:
		f, err := toFloat64(value)
		if err != nil {
			return err
		}
		doc.AddDoublePoint(name, f)

	case FieldTypeBoolean:
		b, err := toBool(value)
		if err != nil {
			return err
		}
		s := strconv.FormatBool(b)
		doc.AddField(name, s, document.FieldTypeKeyword)
		doc.AddSortedDocValuesField(name, s)

	default:
		return fmt.Errorf("unsupported field type: %s", fm.Type)
	}

	return nil
}

func toString(v any) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case json.Number:
		return string(val), nil
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(val), nil
	case nil:
		return "", fmt.Errorf("null value")
	default:
		return "", fmt.Errorf("cannot convert %T to string", v)
	}
}

func toInt64(v any) (int64, error) {
	switch val := v.(type) {
	case json.Number:
		// Try integer parse first; fall back to float for values like "42.0"
		if n, err := strconv.ParseInt(string(val), 10, 64); err == nil {
			return n, nil
		}
		f, err := strconv.ParseFloat(string(val), 64)
		if err != nil {
			return 0, err
		}
		return int64(f), nil
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
	case json.Number:
		return strconv.ParseFloat(string(val), 64)
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
