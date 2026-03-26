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
	doc.AddBytesField("_source", source, document.FieldTypeStored)

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
