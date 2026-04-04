package aggregation

import (
	"testing"

	"gosearch/server/mapping"
)

func TestParseAggregations_ValueCount(t *testing.T) {
	input := map[string]any{
		"my_count": map[string]any{
			"value_count": map[string]any{
				"field": "status",
			},
		},
	}

	aggs, err := ParseAggregations(input, &mapping.MappingDefinition{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(aggs) != 1 {
		t.Fatalf("expected 1 aggregation, got %d", len(aggs))
	}
	if aggs[0].Name() != "my_count" {
		t.Errorf("expected name 'my_count', got %q", aggs[0].Name())
	}
	if _, ok := aggs[0].(*ValueCountAggregator); !ok {
		t.Errorf("expected *ValueCountAggregator, got %T", aggs[0])
	}
}

func TestParseAggregations_Terms(t *testing.T) {
	input := map[string]any{
		"by_status": map[string]any{
			"terms": map[string]any{
				"field": "status",
				"size":  float64(5),
			},
		},
	}

	aggs, err := ParseAggregations(input, &mapping.MappingDefinition{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(aggs) != 1 {
		t.Fatalf("expected 1 aggregation, got %d", len(aggs))
	}
	if aggs[0].Name() != "by_status" {
		t.Errorf("expected name 'by_status', got %q", aggs[0].Name())
	}
	ta, ok := aggs[0].(*TermsAggregator)
	if !ok {
		t.Fatalf("expected *TermsAggregator, got %T", aggs[0])
	}
	if ta.size != 5 {
		t.Errorf("expected size 5, got %d", ta.size)
	}
}

func TestParseAggregations_TermsDefaultSize(t *testing.T) {
	input := map[string]any{
		"by_status": map[string]any{
			"terms": map[string]any{
				"field": "status",
			},
		},
	}

	aggs, err := ParseAggregations(input, &mapping.MappingDefinition{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ta, ok := aggs[0].(*TermsAggregator)
	if !ok {
		t.Fatalf("expected *TermsAggregator, got %T", aggs[0])
	}
	if ta.size != 10 {
		t.Errorf("expected default size 10, got %d", ta.size)
	}
}

func TestParseAggregations_UnknownType(t *testing.T) {
	input := map[string]any{
		"my_agg": map[string]any{
			"avg": map[string]any{
				"field": "price",
			},
		},
	}

	_, err := ParseAggregations(input, &mapping.MappingDefinition{})
	if err == nil {
		t.Fatal("expected error for unknown aggregation type")
	}
}

func TestParseAggregations_InvalidStructure(t *testing.T) {
	input := map[string]any{
		"bad": "not an object",
	}

	_, err := ParseAggregations(input, &mapping.MappingDefinition{})
	if err == nil {
		t.Fatal("expected error for non-object aggregation definition")
	}
}

func TestParseAggregations_MissingField(t *testing.T) {
	input := map[string]any{
		"my_count": map[string]any{
			"value_count": map[string]any{},
		},
	}

	_, err := ParseAggregations(input, &mapping.MappingDefinition{})
	if err == nil {
		t.Fatal("expected error for missing field")
	}
}
