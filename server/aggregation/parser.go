package aggregation

import (
	"fmt"

	"gosearch/server/mapping"
)

// ParseAggregations parses the aggregation JSON DSL into Aggregator instances.
func ParseAggregations(aggsJSON map[string]any, m *mapping.MappingDefinition) ([]Aggregator, error) {
	var aggs []Aggregator
	for name, v := range aggsJSON {
		aggDef, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("aggregation [%s] must be an object", name)
		}
		agg, err := parseAggregation(name, aggDef, m)
		if err != nil {
			return nil, err
		}
		aggs = append(aggs, agg)
	}
	return aggs, nil
}

func parseAggregation(name string, def map[string]any, _ *mapping.MappingDefinition) (Aggregator, error) {
	for aggType, v := range def {
		params, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("aggregation [%s] type [%s] must have an object body", name, aggType)
		}
		switch aggType {
		case "value_count":
			field, ok := params["field"].(string)
			if !ok {
				return nil, fmt.Errorf("aggregation [%s]: value_count requires 'field' string", name)
			}
			return NewValueCountAggregator(name, field), nil
		case "terms":
			field, ok := params["field"].(string)
			if !ok {
				return nil, fmt.Errorf("aggregation [%s]: terms requires 'field' string", name)
			}
			size := 10
			if s, ok := params["size"].(float64); ok {
				size = int(s)
			}
			return NewTermsAggregator(name, field, size), nil
		default:
			return nil, fmt.Errorf("unknown aggregation type [%s]", aggType)
		}
	}
	return nil, fmt.Errorf("aggregation [%s] has no type", name)
}
