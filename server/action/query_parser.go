package action

import (
	"encoding/json"
	"fmt"
	"strings"

	"gosearch/analysis"
	"gosearch/search"
	"gosearch/server/mapping"
)

type QueryParser struct {
	mapping  *mapping.MappingDefinition
	registry *analysis.AnalyzerRegistry
}

func NewQueryParser(m *mapping.MappingDefinition, registry *analysis.AnalyzerRegistry) *QueryParser {
	return &QueryParser{
		mapping:  m,
		registry: registry,
	}
}

func (p *QueryParser) ParseQuery(queryJSON map[string]any) (search.Query, error) {
	if len(queryJSON) != 1 {
		return nil, fmt.Errorf("query must have exactly one top-level key")
	}

	for key, value := range queryJSON {
		switch key {
		case "match_all":
			return search.NewMatchAllQuery(), nil
		case "term":
			return p.parseTerm(value)
		case "match":
			return p.parseMatch(value)
		case "match_phrase":
			return p.parseMatchPhrase(value)
		case "exists":
			return p.parseExists(value)
		case "multi_match":
			return p.parseMultiMatch(value)
		case "bool":
			return p.parseBool(value)
		default:
			return nil, fmt.Errorf("unknown query type [%s]", key)
		}
	}

	return nil, fmt.Errorf("empty query")
}

func (p *QueryParser) parseTerm(value any) (search.Query, error) {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("term query must be an object")
	}

	for field, v := range obj {
		switch v.(type) {
		case string, float64, bool, json.Number:
			return search.NewTermQuery(field, fmt.Sprintf("%v", v)), nil
		default:
			return nil, fmt.Errorf("term query value for [%s] must be a scalar (string, number, or bool)", field)
		}
	}
	return nil, fmt.Errorf("term query must specify a field")
}

// extractMatchParams extracts query text and optional analyzer override from
// a match/match_phrase field value. ES accepts both scalar form ("hello") and
// object form ({"query": "hello", "analyzer": "custom"}).
func extractMatchParams(v any) (text string, analyzerOverride string, err error) {
	switch val := v.(type) {
	case map[string]any:
		q, ok := val["query"]
		if !ok {
			return "", "", fmt.Errorf("object-form value must contain 'query'")
		}
		text = fmt.Sprintf("%v", q)
		if a, ok := val["analyzer"]; ok {
			s, ok := a.(string)
			if !ok {
				return "", "", fmt.Errorf("'analyzer' must be a string")
			}
			analyzerOverride = s
		}
		return text, analyzerOverride, nil
	case string, float64, bool, json.Number:
		return fmt.Sprintf("%v", val), "", nil
	default:
		return "", "", fmt.Errorf("field value must be a string, number, bool, or object with 'query'")
	}
}

func (p *QueryParser) parseMatch(value any) (search.Query, error) {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("match query must be an object")
	}

	for field, v := range obj {
		text, analyzerOverride, err := extractMatchParams(v)
		if err != nil {
			return nil, fmt.Errorf("match query field [%s]: %w", field, err)
		}

		analyzerName := "standard"
		if analyzerOverride != "" {
			analyzerName = analyzerOverride
		} else if fm, exists := p.mapping.Properties[field]; exists && fm.Analyzer != "" {
			analyzerName = fm.Analyzer
		}

		analyzer := p.registry.Get(analyzerName)
		if analyzer == nil {
			return nil, fmt.Errorf("unknown analyzer [%s]", analyzerName)
		}

		tokens, err := analyzer.Analyze(text)
		if err != nil {
			return nil, fmt.Errorf("analyze field [%s]: %w", field, err)
		}

		if len(tokens) == 0 {
			return search.NewMatchNoneQuery(), nil
		}
		if len(tokens) == 1 {
			return search.NewTermQuery(field, tokens[0].Term), nil
		}

		bq := search.NewBooleanQuery()
		for _, token := range tokens {
			bq.Add(search.NewTermQuery(field, token.Term), search.OccurShould)
		}
		return bq, nil
	}
	return nil, fmt.Errorf("match query must specify a field")
}

func (p *QueryParser) parseMultiMatch(value any) (search.Query, error) {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("multi_match query must be an object")
	}

	queryText, ok := obj["query"]
	if !ok {
		return nil, fmt.Errorf("multi_match query must specify 'query'")
	}

	fieldsRaw, ok := obj["fields"]
	if !ok {
		return nil, fmt.Errorf("multi_match query must specify 'fields'")
	}

	fieldsArr, ok := fieldsRaw.([]any)
	if !ok {
		return nil, fmt.Errorf("multi_match 'fields' must be an array")
	}

	if len(fieldsArr) == 0 {
		return nil, fmt.Errorf("multi_match 'fields' must not be empty")
	}

	bq := search.NewBooleanQuery()

	for _, f := range fieldsArr {
		fieldName, ok := f.(string)
		if !ok {
			return nil, fmt.Errorf("multi_match field name must be a string")
		}

		matchObj := map[string]any{
			"match": map[string]any{
				fieldName: queryText,
			},
		}
		subQuery, err := p.ParseQuery(matchObj)
		if err != nil {
			return nil, fmt.Errorf("multi_match field [%s]: %w", fieldName, err)
		}
		bq.Add(subQuery, search.OccurShould)
	}

	return bq, nil
}

func (p *QueryParser) parseExists(value any) (search.Query, error) {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("exists query must be an object")
	}

	field, ok := obj["field"]
	if !ok {
		return nil, fmt.Errorf("exists query must specify 'field'")
	}

	fieldName, ok := field.(string)
	if !ok {
		return nil, fmt.Errorf("exists query 'field' must be a string")
	}

	return search.NewTermQuery("_field_names", fieldName), nil
}

func (p *QueryParser) parseMatchPhrase(value any) (search.Query, error) {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("match_phrase query must be an object")
	}

	for field, v := range obj {
		text := fmt.Sprintf("%v", v)

		analyzerName := "standard"
		if fm, exists := p.mapping.Properties[field]; exists && fm.Analyzer != "" {
			analyzerName = fm.Analyzer
		}

		analyzer := p.registry.Get(analyzerName)
		if analyzer == nil {
			return nil, fmt.Errorf("unknown analyzer [%s]", analyzerName)
		}

		tokens, err := analyzer.Analyze(text)
		if err != nil {
			return nil, fmt.Errorf("analyze field [%s]: %w", field, err)
		}

		if len(tokens) == 0 {
			return search.NewMatchNoneQuery(), nil
		}

		terms := make([]string, len(tokens))
		for i, token := range tokens {
			terms[i] = token.Term
		}
		return search.NewPhraseQuery(field, terms...), nil
	}
	return nil, fmt.Errorf("match_phrase query must specify a field")
}

func (p *QueryParser) parseBool(value any) (search.Query, error) {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("bool query must be an object")
	}

	bq := search.NewBooleanQuery()

	clauseTypes := map[string]search.Occur{
		"must":     search.OccurMust,
		"filter":   search.OccurFilter,
		"should":   search.OccurShould,
		"must_not": search.OccurMustNot,
	}

	for clauseName, occur := range clauseTypes {
		clauses, exists := obj[clauseName]
		if !exists {
			continue
		}
		clauseList, ok := clauses.([]any)
		if !ok {
			return nil, fmt.Errorf("bool %s must be an array", clauseName)
		}
		for _, clause := range clauseList {
			clauseObj, ok := clause.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("each bool clause must be an object")
			}
			subQuery, err := p.ParseQuery(clauseObj)
			if err != nil {
				return nil, fmt.Errorf("parse %s clause: %w", clauseName, err)
			}
			bq.Add(subQuery, occur)
		}
	}

	if len(bq.Clauses) == 0 {
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		return nil, fmt.Errorf("bool query has no recognized clauses, got: [%s]", strings.Join(keys, ", "))
	}

	return bq, nil
}
