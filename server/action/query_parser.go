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

func (p *QueryParser) parseMatch(value any) (search.Query, error) {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("match query must be an object")
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

func (p *QueryParser) parseBool(value any) (search.Query, error) {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("bool query must be an object")
	}

	bq := search.NewBooleanQuery()

	clauseTypes := map[string]search.Occur{
		"must":     search.OccurMust,
		"filter":   search.OccurMust,
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
