package action

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"gosearch/analysis"
	"gosearch/search"
	"gosearch/server/mapping"
)

// QueryJSON is a discriminated union for ES query JSON. Exactly one field is non-nil.
type QueryJSON struct {
	MatchAll    *MatchAllQueryJSON
	Term        *TermQueryJSON
	Match       *MatchQueryJSON
	MatchPhrase *MatchPhraseQueryJSON
	Exists      *ExistsQueryJSON
	MultiMatch  *MultiMatchQueryJSON
	Bool        *BoolQueryJSON
	Range       *RangeQueryJSON
}

type MatchAllQueryJSON struct{}

type TermQueryJSON struct {
	Field string
	Value string
}

type MatchQueryJSON struct {
	Field    string
	Text     string
	Analyzer string
}

type MatchPhraseQueryJSON struct {
	Field    string
	Text     string
	Analyzer string
}

type ExistsQueryJSON struct {
	Field string
}

type MultiMatchQueryJSON struct {
	Query  string
	Fields []string
}

type BoolQueryJSON struct {
	Must    []QueryJSON
	Filter  []QueryJSON
	Should  []QueryJSON
	MustNot []QueryJSON
}

type RangeQueryJSON struct {
	Field string
	GTE   any
	GT    any
	LTE   any
	LT    any
}

func (q *QueryJSON) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("query must be a JSON object: %w", err)
	}
	if len(raw) != 1 {
		return fmt.Errorf("query must have exactly one top-level key, got %d", len(raw))
	}

	for key, value := range raw {
		switch key {
		case "match_all":
			q.MatchAll = &MatchAllQueryJSON{}
			return nil
		case "term":
			return q.unmarshalTerm(value)
		case "match":
			return q.unmarshalMatch(value)
		case "match_phrase":
			return q.unmarshalMatchPhrase(value)
		case "exists":
			return q.unmarshalExists(value)
		case "multi_match":
			return q.unmarshalMultiMatch(value)
		case "bool":
			return q.unmarshalBool(value)
		case "range":
			return q.unmarshalRange(value)
		default:
			return fmt.Errorf("unknown query type [%s]", key)
		}
	}
	return fmt.Errorf("empty query")
}

func (q *QueryJSON) unmarshalTerm(data json.RawMessage) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("term query must be an object")
	}
	for field, raw := range fields {
		var scalar any
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&scalar); err != nil {
			return fmt.Errorf("term query value for [%s]: %w", field, err)
		}
		switch scalar.(type) {
		case string, json.Number, bool:
			q.Term = &TermQueryJSON{Field: field, Value: fmt.Sprintf("%v", scalar)}
			return nil
		default:
			return fmt.Errorf("term query value for [%s] must be a scalar (string, number, or bool)", field)
		}
	}
	return fmt.Errorf("term query must specify a field")
}

func (q *QueryJSON) unmarshalMatch(data json.RawMessage) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("match query must be an object")
	}
	for field, raw := range fields {
		text, analyzer, err := unmarshalMatchParams(raw)
		if err != nil {
			return fmt.Errorf("match query field [%s]: %w", field, err)
		}
		q.Match = &MatchQueryJSON{Field: field, Text: text, Analyzer: analyzer}
		return nil
	}
	return fmt.Errorf("match query must specify a field")
}

func (q *QueryJSON) unmarshalMatchPhrase(data json.RawMessage) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("match_phrase query must be an object")
	}
	for field, raw := range fields {
		text, analyzer, err := unmarshalMatchParams(raw)
		if err != nil {
			return fmt.Errorf("match_phrase query field [%s]: %w", field, err)
		}
		q.MatchPhrase = &MatchPhraseQueryJSON{Field: field, Text: text, Analyzer: analyzer}
		return nil
	}
	return fmt.Errorf("match_phrase query must specify a field")
}

// unmarshalMatchParams handles both scalar form ("hello") and object form
// ({"query": "hello", "analyzer": "custom"}) for match/match_phrase fields.
func unmarshalMatchParams(data json.RawMessage) (text string, analyzer string, err error) {
	// Try object form first: {"query": "...", "analyzer": "..."}
	var obj struct {
		Query    any    `json:"query"`
		Analyzer string `json:"analyzer"`
	}
	if err := json.Unmarshal(data, &obj); err == nil && obj.Query != nil {
		return fmt.Sprintf("%v", obj.Query), obj.Analyzer, nil
	}

	// Scalar form: "hello" or 42 or true
	var scalar any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&scalar); err != nil {
		return "", "", fmt.Errorf("field value must be a string, number, bool, or object with 'query'")
	}
	switch scalar.(type) {
	case string, json.Number, bool:
		return fmt.Sprintf("%v", scalar), "", nil
	default:
		return "", "", fmt.Errorf("field value must be a string, number, bool, or object with 'query'")
	}
}

func (q *QueryJSON) unmarshalExists(data json.RawMessage) error {
	var obj struct {
		Field string `json:"field"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("exists query must be an object")
	}
	if obj.Field == "" {
		return fmt.Errorf("exists query must specify 'field'")
	}
	q.Exists = &ExistsQueryJSON{Field: obj.Field}
	return nil
}

func (q *QueryJSON) unmarshalMultiMatch(data json.RawMessage) error {
	var obj struct {
		Query  string   `json:"query"`
		Fields []string `json:"fields"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("multi_match query must be an object: %w", err)
	}
	if obj.Query == "" {
		var raw map[string]json.RawMessage
		json.Unmarshal(data, &raw)
		if _, ok := raw["query"]; !ok {
			return fmt.Errorf("multi_match query must specify 'query'")
		}
	}
	if obj.Fields == nil {
		return fmt.Errorf("multi_match query must specify 'fields'")
	}
	if len(obj.Fields) == 0 {
		return fmt.Errorf("multi_match 'fields' must not be empty")
	}
	q.MultiMatch = &MultiMatchQueryJSON{Query: obj.Query, Fields: obj.Fields}
	return nil
}

func (q *QueryJSON) unmarshalBool(data json.RawMessage) error {
	var obj struct {
		Must    []QueryJSON `json:"must"`
		Filter  []QueryJSON `json:"filter"`
		Should  []QueryJSON `json:"should"`
		MustNot []QueryJSON `json:"must_not"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("bool query: %w", err)
	}
	total := len(obj.Must) + len(obj.Filter) + len(obj.Should) + len(obj.MustNot)
	if total == 0 {
		var raw map[string]json.RawMessage
		json.Unmarshal(data, &raw)
		keys := make([]string, 0, len(raw))
		for k := range raw {
			keys = append(keys, k)
		}
		return fmt.Errorf("bool query has no recognized clauses, got: [%s]", strings.Join(keys, ", "))
	}
	q.Bool = &BoolQueryJSON{
		Must:    obj.Must,
		Filter:  obj.Filter,
		Should:  obj.Should,
		MustNot: obj.MustNot,
	}
	return nil
}

func (q *QueryJSON) unmarshalRange(data json.RawMessage) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("range query must be an object")
	}
	for field, raw := range fields {
		var params struct {
			GTE any `json:"gte"`
			GT  any `json:"gt"`
			LTE any `json:"lte"`
			LT  any `json:"lt"`
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&params); err != nil {
			return fmt.Errorf("range query value for [%s] must be an object", field)
		}
		q.Range = &RangeQueryJSON{
			Field: field,
			GTE:   params.GTE,
			GT:    params.GT,
			LTE:   params.LTE,
			LT:    params.LT,
		}
		return nil
	}
	return fmt.Errorf("range query must specify a field")
}

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

func (p *QueryParser) ParseQuery(q QueryJSON) (search.Query, error) {
	switch {
	case q.MatchAll != nil:
		return search.NewMatchAllQuery(), nil
	case q.Term != nil:
		return p.parseTerm(q.Term)
	case q.Match != nil:
		return p.parseMatch(q.Match)
	case q.MatchPhrase != nil:
		return p.parseMatchPhrase(q.MatchPhrase)
	case q.Exists != nil:
		return p.parseExists(q.Exists)
	case q.MultiMatch != nil:
		return p.parseMultiMatch(q.MultiMatch)
	case q.Bool != nil:
		return p.parseBool(q.Bool)
	case q.Range != nil:
		return p.parseRange(q.Range)
	default:
		return nil, fmt.Errorf("empty query")
	}
}

func (p *QueryParser) parseTerm(t *TermQueryJSON) (search.Query, error) {
	return search.NewTermQuery(t.Field, t.Value), nil
}

func (p *QueryParser) parseMatch(m *MatchQueryJSON) (search.Query, error) {
	analyzerName := "standard"
	if m.Analyzer != "" {
		analyzerName = m.Analyzer
	} else if fm, exists := p.mapping.Properties[m.Field]; exists && fm.Analyzer != "" {
		analyzerName = fm.Analyzer
	}

	analyzer := p.registry.Get(analyzerName)
	if analyzer == nil {
		return nil, fmt.Errorf("unknown analyzer [%s]", analyzerName)
	}

	tokens, err := analyzer.Analyze(m.Text)
	if err != nil {
		return nil, fmt.Errorf("analyze field [%s]: %w", m.Field, err)
	}

	if len(tokens) == 0 {
		return search.NewMatchNoneQuery(), nil
	}
	if len(tokens) == 1 {
		return search.NewTermQuery(m.Field, tokens[0].Term), nil
	}

	bq := search.NewBooleanQuery()
	for _, token := range tokens {
		bq.Add(search.NewTermQuery(m.Field, token.Term), search.OccurShould)
	}
	return bq, nil
}

func (p *QueryParser) parseMultiMatch(mm *MultiMatchQueryJSON) (search.Query, error) {
	bq := search.NewBooleanQuery()
	for _, fieldName := range mm.Fields {
		subQuery, err := p.parseMatch(&MatchQueryJSON{Field: fieldName, Text: mm.Query})
		if err != nil {
			return nil, fmt.Errorf("multi_match field [%s]: %w", fieldName, err)
		}
		bq.Add(subQuery, search.OccurShould)
	}
	return bq, nil
}

func (p *QueryParser) parseExists(e *ExistsQueryJSON) (search.Query, error) {
	fm, exists := p.mapping.Properties[e.Field]
	if !exists {
		return search.NewMatchNoneQuery(), nil
	}

	switch fm.Type {
	case mapping.FieldTypeText, mapping.FieldTypeKeyword, mapping.FieldTypeBoolean,
		mapping.FieldTypeLong, mapping.FieldTypeDouble:
		return search.NewFieldExistsQuery(e.Field), nil
	default:
		return search.NewMatchNoneQuery(), nil
	}
}

func (p *QueryParser) parseMatchPhrase(mp *MatchPhraseQueryJSON) (search.Query, error) {
	analyzerName := "standard"
	if mp.Analyzer != "" {
		analyzerName = mp.Analyzer
	} else if fm, exists := p.mapping.Properties[mp.Field]; exists && fm.Analyzer != "" {
		analyzerName = fm.Analyzer
	}

	analyzer := p.registry.Get(analyzerName)
	if analyzer == nil {
		return nil, fmt.Errorf("unknown analyzer [%s]", analyzerName)
	}

	tokens, err := analyzer.Analyze(mp.Text)
	if err != nil {
		return nil, fmt.Errorf("analyze field [%s]: %w", mp.Field, err)
	}

	if len(tokens) == 0 {
		return search.NewMatchNoneQuery(), nil
	}

	terms := make([]string, len(tokens))
	for i, token := range tokens {
		terms[i] = token.Term
	}
	return search.NewPhraseQuery(mp.Field, terms...), nil
}

func (p *QueryParser) parseBool(b *BoolQueryJSON) (search.Query, error) {
	bq := search.NewBooleanQuery()

	clauseGroups := []struct {
		clauses []QueryJSON
		occur   search.Occur
		name    string
	}{
		{b.Must, search.OccurMust, "must"},
		{b.Filter, search.OccurFilter, "filter"},
		{b.Should, search.OccurShould, "should"},
		{b.MustNot, search.OccurMustNot, "must_not"},
	}

	for _, cg := range clauseGroups {
		for _, clause := range cg.clauses {
			subQuery, err := p.ParseQuery(clause)
			if err != nil {
				return nil, fmt.Errorf("parse %s clause: %w", cg.name, err)
			}
			bq.Add(subQuery, cg.occur)
		}
	}

	if len(bq.Clauses) == 0 {
		return nil, fmt.Errorf("bool query has no recognized clauses")
	}

	return bq, nil
}

func (p *QueryParser) parseRange(r *RangeQueryJSON) (search.Query, error) {
	fm, exists := p.mapping.Properties[r.Field]
	if !exists {
		return nil, fmt.Errorf("unknown field [%s] in range query", r.Field)
	}

	params := map[string]any{}
	if r.GTE != nil {
		params["gte"] = r.GTE
	}
	if r.GT != nil {
		params["gt"] = r.GT
	}
	if r.LTE != nil {
		params["lte"] = r.LTE
	}
	if r.LT != nil {
		params["lt"] = r.LT
	}

	switch fm.Type {
	case mapping.FieldTypeLong:
		return p.parseLongRange(r.Field, params)
	case mapping.FieldTypeDouble:
		return p.parseDoubleRange(r.Field, params)
	default:
		return nil, fmt.Errorf("range query not supported for field type [%s]", fm.Type)
	}
}

func (p *QueryParser) parseLongRange(field string, params map[string]any) (search.Query, error) {
	minVal := int64(math.MinInt64)
	maxVal := int64(math.MaxInt64)

	if v, ok := params["gte"]; ok {
		n, err := rangeToInt64(v)
		if err != nil {
			return nil, fmt.Errorf("range gte: %w", err)
		}
		minVal = n
	}
	if v, ok := params["gt"]; ok {
		n, err := rangeToInt64(v)
		if err != nil {
			return nil, fmt.Errorf("range gt: %w", err)
		}
		if n == math.MaxInt64 {
			return search.NewMatchNoneQuery(), nil
		}
		minVal = n + 1
	}
	if v, ok := params["lte"]; ok {
		n, err := rangeToInt64(v)
		if err != nil {
			return nil, fmt.Errorf("range lte: %w", err)
		}
		maxVal = n
	}
	if v, ok := params["lt"]; ok {
		n, err := rangeToInt64(v)
		if err != nil {
			return nil, fmt.Errorf("range lt: %w", err)
		}
		if n == math.MinInt64 {
			return search.NewMatchNoneQuery(), nil
		}
		maxVal = n - 1
	}

	return search.NewPointRangeQuery(field, minVal, maxVal), nil
}

func (p *QueryParser) parseDoubleRange(field string, params map[string]any) (search.Query, error) {
	minVal := -math.MaxFloat64
	maxVal := math.MaxFloat64

	if v, ok := params["gte"]; ok {
		f, err := rangeToFloat64(v)
		if err != nil {
			return nil, fmt.Errorf("range gte: %w", err)
		}
		minVal = f
	}
	if v, ok := params["gt"]; ok {
		f, err := rangeToFloat64(v)
		if err != nil {
			return nil, fmt.Errorf("range gt: %w", err)
		}
		minVal = math.Nextafter(f, math.Inf(1))
	}
	if v, ok := params["lte"]; ok {
		f, err := rangeToFloat64(v)
		if err != nil {
			return nil, fmt.Errorf("range lte: %w", err)
		}
		maxVal = f
	}
	if v, ok := params["lt"]; ok {
		f, err := rangeToFloat64(v)
		if err != nil {
			return nil, fmt.Errorf("range lt: %w", err)
		}
		maxVal = math.Nextafter(f, math.Inf(-1))
	}

	return search.NewDoublePointRangeQuery(field, minVal, maxVal), nil
}

func rangeToInt64(v any) (int64, error) {
	switch val := v.(type) {
	case json.Number:
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

func rangeToFloat64(v any) (float64, error) {
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
