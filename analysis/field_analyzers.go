package analysis

// FieldAnalyzers resolves the correct analyzer for each field,
// falling back to a default analyzer.
type FieldAnalyzers struct {
	perField        map[string]*Analyzer
	defaultAnalyzer *Analyzer
}

func NewFieldAnalyzers(defaultAnalyzer *Analyzer) *FieldAnalyzers {
	return &FieldAnalyzers{
		perField:        make(map[string]*Analyzer),
		defaultAnalyzer: defaultAnalyzer,
	}
}

func (fa *FieldAnalyzers) SetFieldAnalyzer(field string, a *Analyzer) {
	fa.perField[field] = a
}

// AnalyzeField analyzes text using the field-specific analyzer, or the default.
func (fa *FieldAnalyzers) AnalyzeField(field, text string) ([]Token, error) {
	a := fa.perField[field]
	if a == nil {
		a = fa.defaultAnalyzer
	}
	return a.Analyze(text)
}
