package analysis

// AnalyzerRegistry maps analyzer names to Analyzer instances.
type AnalyzerRegistry struct {
	analyzers map[string]*Analyzer
}

func NewAnalyzerRegistry() *AnalyzerRegistry {
	return &AnalyzerRegistry{
		analyzers: make(map[string]*Analyzer),
	}
}

func (r *AnalyzerRegistry) Register(name string, a *Analyzer) {
	r.analyzers[name] = a
}

func (r *AnalyzerRegistry) Get(name string) *Analyzer {
	return r.analyzers[name]
}

// DefaultRegistry returns a registry pre-loaded with built-in analyzers.
func DefaultRegistry() *AnalyzerRegistry {
	r := NewAnalyzerRegistry()
	r.Register("standard", NewAnalyzer(NewWhitespaceTokenizer(), NewLowerCaseFilter()))
	r.Register("ngram", NewAnalyzer(NewNGramTokenizer(2, 3), NewLowerCaseFilter()))
	return r
}
