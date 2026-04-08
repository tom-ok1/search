package analysis

// Analyzer combines a Tokenizer and a chain of TokenFilters into a pipeline.
type Analyzer struct {
	tokenizer Tokenizer
	filters   []TokenFilter
}

func NewAnalyzer(tokenizer Tokenizer, filters ...TokenFilter) *Analyzer {
	return &Analyzer{
		tokenizer: tokenizer,
		filters:   filters,
	}
}

// Analyze converts text into a sequence of tokens by running the full pipeline.
func (a *Analyzer) Analyze(text string) ([]Token, error) {
	tokens, err := a.tokenizer.Tokenize(text)
	if err != nil {
		return nil, err
	}
	for _, filter := range a.filters {
		tokens = filter.Filter(tokens)
	}
	return tokens, nil
}
