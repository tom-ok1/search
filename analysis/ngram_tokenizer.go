package analysis

// NGramTokenizer generates all character n-grams of sizes minGram to maxGram.
type NGramTokenizer struct {
	minGram int
	maxGram int
}

func NewNGramTokenizer(minGram, maxGram int) *NGramTokenizer {
	return &NGramTokenizer{
		minGram: minGram,
		maxGram: maxGram,
	}
}

func (t *NGramTokenizer) Tokenize(text string) ([]Token, error) {
	runes := []rune(text)

	var tokens []Token
	position := 0

	for n := t.minGram; n <= t.maxGram; n++ {
		for i := 0; i <= len(runes)-n; i++ {
			term := string(runes[i : i+n])
			startOffset := len(string(runes[:i]))
			endOffset := len(string(runes[:i+n]))
			tokens = append(tokens, Token{
				Term:        term,
				Position:    position,
				StartOffset: startOffset,
				EndOffset:   endOffset,
			})
			position++
		}
	}

	return tokens, nil
}
