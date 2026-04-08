package analysis

// Tokenizer splits text into a sequence of tokens.
type Tokenizer interface {
	Tokenize(text string) ([]Token, error)
}

// WhitespaceTokenizer splits text by whitespace characters.
type WhitespaceTokenizer struct{}

func NewWhitespaceTokenizer() *WhitespaceTokenizer {
	return &WhitespaceTokenizer{}
}

func (t *WhitespaceTokenizer) Tokenize(text string) ([]Token, error) {
	tokens := make([]Token, 0, len(text)/5)
	position := 0
	start := 0
	inToken := false

	for i, ch := range text {
		if isWhitespace(ch) {
			if inToken {
				tokens = append(tokens, Token{
					Term:        text[start:i],
					Position:    position,
					StartOffset: start,
					EndOffset:   i,
				})
				position++
				inToken = false
			}
		} else {
			if !inToken {
				start = i
				inToken = true
			}
		}
	}
	// last token
	if inToken {
		tokens = append(tokens, Token{
			Term:        text[start:],
			Position:    position,
			StartOffset: start,
			EndOffset:   len(text),
		})
	}

	return tokens, nil
}

func isWhitespace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}
