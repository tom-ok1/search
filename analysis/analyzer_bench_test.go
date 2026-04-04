package analysis

import (
	"strings"
	"testing"
)

var sinkTokens []Token

func BenchmarkWhitespaceTokenizer(b *testing.B) {
	tokenizer := NewWhitespaceTokenizer()
	text := strings.Repeat("the quick brown fox jumps over the lazy dog ", 100)

	b.ReportAllocs()
	for b.Loop() {
		tokens, err := tokenizer.Tokenize(strings.NewReader(text))
		if err != nil {
			b.Fatal(err)
		}
		sinkTokens = tokens
	}
}

func BenchmarkNGramTokenizer(b *testing.B) {
	tokenizer := NewNGramTokenizer(2, 3)
	text := "the quick brown fox jumps over the lazy dog"

	b.ReportAllocs()
	for b.Loop() {
		tokens, err := tokenizer.Tokenize(strings.NewReader(text))
		if err != nil {
			b.Fatal(err)
		}
		sinkTokens = tokens
	}
}

func BenchmarkLowerCaseFilter(b *testing.B) {
	tokenizer := NewWhitespaceTokenizer()
	filter := NewLowerCaseFilter()
	text := "The Quick Brown Fox Jumps Over The Lazy Dog"
	tokens, err := tokenizer.Tokenize(strings.NewReader(text))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		input := make([]Token, len(tokens))
		copy(input, tokens)
		sinkTokens = filter.Filter(input)
	}
}

func BenchmarkAnalyzer(b *testing.B) {
	analyzer := NewAnalyzer(NewWhitespaceTokenizer(), NewLowerCaseFilter())

	b.Run("Short", func(b *testing.B) {
		text := "The Quick Brown Fox"
		b.ReportAllocs()
		for b.Loop() {
			tokens, err := analyzer.Analyze(text)
			if err != nil {
				b.Fatal(err)
			}
			sinkTokens = tokens
		}
	})

	b.Run("Long", func(b *testing.B) {
		text := strings.Repeat("The Quick Brown Fox Jumps Over The Lazy Dog ", 100)
		b.ReportAllocs()
		for b.Loop() {
			tokens, err := analyzer.Analyze(text)
			if err != nil {
				b.Fatal(err)
			}
			sinkTokens = tokens
		}
	})
}
