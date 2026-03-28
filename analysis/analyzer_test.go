package analysis

import (
	"strings"
	"testing"
)

func TestAnalyzer(t *testing.T) {
	analyzer := NewAnalyzer(
		NewWhitespaceTokenizer(),
		&LowerCaseFilter{},
	)

	tokens, err := analyzer.Analyze("The Quick Brown Fox")
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{"the", "quick", "brown", "fox"}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, tok := range tokens {
		if tok.Term != expected[i] {
			t.Errorf("token[%d]: expected %q, got %q", i, expected[i], tok.Term)
		}
		if tok.Position != i {
			t.Errorf("token[%d]: expected position %d, got %d", i, i, tok.Position)
		}
	}
}

func TestWhitespaceTokenizerPositions(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	tokens, _ := tokenizer.Tokenize(strings.NewReader("hello world"))

	if tokens[0].StartOffset != 0 || tokens[0].EndOffset != 5 {
		t.Errorf("token 0 offsets: expected [0,5], got [%d,%d]",
			tokens[0].StartOffset, tokens[0].EndOffset)
	}
	if tokens[1].StartOffset != 6 || tokens[1].EndOffset != 11 {
		t.Errorf("token 1 offsets: expected [6,11], got [%d,%d]",
			tokens[1].StartOffset, tokens[1].EndOffset)
	}
}

func TestWhitespaceTokenizerJapanese(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	// "東京 大阪" = 6 bytes + 1 byte space + 6 bytes = 13 bytes
	tokens, err := tokenizer.Tokenize(strings.NewReader("東京 大阪"))
	if err != nil {
		t.Fatal(err)
	}

	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0].Term != "東京" {
		t.Errorf("token 0: expected %q, got %q", "東京", tokens[0].Term)
	}
	if tokens[0].StartOffset != 0 || tokens[0].EndOffset != 6 {
		t.Errorf("token 0 offsets: expected [0,6], got [%d,%d]",
			tokens[0].StartOffset, tokens[0].EndOffset)
	}
	if tokens[1].Term != "大阪" {
		t.Errorf("token 1: expected %q, got %q", "大阪", tokens[1].Term)
	}
	if tokens[1].StartOffset != 7 || tokens[1].EndOffset != 13 {
		t.Errorf("token 1 offsets: expected [7,13], got [%d,%d]",
			tokens[1].StartOffset, tokens[1].EndOffset)
	}
}

func TestAnalyzerJapanese(t *testing.T) {
	analyzer := NewAnalyzer(
		NewWhitespaceTokenizer(),
		&LowerCaseFilter{},
	)

	tokens, err := analyzer.Analyze("東京 大阪 名古屋")
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{"東京", "大阪", "名古屋"}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, tok := range tokens {
		if tok.Term != expected[i] {
			t.Errorf("token[%d]: expected %q, got %q", i, expected[i], tok.Term)
		}
		if tok.Position != i {
			t.Errorf("token[%d]: expected position %d, got %d", i, i, tok.Position)
		}
	}
}
