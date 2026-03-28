package analysis

import (
	"strings"
	"testing"
)

func TestNGramTokenizer(t *testing.T) {
	tokenizer := NewNGramTokenizer(2, 3)
	tokens, err := tokenizer.Tokenize(strings.NewReader("abc"))
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{"ab", "bc", "abc"}
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

func TestNGramTokenizerOffsets(t *testing.T) {
	tokenizer := NewNGramTokenizer(2, 2)
	tokens, err := tokenizer.Tokenize(strings.NewReader("abcd"))
	if err != nil {
		t.Fatal(err)
	}

	// "ab", "bc", "cd"
	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d", len(tokens))
	}
	if tokens[0].StartOffset != 0 || tokens[0].EndOffset != 2 {
		t.Errorf("token 0 offsets: expected [0,2], got [%d,%d]", tokens[0].StartOffset, tokens[0].EndOffset)
	}
	if tokens[1].StartOffset != 1 || tokens[1].EndOffset != 3 {
		t.Errorf("token 1 offsets: expected [1,3], got [%d,%d]", tokens[1].StartOffset, tokens[1].EndOffset)
	}
	if tokens[2].StartOffset != 2 || tokens[2].EndOffset != 4 {
		t.Errorf("token 2 offsets: expected [2,4], got [%d,%d]", tokens[2].StartOffset, tokens[2].EndOffset)
	}
}

func TestNGramTokenizerShortInput(t *testing.T) {
	tokenizer := NewNGramTokenizer(2, 3)
	tokens, err := tokenizer.Tokenize(strings.NewReader("a"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 0 {
		t.Errorf("expected 0 tokens for input shorter than minGram, got %d", len(tokens))
	}
}

func TestNGramTokenizerEmpty(t *testing.T) {
	tokenizer := NewNGramTokenizer(2, 3)
	tokens, err := tokenizer.Tokenize(strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 0 {
		t.Errorf("expected 0 tokens for empty input, got %d", len(tokens))
	}
}

func TestNGramTokenizerJapanese(t *testing.T) {
	tokenizer := NewNGramTokenizer(2, 3)
	tokens, err := tokenizer.Tokenize(strings.NewReader("東京都"))
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{"東京", "京都", "東京都"}
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

func TestNGramTokenizerJapaneseOffsets(t *testing.T) {
	tokenizer := NewNGramTokenizer(2, 2)
	// Each Japanese character is 3 bytes in UTF-8
	// "東京都" = 9 bytes total
	tokens, err := tokenizer.Tokenize(strings.NewReader("東京都"))
	if err != nil {
		t.Fatal(err)
	}

	// bigrams: "東京", "京都"
	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}
	// "東京": bytes [0, 6)
	if tokens[0].Term != "東京" {
		t.Errorf("token 0: expected %q, got %q", "東京", tokens[0].Term)
	}
	if tokens[0].StartOffset != 0 || tokens[0].EndOffset != 6 {
		t.Errorf("token 0 offsets: expected [0,6], got [%d,%d]", tokens[0].StartOffset, tokens[0].EndOffset)
	}
	// "京都": bytes [3, 9)
	if tokens[1].Term != "京都" {
		t.Errorf("token 1: expected %q, got %q", "京都", tokens[1].Term)
	}
	if tokens[1].StartOffset != 3 || tokens[1].EndOffset != 9 {
		t.Errorf("token 1 offsets: expected [3,9], got [%d,%d]", tokens[1].StartOffset, tokens[1].EndOffset)
	}
}
