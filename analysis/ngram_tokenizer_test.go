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

func TestNGramTokenizerExactMinGram(t *testing.T) {
	tokenizer := NewNGramTokenizer(2, 3)
	tokens, err := tokenizer.Tokenize(strings.NewReader("ab"))
	if err != nil {
		t.Fatal(err)
	}
	// Input length == minGram: should produce exactly one token
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].Term != "ab" {
		t.Errorf("expected %q, got %q", "ab", tokens[0].Term)
	}
	if tokens[0].StartOffset != 0 || tokens[0].EndOffset != 2 {
		t.Errorf("offsets: expected [0,2], got [%d,%d]", tokens[0].StartOffset, tokens[0].EndOffset)
	}
}

func TestNGramTokenizerMixedByteWidth(t *testing.T) {
	tokenizer := NewNGramTokenizer(2, 2)
	// "aあ🔍" = a(1 byte) + あ(3 bytes) + 🔍(4 bytes) = 8 bytes, 3 runes
	tokens, err := tokenizer.Tokenize(strings.NewReader("aあ🔍"))
	if err != nil {
		t.Fatal(err)
	}
	// bigrams: "aあ", "あ🔍"
	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0].Term != "aあ" {
		t.Errorf("token 0: expected %q, got %q", "aあ", tokens[0].Term)
	}
	// "aあ" = 1 + 3 = 4 bytes
	if tokens[0].StartOffset != 0 || tokens[0].EndOffset != 4 {
		t.Errorf("token 0 offsets: expected [0,4], got [%d,%d]", tokens[0].StartOffset, tokens[0].EndOffset)
	}
	if tokens[1].Term != "あ🔍" {
		t.Errorf("token 1: expected %q, got %q", "あ🔍", tokens[1].Term)
	}
	// "あ🔍" starts at byte 1, ends at byte 8
	if tokens[1].StartOffset != 1 || tokens[1].EndOffset != 8 {
		t.Errorf("token 1 offsets: expected [1,8], got [%d,%d]", tokens[1].StartOffset, tokens[1].EndOffset)
	}
}

func TestNGramTokenizerEmojiOffsets(t *testing.T) {
	tokenizer := NewNGramTokenizer(2, 2)
	// "🔍🔎" = 4 + 4 = 8 bytes, 2 runes
	tokens, err := tokenizer.Tokenize(strings.NewReader("🔍🔎"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].Term != "🔍🔎" {
		t.Errorf("expected %q, got %q", "🔍🔎", tokens[0].Term)
	}
	if tokens[0].StartOffset != 0 || tokens[0].EndOffset != 8 {
		t.Errorf("offsets: expected [0,8], got [%d,%d]", tokens[0].StartOffset, tokens[0].EndOffset)
	}
}

func TestNGramTokenizerCJKExtensionB(t *testing.T) {
	tokenizer := NewNGramTokenizer(2, 2)
	// 𠮷(4 bytes) + 野(3 bytes) + 家(3 bytes) = 10 bytes, 3 runes
	tokens, err := tokenizer.Tokenize(strings.NewReader("𠮷野家"))
	if err != nil {
		t.Fatal(err)
	}
	// bigrams: "𠮷野", "野家"
	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0].Term != "𠮷野" {
		t.Errorf("token 0: expected %q, got %q", "𠮷野", tokens[0].Term)
	}
	// "𠮷野" = 4 + 3 = 7 bytes
	if tokens[0].StartOffset != 0 || tokens[0].EndOffset != 7 {
		t.Errorf("token 0 offsets: expected [0,7], got [%d,%d]", tokens[0].StartOffset, tokens[0].EndOffset)
	}
	if tokens[1].Term != "野家" {
		t.Errorf("token 1: expected %q, got %q", "野家", tokens[1].Term)
	}
	// "野家" starts at byte 4, ends at byte 10
	if tokens[1].StartOffset != 4 || tokens[1].EndOffset != 10 {
		t.Errorf("token 1 offsets: expected [4,10], got [%d,%d]", tokens[1].StartOffset, tokens[1].EndOffset)
	}
}

func TestNGramTokenizerWhitespaceInInput(t *testing.T) {
	tokenizer := NewNGramTokenizer(2, 2)
	// NGram tokenizer doesn't split on whitespace - it generates ngrams of the whole input
	tokens, err := tokenizer.Tokenize(strings.NewReader("a b"))
	if err != nil {
		t.Fatal(err)
	}
	// "a b" = 3 runes, bigrams: "a ", " b"
	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0].Term != "a " {
		t.Errorf("token 0: expected %q, got %q", "a ", tokens[0].Term)
	}
	if tokens[1].Term != " b" {
		t.Errorf("token 1: expected %q, got %q", " b", tokens[1].Term)
	}
}

func TestNGramTokenizerSpecialChars(t *testing.T) {
	tokenizer := NewNGramTokenizer(2, 2)
	tokens, err := tokenizer.Tokenize(strings.NewReader("@#$"))
	if err != nil {
		t.Fatal(err)
	}
	// bigrams: "@#", "#$"
	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0].Term != "@#" || tokens[1].Term != "#$" {
		t.Errorf("unexpected terms: %q, %q", tokens[0].Term, tokens[1].Term)
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
