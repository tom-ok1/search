package analysis

import (
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
	tokens, _ := tokenizer.Tokenize("hello world")

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
	tokens, err := tokenizer.Tokenize("東京 大阪")
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

func TestWhitespaceTokenizerMultipleSpaces(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	tokens, err := tokenizer.Tokenize("hello   world")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0].Term != "hello" || tokens[1].Term != "world" {
		t.Errorf("unexpected terms: %q, %q", tokens[0].Term, tokens[1].Term)
	}
	if tokens[0].StartOffset != 0 || tokens[0].EndOffset != 5 {
		t.Errorf("token 0 offsets: expected [0,5], got [%d,%d]", tokens[0].StartOffset, tokens[0].EndOffset)
	}
	if tokens[1].StartOffset != 8 || tokens[1].EndOffset != 13 {
		t.Errorf("token 1 offsets: expected [8,13], got [%d,%d]", tokens[1].StartOffset, tokens[1].EndOffset)
	}
}

func TestWhitespaceTokenizerTabsAndMixed(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	tokens, err := tokenizer.Tokenize("hello\tworld\nhello")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d", len(tokens))
	}
	expected := []string{"hello", "world", "hello"}
	for i, tok := range tokens {
		if tok.Term != expected[i] {
			t.Errorf("token[%d]: expected %q, got %q", i, expected[i], tok.Term)
		}
	}
}

func TestWhitespaceTokenizerLeadingTrailing(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	tokens, err := tokenizer.Tokenize("  hello world  ")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0].Term != "hello" {
		t.Errorf("token 0: expected %q, got %q", "hello", tokens[0].Term)
	}
	if tokens[0].StartOffset != 2 || tokens[0].EndOffset != 7 {
		t.Errorf("token 0 offsets: expected [2,7], got [%d,%d]", tokens[0].StartOffset, tokens[0].EndOffset)
	}
	if tokens[1].Term != "world" {
		t.Errorf("token 1: expected %q, got %q", "world", tokens[1].Term)
	}
	if tokens[1].StartOffset != 8 || tokens[1].EndOffset != 13 {
		t.Errorf("token 1 offsets: expected [8,13], got [%d,%d]", tokens[1].StartOffset, tokens[1].EndOffset)
	}
}

func TestWhitespaceTokenizerOnlyWhitespace(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	tokens, err := tokenizer.Tokenize("   ")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 0 {
		t.Errorf("expected 0 tokens for whitespace-only input, got %d", len(tokens))
	}
}

func TestWhitespaceTokenizerEmpty(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	tokens, err := tokenizer.Tokenize("")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 0 {
		t.Errorf("expected 0 tokens for empty input, got %d", len(tokens))
	}
}

func TestWhitespaceTokenizerSingleChar(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	tokens, err := tokenizer.Tokenize("a")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].Term != "a" {
		t.Errorf("expected %q, got %q", "a", tokens[0].Term)
	}
	if tokens[0].StartOffset != 0 || tokens[0].EndOffset != 1 {
		t.Errorf("offsets: expected [0,1], got [%d,%d]", tokens[0].StartOffset, tokens[0].EndOffset)
	}
}

func TestWhitespaceTokenizerEmoji(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	// 🔍 is 4 bytes in UTF-8
	tokens, err := tokenizer.Tokenize("hello 🔍 world")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d", len(tokens))
	}
	if tokens[0].Term != "hello" {
		t.Errorf("token 0: expected %q, got %q", "hello", tokens[0].Term)
	}
	if tokens[1].Term != "🔍" {
		t.Errorf("token 1: expected %q, got %q", "🔍", tokens[1].Term)
	}
	// "hello" = 5 bytes, space = 1 byte, "🔍" starts at 6, ends at 10
	if tokens[1].StartOffset != 6 || tokens[1].EndOffset != 10 {
		t.Errorf("token 1 (emoji) offsets: expected [6,10], got [%d,%d]", tokens[1].StartOffset, tokens[1].EndOffset)
	}
	if tokens[2].Term != "world" {
		t.Errorf("token 2: expected %q, got %q", "world", tokens[2].Term)
	}
	if tokens[2].StartOffset != 11 || tokens[2].EndOffset != 16 {
		t.Errorf("token 2 offsets: expected [11,16], got [%d,%d]", tokens[2].StartOffset, tokens[2].EndOffset)
	}
}

func TestWhitespaceTokenizerSpecialChars(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()

	// Hyphens, dots, underscores should not split
	tokens, err := tokenizer.Tokenize("state-of-the-art node.js hello_world")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d", len(tokens))
	}
	expected := []string{"state-of-the-art", "node.js", "hello_world"}
	for i, tok := range tokens {
		if tok.Term != expected[i] {
			t.Errorf("token[%d]: expected %q, got %q", i, expected[i], tok.Term)
		}
	}
}

func TestWhitespaceTokenizerAtSignsAndHashes(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	tokens, err := tokenizer.Tokenize("user@example.com #tag")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0].Term != "user@example.com" {
		t.Errorf("token 0: expected %q, got %q", "user@example.com", tokens[0].Term)
	}
	if tokens[1].Term != "#tag" {
		t.Errorf("token 1: expected %q, got %q", "#tag", tokens[1].Term)
	}
}

func TestWhitespaceTokenizerSymbolsOnly(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	tokens, err := tokenizer.Tokenize("@#$%")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].Term != "@#$%" {
		t.Errorf("expected %q, got %q", "@#$%", tokens[0].Term)
	}
}

func TestWhitespaceTokenizerAccentedChars(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	// é is 2 bytes, ï is 2 bytes in UTF-8
	tokens, err := tokenizer.Tokenize("café résumé naïve")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d", len(tokens))
	}
	expected := []string{"café", "résumé", "naïve"}
	for i, tok := range tokens {
		if tok.Term != expected[i] {
			t.Errorf("token[%d]: expected %q, got %q", i, expected[i], tok.Term)
		}
	}
	// "café" = c(1) + a(1) + f(1) + é(2) = 5 bytes
	if tokens[0].StartOffset != 0 || tokens[0].EndOffset != 5 {
		t.Errorf("token 0 offsets: expected [0,5], got [%d,%d]", tokens[0].StartOffset, tokens[0].EndOffset)
	}
}

func TestWhitespaceTokenizerLongToken(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	long := "superlongwordwithoutanyspaces"
	tokens, err := tokenizer.Tokenize(long)
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].Term != long {
		t.Errorf("expected %q, got %q", long, tokens[0].Term)
	}
}

func TestWhitespaceTokenizerMixedScripts(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	tokens, err := tokenizer.Tokenize("hello 東京 world café")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 4 {
		t.Fatalf("expected 4 tokens, got %d", len(tokens))
	}
	expected := []string{"hello", "東京", "world", "café"}
	for i, tok := range tokens {
		if tok.Term != expected[i] {
			t.Errorf("token[%d]: expected %q, got %q", i, expected[i], tok.Term)
		}
	}
}

func TestWhitespaceTokenizerUnicodePunctuation(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	// 「」『』 are not whitespace, should stay as part of token
	tokens, err := tokenizer.Tokenize("「東京」の『タワー』")
	if err != nil {
		t.Fatal(err)
	}
	// No whitespace in this input, so entire string is one token
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].Term != "「東京」の『タワー』" {
		t.Errorf("expected %q, got %q", "「東京」の『タワー』", tokens[0].Term)
	}
}

func TestWhitespaceTokenizerFullWidth(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	// Each full-width char is 3 bytes in UTF-8
	tokens, err := tokenizer.Tokenize("ＨＥＬＬＯ")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].Term != "ＨＥＬＬＯ" {
		t.Errorf("expected %q, got %q", "ＨＥＬＬＯ", tokens[0].Term)
	}
	// 5 full-width chars * 3 bytes each = 15 bytes
	if tokens[0].StartOffset != 0 || tokens[0].EndOffset != 15 {
		t.Errorf("offsets: expected [0,15], got [%d,%d]", tokens[0].StartOffset, tokens[0].EndOffset)
	}
}

func TestWhitespaceTokenizerBackslashes(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	tokens, err := tokenizer.Tokenize("path\\to\\file")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].Term != "path\\to\\file" {
		t.Errorf("expected %q, got %q", "path\\to\\file", tokens[0].Term)
	}
}

func TestWhitespaceTokenizerCJKExtensionB(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	// 𠮷 is 4 bytes in UTF-8 (CJK Extension B)
	tokens, err := tokenizer.Tokenize("𠮷野家")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].Term != "𠮷野家" {
		t.Errorf("expected %q, got %q", "𠮷野家", tokens[0].Term)
	}
	// 𠮷(4) + 野(3) + 家(3) = 10 bytes
	if tokens[0].StartOffset != 0 || tokens[0].EndOffset != 10 {
		t.Errorf("offsets: expected [0,10], got [%d,%d]", tokens[0].StartOffset, tokens[0].EndOffset)
	}
}

func TestWhitespaceTokenizerSlashes(t *testing.T) {
	tokenizer := NewWhitespaceTokenizer()
	tokens, err := tokenizer.Tokenize("TCP/IP input/output")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0].Term != "TCP/IP" || tokens[1].Term != "input/output" {
		t.Errorf("unexpected terms: %q, %q", tokens[0].Term, tokens[1].Term)
	}
}

func TestLowerCaseFilterAccented(t *testing.T) {
	analyzer := NewAnalyzer(NewWhitespaceTokenizer(), NewLowerCaseFilter())
	tokens, err := analyzer.Analyze("Café RÉSUMÉ Naïve")
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"café", "résumé", "naïve"}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, tok := range tokens {
		if tok.Term != expected[i] {
			t.Errorf("token[%d]: expected %q, got %q", i, expected[i], tok.Term)
		}
	}
}

func TestLowerCaseFilterFullWidth(t *testing.T) {
	analyzer := NewAnalyzer(NewWhitespaceTokenizer(), NewLowerCaseFilter())
	tokens, err := analyzer.Analyze("ＨＥＬＬＯ")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	// strings.ToLower should convert full-width uppercase to full-width lowercase
	if tokens[0].Term != "ｈｅｌｌｏ" {
		t.Errorf("expected %q, got %q", "ｈｅｌｌｏ", tokens[0].Term)
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
