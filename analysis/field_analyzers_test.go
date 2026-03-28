package analysis

import "testing"

func TestFieldAnalyzers_DefaultFallback(t *testing.T) {
	fa := NewFieldAnalyzers(NewAnalyzer(NewWhitespaceTokenizer(), NewLowerCaseFilter()))

	tokens, err := fa.AnalyzeField("body", "Hello World")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 || tokens[0].Term != "hello" {
		t.Errorf("expected default analyzer, got %v", tokens)
	}
}

func TestFieldAnalyzers_PerField(t *testing.T) {
	standard := NewAnalyzer(NewWhitespaceTokenizer(), NewLowerCaseFilter())
	ngram := NewAnalyzer(NewNGramTokenizer(2, 3), NewLowerCaseFilter())

	fa := NewFieldAnalyzers(standard)
	fa.SetFieldAnalyzer("title", ngram)

	// "title" should use ngram
	tokens, err := fa.AnalyzeField("title", "abc")
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"ab", "bc", "abc"}
	if len(tokens) != len(expected) {
		t.Fatalf("title: expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, tok := range tokens {
		if tok.Term != expected[i] {
			t.Errorf("title token[%d]: expected %q, got %q", i, expected[i], tok.Term)
		}
	}

	// "body" should use standard (default)
	tokens, err = fa.AnalyzeField("body", "Hello World")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 || tokens[0].Term != "hello" {
		t.Errorf("body: expected standard analyzer, got %v", tokens)
	}
}
