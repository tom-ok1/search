package analysis

import "testing"

func TestAnalyzerRegistry(t *testing.T) {
	r := NewAnalyzerRegistry()
	a := NewAnalyzer(NewWhitespaceTokenizer(), NewLowerCaseFilter())
	r.Register("test", a)

	got := r.Get("test")
	if got != a {
		t.Error("expected registered analyzer")
	}
	if r.Get("nonexistent") != nil {
		t.Error("expected nil for unregistered analyzer")
	}
}

func TestDefaultRegistry(t *testing.T) {
	r := DefaultRegistry()

	if r.Get("standard") == nil {
		t.Error("expected standard analyzer")
	}
	if r.Get("ngram") == nil {
		t.Error("expected ngram analyzer")
	}

	// Verify standard analyzer works
	tokens, err := r.Get("standard").Analyze("Hello World")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 || tokens[0].Term != "hello" {
		t.Errorf("standard analyzer: unexpected tokens %v", tokens)
	}

	// Verify ngram analyzer works
	tokens, err = r.Get("ngram").Analyze("AB")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 || tokens[0].Term != "ab" {
		t.Errorf("ngram analyzer: unexpected tokens %v", tokens)
	}
}
