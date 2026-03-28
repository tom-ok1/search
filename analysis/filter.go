package analysis

import "strings"

// TokenFilter transforms a sequence of tokens.
type TokenFilter interface {
	Filter(tokens []Token) []Token
}

// LowerCaseFilter converts all token terms to lowercase.
type LowerCaseFilter struct{}

// NewLowerCaseFilter creates a new LowerCaseFilter.
func NewLowerCaseFilter() *LowerCaseFilter {
	return &LowerCaseFilter{}
}

func (f *LowerCaseFilter) Filter(tokens []Token) []Token {
	for i := range tokens {
		tokens[i].Term = strings.ToLower(tokens[i].Term)
	}
	return tokens
}
