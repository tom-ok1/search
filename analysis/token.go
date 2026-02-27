package analysis

// Token represents a single searchable unit extracted from text.
type Token struct {
	// Term is the token string (after normalization).
	Term string
	// Position is the logical token position within the document (0-based).
	// Used for phrase queries.
	Position int
	// StartOffset is the start byte position in the original text.
	StartOffset int
	// EndOffset is the end byte position in the original text.
	EndOffset int
}
