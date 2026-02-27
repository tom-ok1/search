package index

import (
	"gosearch/analysis"
	"gosearch/document"
)

// FieldIndex is the inverted index for a single field.
// It maps term to PostingsList.
type FieldIndex struct {
	postings map[string]*PostingsList
}

func newFieldIndex() *FieldIndex {
	return &FieldIndex{
		postings: make(map[string]*PostingsList),
	}
}

// InMemoryIndex is an in-memory inverted index.
type InMemoryIndex struct {
	analyzer     *analysis.Analyzer
	fields       map[string]*FieldIndex    // fieldName -> FieldIndex
	docCount     int
	storedFields map[int]map[string]string // docID -> fieldName -> value
	fieldLengths map[string][]int // fieldName -> docID -> token count (dense, indexed by docID)
}

func NewInMemoryIndex(analyzer *analysis.Analyzer) *InMemoryIndex {
	return &InMemoryIndex{
		analyzer:     analyzer,
		fields:       make(map[string]*FieldIndex),
		storedFields: make(map[int]map[string]string),
		fieldLengths: make(map[string][]int),
	}
}

// AddDocument adds a document to the index.
func (idx *InMemoryIndex) AddDocument(doc *document.Document) error {
	docID := idx.docCount
	idx.docCount++

	for _, field := range doc.Fields {
		switch field.Type {
		case document.FieldTypeText:
			if err := idx.indexTextField(docID, field); err != nil {
				return err
			}
		case document.FieldTypeKeyword:
			idx.indexKeywordField(docID, field)
		}

		// store fields for retrieval
		if field.Type == document.FieldTypeStored || field.Type == document.FieldTypeText {
			if idx.storedFields[docID] == nil {
				idx.storedFields[docID] = make(map[string]string)
			}
			idx.storedFields[docID][field.Name] = field.Value
		}
	}

	return nil
}

// indexTextField analyzes the field and adds it to the inverted index.
func (idx *InMemoryIndex) indexTextField(docID int, field document.Field) error {
	tokens, err := idx.analyzer.Analyze(field.Value)
	if err != nil {
		return err
	}

	// Record field length for BM25 scoring (docIDs are sequential within a segment)
	idx.fieldLengths[field.Name] = append(idx.fieldLengths[field.Name], len(tokens))

	fi := idx.getOrCreateFieldIndex(field.Name)

	// aggregate freq and positions per term
	termInfo := make(map[string]*Posting)
	for _, token := range tokens {
		posting, exists := termInfo[token.Term]
		if !exists {
			posting = &Posting{DocID: docID}
			termInfo[token.Term] = posting
		}
		posting.Freq++
		posting.Positions = append(posting.Positions, token.Position)
	}

	// append to postings lists
	for term, posting := range termInfo {
		pl, exists := fi.postings[term]
		if !exists {
			pl = &PostingsList{Term: term}
			fi.postings[term] = pl
		}
		pl.Postings = append(pl.Postings, *posting)
	}

	return nil
}

// indexKeywordField indexes the field value as a single term without analysis.
func (idx *InMemoryIndex) indexKeywordField(docID int, field document.Field) {
	fi := idx.getOrCreateFieldIndex(field.Name)

	pl, exists := fi.postings[field.Value]
	if !exists {
		pl = &PostingsList{Term: field.Value}
		fi.postings[field.Value] = pl
	}
	pl.Postings = append(pl.Postings, Posting{
		DocID:     docID,
		Freq:      1,
		Positions: []int{0},
	})
}

func (idx *InMemoryIndex) getOrCreateFieldIndex(fieldName string) *FieldIndex {
	fi, exists := idx.fields[fieldName]
	if !exists {
		fi = newFieldIndex()
		idx.fields[fieldName] = fi
	}
	return fi
}

// GetPostings returns the postings list for a term in the given field.
func (idx *InMemoryIndex) GetPostings(fieldName, term string) *PostingsList {
	fi, exists := idx.fields[fieldName]
	if !exists {
		return nil
	}
	return fi.postings[term]
}

// GetStoredFields returns the stored fields for a document.
func (idx *InMemoryIndex) GetStoredFields(docID int) map[string]string {
	return idx.storedFields[docID]
}

// DocCount returns the number of documents in the index.
func (idx *InMemoryIndex) DocCount() int {
	return idx.docCount
}

// FieldLength returns the token count of a field in a document.
func (idx *InMemoryIndex) FieldLength(fieldName string, docID int) int {
	lengths, exists := idx.fieldLengths[fieldName]
	if !exists {
		return 0
	}
	return lengths[docID]
}

// AvgFieldLength returns the average token count across all documents for a field.
func (idx *InMemoryIndex) AvgFieldLength(fieldName string) float64 {
	lengths, exists := idx.fieldLengths[fieldName]
	if !exists || len(lengths) == 0 {
		return 0
	}
	total := 0
	for _, l := range lengths {
		total += l
	}
	return float64(total) / float64(len(lengths))
}
