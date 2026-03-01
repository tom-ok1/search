package index

import (
	"fmt"
	"gosearch/analysis"
	"gosearch/document"
)

// IndexWriter adds documents to the index and manages segments.
type IndexWriter struct {
	analyzer       *analysis.Analyzer
	segments       []*Segment
	buffer         *Segment // in-memory buffer (not yet a segment)
	bufferSize     int      // flush threshold (number of documents)
	segmentCounter int
}

func NewIndexWriter(analyzer *analysis.Analyzer, bufferSize int) *IndexWriter {
	w := &IndexWriter{
		analyzer:   analyzer,
		bufferSize: bufferSize,
	}
	w.buffer = newSegment(w.nextSegmentName())
	return w
}

func (w *IndexWriter) nextSegmentName() string {
	name := fmt.Sprintf("_seg%d", w.segmentCounter)
	w.segmentCounter++
	return name
}

// AddDocument adds a document to the in-memory buffer.
// When the buffer reaches the threshold, it is automatically flushed.
func (w *IndexWriter) AddDocument(doc *document.Document) error {
	docID := w.buffer.docCount
	w.buffer.docCount++

	for _, field := range doc.Fields {
		switch field.Type {
		case document.FieldTypeText:
			tokens, err := w.analyzer.Analyze(field.Value)
			if err != nil {
				return err
			}
			fi := w.getOrCreateFieldIndex(w.buffer, field.Name)

			// Extend the fieldLengths slice with zeros so it can be indexed by docID.
			if w.buffer.fieldLengths[field.Name] == nil {
				w.buffer.fieldLengths[field.Name] = make([]int, 0)
			}
			for len(w.buffer.fieldLengths[field.Name]) <= docID {
				w.buffer.fieldLengths[field.Name] = append(w.buffer.fieldLengths[field.Name], 0)
			}
			w.buffer.fieldLengths[field.Name][docID] = len(tokens)

			// Build postings
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

			for term, posting := range termInfo {
				pl, exists := fi.postings[term]
				if !exists {
					pl = &PostingsList{Term: term}
					fi.postings[term] = pl
				}
				pl.Postings = append(pl.Postings, *posting)
			}

		case document.FieldTypeKeyword:
			fi := w.getOrCreateFieldIndex(w.buffer, field.Name)
			pl, exists := fi.postings[field.Value]
			if !exists {
				pl = &PostingsList{Term: field.Value}
				fi.postings[field.Value] = pl
			}
			pl.Postings = append(pl.Postings, Posting{
				DocID: docID, Freq: 1, Positions: []int{0},
			})
		}

		// Stored fields
		if field.Type == document.FieldTypeStored || field.Type == document.FieldTypeText {
			if w.buffer.storedFields[docID] == nil {
				w.buffer.storedFields[docID] = make(map[string]string)
			}
			w.buffer.storedFields[docID][field.Name] = field.Value
		}
	}

	// Auto flush
	if w.buffer.docCount >= w.bufferSize {
		w.Flush()
	}

	return nil
}

// Flush commits the in-memory buffer as a new segment.
func (w *IndexWriter) Flush() {
	if w.buffer.docCount == 0 {
		return
	}
	w.segments = append(w.segments, w.buffer)
	w.buffer = newSegment(w.nextSegmentName())
}

// Segments returns all current segments (including the buffer if non-empty).
func (w *IndexWriter) Segments() []*Segment {
	if w.buffer.docCount > 0 {
		return append(w.segments, w.buffer)
	}
	return w.segments
}

// DeleteDocuments marks documents matching the given field/term as deleted.
func (w *IndexWriter) DeleteDocuments(field, term string) {
	for _, seg := range w.Segments() {
		fi, exists := seg.fields[field]
		if !exists {
			continue
		}
		pl, exists := fi.postings[term]
		if !exists {
			continue
		}
		for _, posting := range pl.Postings {
			seg.MarkDeleted(posting.DocID)
		}
	}
}

func (w *IndexWriter) getOrCreateFieldIndex(seg *Segment, fieldName string) *FieldIndex {
	fi, exists := seg.fields[fieldName]
	if !exists {
		fi = newFieldIndex()
		seg.fields[fieldName] = fi
	}
	return fi
}
