package index

import (
	"fmt"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

// DocumentsWriterPerThread owns a single InMemorySegment and performs
// document indexing without locks. Each indexing goroutine gets its own DWPT.
type DocumentsWriterPerThread struct {
	segment        *InMemorySegment
	analyzer       *analysis.Analyzer
	bytesUsed      int64
	flushPending   bool
	deleteQueue    *DeleteQueue
	deleteSlice    *DeleteSlice
	pendingUpdates *BufferedUpdates
}

func newDWPT(segmentName string, analyzer *analysis.Analyzer, deleteQueue *DeleteQueue) *DocumentsWriterPerThread {
	return &DocumentsWriterPerThread{
		segment:        newInMemorySegment(segmentName),
		analyzer:       analyzer,
		deleteQueue:    deleteQueue,
		deleteSlice:    deleteQueue.newSlice(),
		pendingUpdates: newBufferedUpdates(),
	}
}

// addDocument indexes a single document into this DWPT's in-memory segment.
// Returns the number of new bytes used.
func (dwpt *DocumentsWriterPerThread) addDocument(doc *document.Document) (int64, error) {
	seg := dwpt.segment
	docID := seg.docCount
	seg.docCount++

	var bytesAdded int64

	for _, field := range doc.Fields {
		switch field.Type {
		case document.FieldTypeText:
			tokens, err := dwpt.analyzer.Analyze(field.Value)
			if err != nil {
				return 0, err
			}
			fi := dwpt.getOrCreateFieldIndex(field.Name)

			if seg.fieldLengths[field.Name] == nil {
				seg.fieldLengths[field.Name] = make([]int, 0)
			}
			for len(seg.fieldLengths[field.Name]) <= docID {
				seg.fieldLengths[field.Name] = append(seg.fieldLengths[field.Name], 0)
			}
			seg.fieldLengths[field.Name][docID] = len(tokens)
			bytesAdded += 4 // fieldLengths entry

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
				// term string + DocID(8) + Freq(8) + positions
				bytesAdded += int64(len(term) + 16 + 8*len(posting.Positions))
			}

		case document.FieldTypeKeyword:
			fi := dwpt.getOrCreateFieldIndex(field.Name)
			pl, exists := fi.postings[field.Value]
			if !exists {
				pl = &PostingsList{Term: field.Value}
				fi.postings[field.Value] = pl
			}
			pl.Postings = append(pl.Postings, Posting{
				DocID: docID, Freq: 1, Positions: []int{0},
			})
			bytesAdded += int64(len(field.Value) + 16 + 8)

		case document.FieldTypeNumericDocValues:
			vals := seg.numericDocValues[field.Name]
			if len(vals) <= docID {
				vals = append(vals, make([]int64, docID+1-len(vals))...)
			}
			vals[docID] = field.NumericValue
			seg.numericDocValues[field.Name] = vals
			bytesAdded += 8

		case document.FieldTypeSortedDocValues:
			svals := seg.sortedDocValues[field.Name]
			if len(svals) <= docID {
				svals = append(svals, make([]string, docID+1-len(svals))...)
			}
			svals[docID] = field.Value
			seg.sortedDocValues[field.Name] = svals
			bytesAdded += int64(len(field.Value))
		}

		// Stored fields
		if field.Type == document.FieldTypeStored || field.Type == document.FieldTypeText {
			if seg.storedFields[docID] == nil {
				seg.storedFields[docID] = make(map[string][]byte)
			}
			var storedValue []byte
			if field.BytesValue != nil {
				storedValue = field.BytesValue
			} else {
				storedValue = []byte(field.Value)
			}
			seg.storedFields[docID][field.Name] = storedValue
			bytesAdded += int64(len(field.Name) + len(storedValue))
		}
	}

	dwpt.bytesUsed += bytesAdded

	// After adding the document, catch up with global deletes.
	// docIDUpto = docID means deletes apply to docs with ID < docID (NOT this doc).
	if dwpt.deleteQueue.updateSlice(dwpt.deleteSlice) {
		dwpt.deleteSlice.apply(dwpt.pendingUpdates, docID)
	} else {
		dwpt.deleteSlice.reset()
	}

	return bytesAdded, nil
}

// prepareFlush freezes global deletes and applies remaining delete slice.
func (dwpt *DocumentsWriterPerThread) prepareFlush() *FrozenBufferedUpdates {
	numDocsInRAM := dwpt.segment.docCount
	globalUpdates := dwpt.deleteQueue.freezeGlobalBuffer(dwpt.deleteSlice)
	dwpt.deleteSlice.apply(dwpt.pendingUpdates, numDocsInRAM)
	return globalUpdates
}

// flush writes this DWPT's segment to disk and returns a SegmentCommitInfo.
// Buffered delete terms from pendingUpdates are applied using docIDUpto.
func (dwpt *DocumentsWriterPerThread) flush(dir store.Directory) (*SegmentCommitInfo, error) {
	seg := dwpt.segment
	if seg.docCount == 0 {
		return nil, nil
	}

	files, fields, err := WriteSegmentV2(dir, seg)
	if err != nil {
		return nil, fmt.Errorf("flush segment %s: %w", seg.name, err)
	}

	info := &SegmentCommitInfo{
		Name:   seg.name,
		MaxDoc: seg.docCount,
		Fields: fields,
		Files:  files,
	}

	if !dwpt.pendingUpdates.any() {
		return info, nil
	}

	// when deleted terms exist
	if err := dwpt.applyDeletes(dir, info); err != nil {
		return nil, err
	}
	dwpt.pendingUpdates.clear()

	return info, nil
}

// applyDeletes resolves buffered delete terms against the in-memory segment,
// writes a .del bitset file, and updates info with the delete count and file.
func (dwpt *DocumentsWriterPerThread) applyDeletes(dir store.Directory, info *SegmentCommitInfo) error {
	seg := dwpt.segment
	delBitset := NewBitset(seg.docCount)
	delCount := 0

	for _, dt := range dwpt.pendingUpdates.terms() {
		fi, exists := seg.fields[dt.Field]
		if !exists {
			continue
		}
		pl, exists := fi.postings[dt.Term]
		if !exists {
			continue
		}
		for _, posting := range pl.Postings {
			if posting.DocID < dt.DocIDUpto && !delBitset.Get(posting.DocID) {
				delBitset.Set(posting.DocID)
				delCount++
			}
		}
	}

	if delCount == 0 {
		return nil
	}

	pd := &PendingDeletes{
		info:               info,
		writeableLiveDocs:  delBitset,
		pendingDeleteCount: delCount,
	}
	delFile, err := pd.WriteLiveDocs(dir)
	if err != nil {
		return fmt.Errorf("write intra-segment deletes for %s: %w", seg.name, err)
	}

	info.DelCount = delCount
	info.Files = append(info.Files, delFile)
	return nil
}

// estimateBytesUsed returns the current RAM usage estimate.
func (dwpt *DocumentsWriterPerThread) estimateBytesUsed() int64 {
	return dwpt.bytesUsed
}

// reset replaces the internal segment with a fresh one.
func (dwpt *DocumentsWriterPerThread) reset(name string) {
	dwpt.segment = newInMemorySegment(name)
	dwpt.bytesUsed = 0
	dwpt.flushPending = false
	dwpt.deleteSlice = dwpt.deleteQueue.newSlice()
	dwpt.pendingUpdates = newBufferedUpdates()
}

func (dwpt *DocumentsWriterPerThread) getOrCreateFieldIndex(fieldName string) *FieldIndex {
	fi, exists := dwpt.segment.fields[fieldName]
	if !exists {
		fi = newFieldIndex()
		dwpt.segment.fields[fieldName] = fi
	}
	return fi
}
