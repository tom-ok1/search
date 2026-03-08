package index

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"fmt"
	"gosearch/fst"
	"encoding/binary"
	"gosearch/store"
	"sort"
)

// MergeInput represents a single segment to be merged along with its deletion state.
type MergeInput struct {
	Segment   *DiskSegment
	IsDeleted func(docID int) bool
}

// MergeResult holds the outcome of a streaming merge.
type MergeResult struct {
	DocCount int
	Fields   []string
	Files    []string
}

// MergeSegmentsToDisk merges multiple disk segments and writes the result
// directly to disk, streaming postings per term to avoid holding all
// postings in memory at once.
func MergeSegmentsToDisk(dir store.Directory, inputs []MergeInput, newName string) (*MergeResult, error) {
	// Phase 1: Build docID mapping and collect stored fields + field lengths.
	type docMapping struct {
		inputIdx int
		oldDocID int
	}
	var mappings []docMapping
	remaps := make([]map[int]int, len(inputs))

	newDocID := 0
	storedFields := make(map[int]map[string]string)
	for i, input := range inputs {
		remaps[i] = make(map[int]int)
		for oldDoc := 0; oldDoc < input.Segment.DocCount(); oldDoc++ {
			if input.IsDeleted(oldDoc) {
				continue
			}
			remaps[i][oldDoc] = newDocID
			mappings = append(mappings, docMapping{inputIdx: i, oldDocID: oldDoc})

			sf, err := input.Segment.StoredFields(oldDoc)
			if err != nil {
				return nil, err
			}
			if len(sf) > 0 {
				storedFields[newDocID] = sf
			}

			newDocID++
		}
	}
	docCount := newDocID

	allFields := collectAllFields(inputs)

	// Phase 2: Collect field lengths.
	fieldLengths := make(map[string][]int)
	for _, field := range allFields {
		lengths := make([]int, docCount)
		for _, m := range mappings {
			newID := remaps[m.inputIdx][m.oldDocID]
			lengths[newID] = inputs[m.inputIdx].Segment.FieldLength(field, m.oldDocID)
		}
		fieldLengths[field] = lengths
	}

	var files []string

	// Write metadata.
	meta := SegmentMeta{
		Name:     newName,
		DocCount: docCount,
		Fields:   allFields,
	}
	metaFileName := newName + ".meta"
	metaOut, err := dir.CreateOutput(metaFileName)
	if err != nil {
		return nil, err
	}
	metaBytes, _ := json.Marshal(meta)
	metaOut.Write(metaBytes)
	metaOut.Close()
	files = append(files, metaFileName)

	// Phase 3: Merge postings using k-way merge and stream to disk.
	for _, field := range allFields {
		if err := mergeFieldPostingsToDisk(dir, newName, field, inputs, remaps); err != nil {
			return nil, err
		}
		files = append(files,
			fmt.Sprintf("%s.%s.tidx", newName, field),
			fmt.Sprintf("%s.%s.tdat", newName, field),
		)
	}

	// Phase 4: Write stored fields.
	if err := writeMergedStoredFields(dir, newName, docCount, storedFields); err != nil {
		return nil, err
	}
	files = append(files, newName+".stored")

	// Phase 5: Write field lengths.
	for _, field := range allFields {
		if err := writeFieldLengthsV2(dir, newName, field, fieldLengths[field], docCount); err != nil {
			return nil, err
		}
		files = append(files, fmt.Sprintf("%s.%s.flen", newName, field))
	}

	return &MergeResult{
		DocCount: docCount,
		Fields:   allFields,
		Files:    files,
	}, nil
}

// mergeFieldPostingsToDisk performs k-way merge for a single field and writes
// .tdat and .tidx directly to disk. Only the current term's postings are held
// in memory; completed postings are written and discarded immediately.
func mergeFieldPostingsToDisk(
	dir store.Directory,
	segName, field string,
	inputs []MergeInput,
	remaps []map[int]int,
) error {
	// Initialize the heap with one entry per segment that has this field.
	var h termHeap
	for i, input := range inputs {
		iter := input.Segment.TermIterator(field)
		if iter == nil {
			continue
		}
		if iter.Next() {
			h = append(h, &termHeapEntry{
				term:     iter.Term(),
				inputIdx: i,
				iter:     iter,
			})
		}
	}
	heap.Init(&h)

	// Stream postings to .tdat buffer, collecting term metadata.
	type termMeta struct {
		term           string
		docFreq        int
		postingsOffset uint64
		postingsLength uint32
	}
	var termMetas []termMeta
	tdatBuf := &bytes.Buffer{}

	for h.Len() > 0 {
		currentTerm := h[0].term

		// Collect postings from all segments that have this term.
		var postings []Posting
		for h.Len() > 0 && h[0].term == currentTerm {
			entry := h[0]
			i := entry.inputIdx
			input := inputs[i]

			pi := input.Segment.PostingsIterator(field, currentTerm)
			for pi.Next() {
				oldDoc := pi.DocID()
				if input.IsDeleted(oldDoc) {
					continue
				}
				newID := remaps[i][oldDoc]
				positions := make([]int, len(pi.Positions()))
				copy(positions, pi.Positions())
				postings = append(postings, Posting{
					DocID:     newID,
					Freq:      pi.Freq(),
					Positions: positions,
				})
			}

			if entry.iter.Next() {
				entry.term = entry.iter.Term()
				heap.Fix(&h, 0)
			} else {
				heap.Pop(&h)
			}
		}

		if len(postings) == 0 {
			continue
		}

		sort.Slice(postings, func(i, j int) bool {
			return postings[i].DocID < postings[j].DocID
		})

		// Write postings to .tdat buffer and record metadata.
		startOffset := uint64(tdatBuf.Len())
		prevDocID := 0
		for _, posting := range postings {
			writeVIntToBuffer(tdatBuf, posting.DocID-prevDocID)
			prevDocID = posting.DocID
			writeVIntToBuffer(tdatBuf, posting.Freq)
			writeVIntToBuffer(tdatBuf, len(posting.Positions))
			prevPos := 0
			for _, pos := range posting.Positions {
				writeVIntToBuffer(tdatBuf, pos-prevPos)
				prevPos = pos
			}
		}

		termMetas = append(termMetas, termMeta{
			term:           currentTerm,
			docFreq:        len(postings),
			postingsOffset: startOffset,
			postingsLength: uint32(uint64(tdatBuf.Len()) - startOffset),
		})
	}

	// Write .tdat file.
	tdatOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.tdat", segName, field))
	if err != nil {
		return err
	}
	tdatOut.Write(tdatBuf.Bytes())
	tdatOut.Close()

	// Build FST and .tidx from collected term metadata.
	fstBuilder := fst.NewBuilder()
	for i, tm := range termMetas {
		if err := fstBuilder.Add([]byte(tm.term), uint64(i)); err != nil {
			return fmt.Errorf("fst build: %w", err)
		}
	}
	fstBytes, err := fstBuilder.Finish()
	if err != nil {
		return fmt.Errorf("fst finish: %w", err)
	}

	tidxBuf := &bytes.Buffer{}
	writeUint32ToBuffer(tidxBuf, uint32(len(fstBytes)))
	tidxBuf.Write(fstBytes)
	writeUint32ToBuffer(tidxBuf, uint32(len(termMetas)))
	for _, tm := range termMetas {
		writeUint32ToBuffer(tidxBuf, uint32(tm.docFreq))
		writeUint64ToBuffer(tidxBuf, tm.postingsOffset)
		writeUint32ToBuffer(tidxBuf, tm.postingsLength)
	}

	tidxOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.tidx", segName, field))
	if err != nil {
		return err
	}
	tidxOut.Write(tidxBuf.Bytes())
	tidxOut.Close()

	return nil
}

// writeMergedStoredFields writes stored fields directly from the merge result.
func writeMergedStoredFields(dir store.Directory, segName string, docCount int, storedFields map[int]map[string]string) error {
	buf := &bytes.Buffer{}

	writeUint32ToBuffer(buf, uint32(docCount))

	// Reserve space for offset table.
	offsetTableStart := buf.Len()
	for i := 0; i < docCount; i++ {
		writeUint64ToBuffer(buf, 0)
	}

	data := buf.Bytes()
	docOffsets := make([]uint64, docCount)

	storedBuf := &bytes.Buffer{}
	for docID := 0; docID < docCount; docID++ {
		docOffsets[docID] = uint64(len(data) + storedBuf.Len())
		fields := storedFields[docID]
		writeVIntToBuffer(storedBuf, len(fields))
		for name, value := range fields {
			nameBytes := []byte(name)
			writeVIntToBuffer(storedBuf, len(nameBytes))
			storedBuf.Write(nameBytes)
			valueBytes := []byte(value)
			writeVIntToBuffer(storedBuf, len(valueBytes))
			storedBuf.Write(valueBytes)
		}
	}

	for i, offset := range docOffsets {
		binary.LittleEndian.PutUint64(data[offsetTableStart+i*8:], offset)
	}

	out, err := dir.CreateOutput(segName + ".stored")
	if err != nil {
		return err
	}
	defer out.Close()
	out.Write(data)
	out.Write(storedBuf.Bytes())

	return nil
}

// termHeapEntry holds a term iterator for one segment in the k-way merge.
type termHeapEntry struct {
	term     string
	inputIdx int
	iter     *TermIterator
}

var _ heap.Interface = (*termHeap)(nil)

// termHeap is a min-heap ordered by term (lexicographic).
type termHeap []*termHeapEntry

func (h termHeap) Len() int           { return len(h) }
func (h termHeap) Less(i, j int) bool { return h[i].term < h[j].term }
func (h termHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *termHeap) Push(x any)        { *h = append(*h, x.(*termHeapEntry)) }
func (h *termHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return entry
}

// collectAllFields returns a deduplicated, sorted list of all field names across all inputs.
func collectAllFields(inputs []MergeInput) []string {
	fieldSet := make(map[string]bool)
	for _, input := range inputs {
		for _, f := range input.Segment.Fields() {
			fieldSet[f] = true
		}
	}
	fields := make([]string, 0, len(fieldSet))
	for f := range fieldSet {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	return fields
}
