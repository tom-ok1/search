package index

import (
	"bytes"
	"container/heap"
	"fmt"
	"sort"

	"gosearch/store"
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
	// Phase 1: Build docID mapping and stream stored fields to disk.
	// remaps[i] maps old docID → new docID for input i (-1 means deleted).
	remaps := make([][]int, len(inputs))

	storedOut, err := dir.CreateOutput(newName + ".stored")
	if err != nil {
		return nil, err
	}

	newDocID := 0
	var storedOffsets []uint64
	var storedPos uint64
	scratch := &bytes.Buffer{}

	for i, input := range inputs {
		n := input.Segment.DocCount()
		remaps[i] = make([]int, n)
		for oldDoc := 0; oldDoc < n; oldDoc++ {
			if input.IsDeleted(oldDoc) {
				remaps[i][oldDoc] = -1
				continue
			}
			remaps[i][oldDoc] = newDocID

			sf, err := input.Segment.StoredFields(oldDoc)
			if err != nil {
				storedOut.Close()
				return nil, err
			}
			storedOffsets = append(storedOffsets, storedPos)
			storedPos += writeStoredFieldsEntry(storedOut, sf, scratch)

			newDocID++
		}
	}
	docCount := newDocID

	// Write trailer: offset table + doc_count.
	for _, offset := range storedOffsets {
		storedOut.WriteUint64(offset)
	}
	storedOut.WriteUint32(uint32(docCount))
	storedOut.Close()

	allFields := collectAllFields(inputs)

	// Phase 2: Collect field lengths.
	fieldLengths := make(map[string][]int)
	for _, field := range allFields {
		lengths := make([]int, docCount)
		for i, input := range inputs {
			for oldDoc := 0; oldDoc < input.Segment.DocCount(); oldDoc++ {
				newID := remaps[i][oldDoc]
				if newID < 0 {
					continue
				}
				lengths[newID] = input.Segment.FieldLength(field, oldDoc)
			}
		}
		fieldLengths[field] = lengths
	}

	var files []string

	// Write metadata.
	metaFileName, err := writeSegmentMeta(dir, SegmentMeta{
		Name:     newName,
		DocCount: docCount,
		Fields:   allFields,
	})
	if err != nil {
		return nil, err
	}
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

	// Stored fields already written during Phase 1.
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
// .tdat and .tidx directly to disk.
func mergeFieldPostingsToDisk(
	dir store.Directory,
	segName, field string,
	inputs []MergeInput,
	remaps [][]int,
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

	// Open .tdat file for streaming writes.
	tdatOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.tdat", segName, field))
	if err != nil {
		return err
	}

	var metas []termMeta
	var globalOffset uint64
	var postings []Posting
	termBuf := &bytes.Buffer{}

	for h.Len() > 0 {
		currentTerm := h[0].term

		// Collect postings from all segments that have this term.
		postings = postings[:0]
		for h.Len() > 0 && h[0].term == currentTerm {
			entry := h[0]
			i := entry.inputIdx

			pi := inputs[i].Segment.PostingsIterator(field, currentTerm)
			for pi.Next() {
				oldDoc := pi.DocID()
				newID := remaps[i][oldDoc]
				if newID < 0 {
					continue
				}
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

		// Write postings to a small per-term buffer, then flush to disk.
		termBuf.Reset()
		writePostingsToBuffer(termBuf, postings)
		length := uint32(termBuf.Len())
		tdatOut.Write(termBuf.Bytes())

		metas = append(metas, termMeta{
			term:           currentTerm,
			docFreq:        len(postings),
			postingsOffset: globalOffset,
			postingsLength: length,
		})
		globalOffset += uint64(length)
	}

	tdatOut.Close()

	return writeTermIndex(dir, segName, field, metas)
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
