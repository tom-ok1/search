package index

import (
	"bytes"
	"container/heap"
	"fmt"
	"sort"

	"gosearch/fst"
	"gosearch/index/bkd"
	"gosearch/store"
)

// MergeInput represents a single segment to be merged along with its deletion state.
type MergeInput struct {
	Segment   *DiskSegment
	IsDeleted func(docID int) bool
}

// segmentDocMap holds per-segment state for mapping old doc IDs to new merged IDs.
type segmentDocMap struct {
	liveDocs *Bitset
	offset   int // start position in the merged ID space
}

// DocIDMapper maps old segment-local doc IDs to new merged doc IDs.
// It uses precomputed rank tables for O(1) lookups.
type DocIDMapper struct {
	segments  []segmentDocMap
	liveCount int
}

// NewDocIDMapper builds a mapper by scanning each input's deletion state.
func NewDocIDMapper(inputs []MergeInput) *DocIDMapper {
	m := &DocIDMapper{
		segments: make([]segmentDocMap, len(inputs)),
	}
	for i, input := range inputs {
		n := input.Segment.DocCount()
		seg := &m.segments[i]
		seg.offset = m.liveCount
		seg.liveDocs = NewBitset(n)
		for oldDoc := range n {
			if !input.IsDeleted(oldDoc) {
				seg.liveDocs.Set(oldDoc)
				m.liveCount++
			}
		}
		seg.liveDocs.BuildRankTable()
	}
	return m
}

// IsLive reports whether oldDoc in segment segIdx is live (not deleted).
func (m *DocIDMapper) IsLive(segIdx, oldDoc int) bool {
	return m.segments[segIdx].liveDocs.Get(oldDoc)
}

// Map returns the new merged doc ID for oldDoc in segment segIdx.
// The caller must ensure IsLive(segIdx, oldDoc) is true.
func (m *DocIDMapper) Map(segIdx, oldDoc int) int {
	return m.segments[segIdx].offset + m.segments[segIdx].liveDocs.Rank(oldDoc)
}

// LiveDocCount returns the total number of live documents across all segments.
func (m *DocIDMapper) LiveDocCount() int {
	return m.liveCount
}

// MergeResult holds the outcome of a streaming merge.
type MergeResult struct {
	DocCount        int
	Fields          []string
	NumericDVFields []string
	SortedDVFields  []string
	Files           []string
}

// MergeSegmentsToDisk merges multiple disk segments and writes the result
// directly to disk, streaming postings per term to avoid holding all
// postings in memory at once.
func MergeSegmentsToDisk(dir store.Directory, inputs []MergeInput, newName string) (*MergeResult, error) {
	mapper := NewDocIDMapper(inputs)
	docCount := mapper.LiveDocCount()

	allFields := collectMergeFields(inputs, (*DiskSegment).Fields)
	numericDVFields := collectMergeFields(inputs, (*DiskSegment).NumericDVFields)
	sortedDVFields := collectMergeFields(inputs, (*DiskSegment).SortedDVFields)

	// Collect point fields from inputs.
	pointFieldSet := make(map[string]struct{})
	for _, input := range inputs {
		for f := range input.Segment.PointFields() {
			pointFieldSet[f] = struct{}{}
		}
	}
	pointFields := make([]string, 0, len(pointFieldSet))
	for f := range pointFieldSet {
		pointFields = append(pointFields, f)
	}
	sort.Strings(pointFields)

	var files []string

	// Write metadata.
	metaFileName, err := writeSegmentMeta(dir, SegmentMeta{
		Name:            newName,
		DocCount:        docCount,
		Fields:          allFields,
		NumericDVFields: numericDVFields,
		SortedDVFields:  sortedDVFields,
		PointFields:     pointFields,
	})
	if err != nil {
		return nil, err
	}
	files = append(files, metaFileName)

	// Merge stored fields.
	if err := mergeStoredFieldsToDisk(dir, newName, inputs, mapper); err != nil {
		return nil, err
	}
	files = append(files, newName+".stored")

	// Merge postings per field.
	for _, field := range allFields {
		if err := mergeFieldPostingsToDisk(dir, newName, field, inputs, mapper); err != nil {
			return nil, err
		}
		files = append(files,
			fmt.Sprintf("%s.%s.tidx", newName, field),
			fmt.Sprintf("%s.%s.tfst", newName, field),
			fmt.Sprintf("%s.%s.tdat", newName, field),
		)
	}

	// Merge field lengths.
	for _, field := range allFields {
		if err := mergeFieldLengthsToDisk(dir, newName, field, inputs, mapper); err != nil {
			return nil, err
		}
		files = append(files, fmt.Sprintf("%s.%s.flen", newName, field))
	}

	// Merge numeric doc values.
	for _, field := range numericDVFields {
		if err := mergeNumericDocValuesToDisk(dir, newName, field, inputs, mapper); err != nil {
			return nil, err
		}
		files = append(files, fmt.Sprintf("%s.%s.ndv", newName, field))

		_, isPoint := pointFieldSet[field]
		if isPoint {
			if err := mergePointFieldToDisk(dir, newName, field, inputs, mapper); err != nil {
				return nil, err
			}
			files = append(files, fmt.Sprintf("%s.%s.kd", newName, field))
		} else {
			if err := writeNumericDocValuesSkipIndexFromNDV(dir, newName, field, docCount); err != nil {
				return nil, err
			}
			files = append(files, fmt.Sprintf("%s.%s.ndvs", newName, field))
		}
	}

	// Merge sorted doc values.
	for _, field := range sortedDVFields {
		if err := mergeSortedDocValuesToDisk(dir, newName, field, inputs, mapper); err != nil {
			return nil, err
		}
		if err := writeSortedDocValuesSkipIndexFromOrd(dir, newName, field, docCount); err != nil {
			return nil, err
		}
		files = append(files,
			fmt.Sprintf("%s.%s.sdvo", newName, field),
			fmt.Sprintf("%s.%s.sdvd", newName, field),
			fmt.Sprintf("%s.%s.sdvs", newName, field),
		)
	}

	return &MergeResult{
		DocCount:        docCount,
		Fields:          allFields,
		NumericDVFields: numericDVFields,
		SortedDVFields:  sortedDVFields,
		Files:           files,
	}, nil
}

// mergeStoredFieldsToDisk streams stored fields from all input segments to disk.
func mergeStoredFieldsToDisk(dir store.Directory, segName string, inputs []MergeInput, mapper *DocIDMapper) error {
	out, err := dir.CreateOutput(segName + ".stored")
	if err != nil {
		return err
	}
	defer out.Close()

	var offsets []uint64
	var pos uint64
	scratch := &bytes.Buffer{}

	for i, input := range inputs {
		for oldDoc := range input.Segment.DocCount() {
			if !mapper.IsLive(i, oldDoc) {
				continue
			}
			sf, err := input.Segment.StoredFields(oldDoc)
			if err != nil {
				return err
			}
			offsets = append(offsets, pos)
			n, err := writeStoredFieldsEntry(out, sf, scratch)
			if err != nil {
				return fmt.Errorf("write stored fields: %w", err)
			}
			pos += n
		}
	}

	for _, offset := range offsets {
		if err := out.WriteUint64(offset); err != nil {
			return fmt.Errorf("write stored offset: %w", err)
		}
	}
	if err := out.WriteUint32(uint32(mapper.LiveDocCount())); err != nil {
		return fmt.Errorf("write stored doc count: %w", err)
	}
	return nil
}

// mergeFieldLengthsToDisk streams field lengths for a single field to disk.
func mergeFieldLengthsToDisk(dir store.Directory, segName, field string, inputs []MergeInput, mapper *DocIDMapper) error {
	out, err := dir.CreateOutput(fmt.Sprintf("%s.%s.flen", segName, field))
	if err != nil {
		return err
	}
	defer out.Close()

	if err := out.WriteUint32(uint32(mapper.LiveDocCount())); err != nil {
		return fmt.Errorf("write flen doc count: %w", err)
	}
	for i, input := range inputs {
		for oldDoc := 0; oldDoc < input.Segment.DocCount(); oldDoc++ {
			if !mapper.IsLive(i, oldDoc) {
				continue
			}
			l := input.Segment.FieldLength(field, oldDoc)
			if err := out.WriteUint32(uint32(l)); err != nil {
				return fmt.Errorf("write flen: %w", err)
			}
		}
	}
	return nil
}

// mergeNumericDocValuesToDisk streams numeric doc values for a single field to disk.
func mergeNumericDocValuesToDisk(dir store.Directory, segName, field string, inputs []MergeInput, mapper *DocIDMapper) error {
	out, err := dir.CreateOutput(fmt.Sprintf("%s.%s.ndv", segName, field))
	if err != nil {
		return err
	}
	defer out.Close()

	docCount := mapper.LiveDocCount()

	// Collect presence + values, then determine mode.
	presence := NewBitset(docCount)
	var values []int64

	for i, input := range inputs {
		ndv := input.Segment.NumericDocValues(field)
		if ndv == nil {
			continue
		}
		for oldDoc := 0; oldDoc < input.Segment.DocCount(); oldDoc++ {
			if !mapper.IsLive(i, oldDoc) {
				continue
			}
			if !ndv.HasValue(oldDoc) {
				continue
			}
			newDoc := mapper.Map(i, oldDoc)
			v, err := ndv.Get(oldDoc)
			if err != nil {
				return fmt.Errorf("read numeric DV for field %s doc %d: %w", field, oldDoc, err)
			}
			presence.Set(newDoc)
			values = append(values, v)
		}
	}

	numWithValue := len(values)
	switch numWithValue {
	case 0:
		if _, err := out.Write([]byte{ndvModeEmpty}); err != nil {
			return fmt.Errorf("write ndv mode: %w", err)
		}
	case docCount:
		// All docs have values — dense.
		if _, err := out.Write([]byte{ndvModeDense}); err != nil {
			return fmt.Errorf("write ndv mode: %w", err)
		}
		for _, v := range values {
			if err := out.WriteUint64(uint64(v)); err != nil {
				return fmt.Errorf("write ndv value: %w", err)
			}
		}
	default:
		// Sparse.
		if _, err := out.Write([]byte{ndvModeSparse}); err != nil {
			return fmt.Errorf("write ndv mode: %w", err)
		}
		if _, err := out.Write(presence.Bytes()); err != nil {
			return fmt.Errorf("write ndv bitset: %w", err)
		}
		for _, v := range values {
			if err := out.WriteUint64(uint64(v)); err != nil {
				return fmt.Errorf("write ndv sparse value: %w", err)
			}
		}
	}

	if err := out.WriteUint32(uint32(docCount)); err != nil {
		return err
	}
	return nil
}

// mergePointFieldToDisk writes the BKD tree (.kd) for a point field during segment merge.
// It uses streaming k-way merge: each input segment's BKD tree is read leaf-by-leaf
// via MergeReader, merged through a min-heap, and written to a OneDimensionBKDWriter.
// Memory usage is O(segments × MaxPointsInLeafNode) instead of O(total points).
func mergePointFieldToDisk(dir store.Directory, segName, field string, inputs []MergeInput, mapper *DocIDMapper) error {
	type readerEntry struct {
		reader *bkd.MergeReader
		segIdx int
	}

	var entries []readerEntry
	for i, input := range inputs {
		pv := input.Segment.PointValues(field)
		if pv == nil {
			continue
		}
		tree := pv.PointTree()
		if tree.Size() == 0 {
			continue
		}
		mr, err := bkd.NewMergeReader(tree)
		if err != nil {
			return fmt.Errorf("create MergeReader for seg %d field %s: %w", i, field, err)
		}
		entries = append(entries, readerEntry{reader: mr, segIdx: i})
	}

	odw, err := bkd.NewOneDimensionBKDWriter(dir, segName, field)
	if err != nil {
		return err
	}
	defer odw.Abort()

	// Seed the heap: advance each reader to its first live doc.
	var h pointHeap
	for idx := range entries {
		e := &entries[idx]
		for e.reader.Next() {
			if !mapper.IsLive(e.segIdx, e.reader.DocID()) {
				continue
			}
			h = append(h, &pointHeapEntry{
				value:    e.reader.Value(),
				docID:    mapper.Map(e.segIdx, e.reader.DocID()),
				entryIdx: idx,
			})
			break
		}
	}

	if len(h) == 0 {
		return odw.Finish()
	}

	heap.Init(&h)

	// k-way merge loop.
	for len(h) > 0 {
		top := h[0]
		if err := odw.Add(top.docID, top.value); err != nil {
			return err
		}

		// Advance this reader to the next live doc.
		e := &entries[top.entryIdx]
		advanced := false
		for e.reader.Next() {
			if !mapper.IsLive(e.segIdx, e.reader.DocID()) {
				continue
			}
			top.value = e.reader.Value()
			top.docID = mapper.Map(e.segIdx, e.reader.DocID())
			advanced = true
			break
		}

		if advanced {
			heap.Fix(&h, 0)
		} else {
			heap.Pop(&h)
		}
	}

	return odw.Finish()
}

// mergeFieldPostingsToDisk performs k-way merge for a single field and writes
// .tdat and .tidx directly to disk.
func mergeFieldPostingsToDisk(
	dir store.Directory,
	segName, field string,
	inputs []MergeInput,
	mapper *DocIDMapper,
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

	// Open .tdat and .tidx files for streaming writes.
	tdatOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.tdat", segName, field))
	if err != nil {
		return err
	}
	defer tdatOut.Close()

	tidxOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.tidx", segName, field))
	if err != nil {
		return err
	}
	defer tidxOut.Close()

	// Open .tfst for streaming FST writes.
	tfstOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.tfst", segName, field))
	if err != nil {
		return err
	}
	defer tfstOut.Close()

	fstBuilder := fst.NewBuilderWithWriter(tfstOut)
	var ordinal uint64
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
				if !mapper.IsLive(i, oldDoc) {
					continue
				}
				postings = append(postings, Posting{
					DocID:     mapper.Map(i, oldDoc),
					Freq:      pi.Freq(),
					Positions: pi.Positions(),
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
		if _, err := tdatOut.Write(termBuf.Bytes()); err != nil {
			return fmt.Errorf("write tdat: %w", err)
		}

		// Stream metadata to .tidx
		if err := tidxOut.WriteUint32(uint32(len(postings))); err != nil {
			return fmt.Errorf("write tidx: %w", err)
		}
		if err := tidxOut.WriteUint64(globalOffset); err != nil {
			return fmt.Errorf("write tidx: %w", err)
		}
		if err := tidxOut.WriteUint32(length); err != nil {
			return fmt.Errorf("write tidx: %w", err)
		}

		if err := fstBuilder.Add([]byte(currentTerm), ordinal); err != nil {
			return fmt.Errorf("fst build: %w", err)
		}
		ordinal++
		globalOffset += uint64(length)
	}

	// Finish writes the trailer to .tfst.
	if err := fstBuilder.Finish(); err != nil {
		return fmt.Errorf("fst finish: %w", err)
	}
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

// mergeSortedDocValuesToDisk merges sorted doc values from multiple segments
// using a k-way merge of their dictionaries and streaming ordinal writes.
func mergeSortedDocValuesToDisk(dir store.Directory, segName, field string, inputs []MergeInput, mapper *DocIDMapper) error {
	// Collect SortedDocValues handles.
	sdvs := make([]SortedDocValues, len(inputs))
	for i, input := range inputs {
		sdvs[i] = input.Segment.SortedDocValues(field)
	}

	// In-memory ordinal map: segmentOrd → globalOrd for each segment.
	segmentToGlobalOrds := make([][]int32, len(inputs))
	for i, sdv := range sdvs {
		if sdv != nil && sdv.ValueCount() > 0 {
			segmentToGlobalOrds[i] = make([]int32, sdv.ValueCount())
		}
	}

	// Initialize the heap with ord=0 from each segment that has values.
	var h sdvHeap
	for i, sdv := range sdvs {
		if sdv == nil || sdv.ValueCount() == 0 {
			continue
		}
		val, err := sdv.LookupOrd(0)
		if err != nil {
			return fmt.Errorf("lookup initial ord for seg %d: %w", i, err)
		}
		h = append(h, &sdvHeapEntry{
			value:  val,
			segIdx: i,
			ord:    0,
			maxOrd: sdv.ValueCount(),
			sdv:    sdv,
		})
	}
	heap.Init(&h)

	// K-way merge dictionaries → write .sdvd
	sdvdOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.sdvd", segName, field))
	if err != nil {
		return err
	}
	defer sdvdOut.Close()

	var newOrd int32
	var offsets []uint64
	var pos uint64

	for h.Len() > 0 {
		currentValue := h[0].value

		// Collect all segments that have this same value.
		for h.Len() > 0 && bytes.Equal(h[0].value, currentValue) {
			entry := h[0]
			segmentToGlobalOrds[entry.segIdx][entry.ord] = newOrd

			entry.ord++
			if entry.ord < entry.maxOrd {
				val, err := entry.sdv.LookupOrd(entry.ord)
				if err != nil {
					return fmt.Errorf("lookup ord %d seg %d: %w", entry.ord, entry.segIdx, err)
				}
				entry.value = val
				heap.Fix(&h, 0)
			} else {
				heap.Pop(&h)
			}
		}

		// Write value data.
		offsets = append(offsets, pos)
		if _, err := sdvdOut.Write(currentValue); err != nil {
			return fmt.Errorf("write sdvd value: %w", err)
		}
		pos += uint64(len(currentValue))
		newOrd++
	}

	// Write offset table and trailer.
	for _, offset := range offsets {
		if err := sdvdOut.WriteUint64(offset); err != nil {
			return fmt.Errorf("write sdvd offset: %w", err)
		}
	}
	if err := sdvdOut.WriteUint32(uint32(newOrd)); err != nil {
		return fmt.Errorf("write sdvd trailer: %w", err)
	}

	// Stream .sdvo: iterate segments sequentially, remap ordinals.
	sdvoOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.sdvo", segName, field))
	if err != nil {
		return err
	}
	defer sdvoOut.Close()

	for i, input := range inputs {
		for oldDoc := 0; oldDoc < input.Segment.DocCount(); oldDoc++ {
			if !mapper.IsLive(i, oldDoc) {
				continue
			}
			mapped := int32(-1)
			if sdvs[i] != nil {
				oldOrd, err := sdvs[i].OrdValue(oldDoc)
				if err != nil {
					return fmt.Errorf("read sdvo ord seg %d doc %d: %w", i, oldDoc, err)
				}
				if oldOrd >= 0 {
					mapped = segmentToGlobalOrds[i][oldOrd]
				}
			}
			if err := sdvoOut.WriteUint32(uint32(mapped)); err != nil {
				return fmt.Errorf("write sdvo: %w", err)
			}
		}
	}

	if err := sdvoOut.WriteUint32(uint32(mapper.LiveDocCount())); err != nil {
		return fmt.Errorf("write sdvo trailer: %w", err)
	}

	return nil
}

// sdvHeapEntry holds a dictionary value iterator for one segment in the k-way merge.
type sdvHeapEntry struct {
	value  []byte
	segIdx int
	ord    int
	maxOrd int
	sdv    SortedDocValues
}

// sdvHeap is a min-heap ordered by value (lexicographic).
type sdvHeap []*sdvHeapEntry

func (h sdvHeap) Len() int           { return len(h) }
func (h sdvHeap) Less(i, j int) bool { return bytes.Compare(h[i].value, h[j].value) < 0 }
func (h sdvHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *sdvHeap) Push(x any)        { *h = append(*h, x.(*sdvHeapEntry)) }
func (h *sdvHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return entry
}

// pointHeapEntry holds one MergeReader's current point for the k-way merge heap.
type pointHeapEntry struct {
	value    int64
	docID    int
	entryIdx int // index into the entries slice for looking up the reader and segIdx
}

// pointHeap is a min-heap ordered by (value, docID).
type pointHeap []*pointHeapEntry

func (h pointHeap) Len() int { return len(h) }
func (h pointHeap) Less(i, j int) bool {
	if h[i].value != h[j].value {
		return h[i].value < h[j].value
	}
	return h[i].docID < h[j].docID
}
func (h pointHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *pointHeap) Push(x any)   { *h = append(*h, x.(*pointHeapEntry)) }
func (h *pointHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return entry
}

// collectMergeFields returns a deduplicated, sorted list of field names
// extracted from each input segment using the given accessor.
func collectMergeFields(inputs []MergeInput, accessor func(*DiskSegment) []string) []string {
	fieldSet := make(map[string]bool)
	for _, input := range inputs {
		for _, f := range accessor(input.Segment) {
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
