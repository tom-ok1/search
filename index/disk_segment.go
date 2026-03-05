package index

import (
	"encoding/json"
	"fmt"
	"gosearch/store"
	"os"
)

// SegmentMeta holds segment metadata persisted as JSON.
type SegmentMeta struct {
	Name          string   `json:"name"`
	DocCount      int      `json:"doc_count"`
	Fields        []string `json:"fields"`
}

// ---------------------------------------------------------------------------
// DiskSegment (mmap-based)
// ---------------------------------------------------------------------------

// DiskSegment is a mmap-based lazy-loading SegmentReader.
// Only lightweight metadata is held in memory; all data is accessed
// via mmap'd files and decoded on demand.
type DiskSegment struct {
	name      string
	docCount  int
	fieldList []string

	// Per-field mmap'd files
	termIndex map[string]*store.MMapIndexInput // field → .tidx
	termData  map[string]*store.MMapIndexInput // field → .tdat
	fieldLens map[string]*store.MMapIndexInput // field → .flen

	// Segment-level mmap'd files
	stored  *store.MMapIndexInput // .stored
	deleted *store.MMapIndexInput // .del (nil if no deletions)
}

// OpenDiskSegment opens a V2 segment from the given directory path.
func OpenDiskSegment(dirPath string, segName string) (*DiskSegment, error) {
	// Read metadata
	metaBytes, err := os.ReadFile(fmt.Sprintf("%s/%s.meta", dirPath, segName))
	if err != nil {
		return nil, fmt.Errorf("read meta: %w", err)
	}
	var meta SegmentMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, fmt.Errorf("parse meta: %w", err)
	}

	ds := &DiskSegment{
		name:      meta.Name,
		docCount:  meta.DocCount,
		fieldList: meta.Fields,
		termIndex: make(map[string]*store.MMapIndexInput),
		termData:  make(map[string]*store.MMapIndexInput),
		fieldLens: make(map[string]*store.MMapIndexInput),
	}

	// Mmap per-field files
	for _, field := range meta.Fields {
		tidx, err := store.OpenMMap(fmt.Sprintf("%s/%s.%s.tidx", dirPath, segName, field))
		if err != nil {
			ds.Close()
			return nil, fmt.Errorf("mmap tidx for %s: %w", field, err)
		}
		ds.termIndex[field] = tidx

		tdat, err := store.OpenMMap(fmt.Sprintf("%s/%s.%s.tdat", dirPath, segName, field))
		if err != nil {
			ds.Close()
			return nil, fmt.Errorf("mmap tdat for %s: %w", field, err)
		}
		ds.termData[field] = tdat

		flen, err := store.OpenMMap(fmt.Sprintf("%s/%s.%s.flen", dirPath, segName, field))
		if err != nil {
			ds.Close()
			return nil, fmt.Errorf("mmap flen for %s: %w", field, err)
		}
		ds.fieldLens[field] = flen
	}

	// Mmap stored fields
	storedPath := fmt.Sprintf("%s/%s.stored", dirPath, segName)
	ds.stored, err = store.OpenMMap(storedPath)
	if err != nil {
		ds.Close()
		return nil, fmt.Errorf("mmap stored: %w", err)
	}

	// Optionally mmap deleted docs bitmap
	delPath := fmt.Sprintf("%s/%s.del", dirPath, segName)
	if _, statErr := os.Stat(delPath); statErr == nil {
		ds.deleted, err = store.OpenMMap(delPath)
		if err != nil {
			ds.Close()
			return nil, fmt.Errorf("mmap del: %w", err)
		}
	}

	return ds, nil
}

// Close unmaps all memory-mapped files.
func (ds *DiskSegment) Close() error {
	for _, m := range ds.termIndex {
		m.Close()
	}
	for _, m := range ds.termData {
		m.Close()
	}
	for _, m := range ds.fieldLens {
		m.Close()
	}
	if ds.stored != nil {
		ds.stored.Close()
	}
	if ds.deleted != nil {
		ds.deleted.Close()
	}
	return nil
}

// --- SegmentReader interface ---

func (ds *DiskSegment) Name() string { return ds.name }

func (ds *DiskSegment) DocCount() int { return ds.docCount }

func (ds *DiskSegment) LiveDocCount() int {
	if ds.deleted == nil {
		return ds.docCount
	}
	count := 0
	for i := 0; i < ds.docCount; i++ {
		if !ds.IsDeleted(i) {
			count++
		}
	}
	return count
}

func (ds *DiskSegment) IsDeleted(docID int) bool {
	if ds.deleted == nil {
		return false
	}
	// Bitmap format: [doc_count: uint32][bitmap: ceil(doc_count/8) bytes]
	byteIdx := 4 + docID/8
	bitIdx := uint(docID % 8)
	if byteIdx >= ds.deleted.Length() {
		return false
	}
	ds.deleted.Seek(byteIdx)
	val, err := ds.deleted.ReadByte()
	if err != nil {
		return false
	}
	return val&(1<<bitIdx) != 0
}

// DocFreq looks up the term in the .tidx file via binary search and returns doc_freq.
func (ds *DiskSegment) DocFreq(field, term string) int {
	tidx := ds.termIndex[field]
	if tidx == nil {
		return 0
	}

	_, docFreq, _, _ := ds.lookupTerm(tidx, term)
	return docFreq
}

// PostingsIterator returns an iterator that reads postings from the mmap'd .tdat file.
func (ds *DiskSegment) PostingsIterator(field, term string) PostingsIterator {
	tidx := ds.termIndex[field]
	tdat := ds.termData[field]
	if tidx == nil || tdat == nil {
		return EmptyPostingsIterator{}
	}

	found, docFreq, postingsOffset, postingsLength := ds.lookupTerm(tidx, term)
	if !found {
		return EmptyPostingsIterator{}
	}

	slice, err := tdat.Slice(int(postingsOffset), int(postingsLength))
	if err != nil {
		return EmptyPostingsIterator{}
	}

	return &DiskPostingsIterator{
		input:     slice,
		remaining: docFreq,
	}
}

// FieldLength reads a single field length from the mmap'd .flen file. O(1).
func (ds *DiskSegment) FieldLength(field string, docID int) int {
	flen := ds.fieldLens[field]
	if flen == nil {
		return 0
	}
	// Format: [doc_count: uint32][lengths: doc_count × uint32]
	offset := 4 + docID*4
	v, err := flen.ReadUint32At(offset)
	if err != nil {
		return 0
	}
	return int(v)
}

// TotalFieldLength sums all field lengths by reading the mmap'd .flen file.
func (ds *DiskSegment) TotalFieldLength(field string) int {
	flen := ds.fieldLens[field]
	if flen == nil {
		return 0
	}
	total := 0
	for i := 0; i < ds.docCount; i++ {
		offset := 4 + i*4
		v, err := flen.ReadUint32At(offset)
		if err != nil {
			continue
		}
		total += int(v)
	}
	return total
}

// StoredFields reads stored fields for a single document from the mmap'd .stored file. O(1) seek.
func (ds *DiskSegment) StoredFields(docID int) (map[string]string, error) {
	if ds.stored == nil || docID >= ds.docCount {
		return nil, nil
	}

	// Format: [doc_count: uint32][offset_table: doc_count × uint64][data...]
	offset, err := ds.stored.ReadUint64At(4 + docID*8)
	if err != nil {
		return nil, fmt.Errorf("read stored offset: %w", err)
	}

	ds.stored.Seek(int(offset))
	fieldCount, err := ds.stored.ReadVInt()
	if err != nil {
		return nil, fmt.Errorf("read field count: %w", err)
	}

	fields := make(map[string]string, fieldCount)
	for i := 0; i < fieldCount; i++ {
		nameLen, _ := ds.stored.ReadVInt()
		nameBytes, _ := ds.stored.ReadBytes(nameLen)
		valueLen, _ := ds.stored.ReadVInt()
		valueBytes, _ := ds.stored.ReadBytes(valueLen)
		fields[string(nameBytes)] = string(valueBytes)
	}
	return fields, nil
}

// lookupTerm performs a binary search on the .tidx file for the given term.
// Returns (found, docFreq, postingsOffset, postingsLength).
func (ds *DiskSegment) lookupTerm(tidx *store.MMapIndexInput, term string) (bool, int, uint64, uint32) {
	// .tidx format:
	//   [term_count: uint32]
	//   [offset_table: term_count × uint64]
	//   [term_entries: ...]

	termCountVal, err := tidx.ReadUint32At(0)
	if err != nil {
		return false, 0, 0, 0
	}
	termCount := int(termCountVal)
	if termCount == 0 {
		return false, 0, 0, 0
	}

	termBytes := []byte(term)

	lo, hi := 0, termCount-1
	for lo <= hi {
		mid := lo + (hi-lo)/2

		// Read offset of the mid-th term entry from the offset table
		entryOffset, err := tidx.ReadUint64At(4 + mid*8)
		if err != nil {
			return false, 0, 0, 0
		}

		// Read term at that offset
		midTermLen, docFreq, postingsOffset, postingsLength, midTerm := ds.readTermEntry(tidx, int(entryOffset))
		_ = midTermLen

		cmp := compareBytes(midTerm, termBytes)
		if cmp == 0 {
			return true, docFreq, postingsOffset, postingsLength
		} else if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	return false, 0, 0, 0
}

// readTermEntry reads a single term entry from the .tidx file at the given offset.
// Returns (termLen, docFreq, postingsOffset, postingsLength, termBytes).
func (ds *DiskSegment) readTermEntry(tidx *store.MMapIndexInput, offset int) (int, int, uint64, uint32, []byte) {
	tidx.Seek(offset)
	termLen, _ := tidx.ReadUint16()
	termBytes, _ := tidx.ReadBytes(int(termLen))
	docFreqVal, _ := tidx.ReadUint32()
	postingsOffset, _ := tidx.ReadUint64()
	postingsLength, _ := tidx.ReadUint32()
	return int(termLen), int(docFreqVal), postingsOffset, postingsLength, termBytes
}

// compareBytes compares two byte slices lexicographically.
func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// Compile-time check: DiskSegment implements SegmentReader.
var _ SegmentReader = (*DiskSegment)(nil)
