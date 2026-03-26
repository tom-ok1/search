package index

import (
	"encoding/json"
	"fmt"
	"gosearch/fst"
	"gosearch/store"
	"os"
	"sync/atomic"
)

// DiskSegment is a mmap-based lazy-loading SegmentReader.
// Only lightweight metadata is held in memory; all data is accessed
// via mmap'd files and decoded on demand.
type DiskSegment struct {
	refCount  atomic.Int32
	name      string
	docCount  int
	fieldList []string

	// Per-field mmap'd files
	termIndex    map[string]*store.MMapIndexInput // field → .tidx (metadata array)
	termFSTFiles map[string]*store.MMapIndexInput // field → .tfst (FST file)
	termData     map[string]*store.MMapIndexInput // field → .tdat
	fieldLens    map[string]*store.MMapIndexInput // field → .flen
	termFSTs     map[string]*fst.FST              // field → FST (term → ordinal)

	// Segment-level mmap'd files
	stored  *store.MMapIndexInput // .stored
	deleted *store.MMapIndexInput // .del (nil if no deletions)

	// Doc values mmap'd files
	numericDV    map[string]*store.MMapIndexInput // field → .ndv
	sortedDVOrd  map[string]*store.MMapIndexInput // field → .sdvo
	sortedDVDict map[string]*store.MMapIndexInput // field → .sdvd
	dvSkip       map[string]*store.MMapIndexInput // field → .ndvs or .sdvs
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
		name:         meta.Name,
		docCount:     meta.DocCount,
		fieldList:    meta.Fields,
		termIndex:    make(map[string]*store.MMapIndexInput),
		termFSTFiles: make(map[string]*store.MMapIndexInput),
		termData:     make(map[string]*store.MMapIndexInput),
		fieldLens:    make(map[string]*store.MMapIndexInput),
		termFSTs:     make(map[string]*fst.FST),
		numericDV:    make(map[string]*store.MMapIndexInput),
		sortedDVOrd:  make(map[string]*store.MMapIndexInput),
		sortedDVDict: make(map[string]*store.MMapIndexInput),
		dvSkip:       make(map[string]*store.MMapIndexInput),
	}

	// Mmap per-field files
	for _, field := range meta.Fields {
		tidx, err := store.OpenMMap(fmt.Sprintf("%s/%s.%s.tidx", dirPath, segName, field))
		if err != nil {
			ds.Close()
			return nil, fmt.Errorf("mmap tidx for %s: %w", field, err)
		}
		ds.termIndex[field] = tidx

		// Parse FST from separate .tfst file
		tfst, err := store.OpenMMap(fmt.Sprintf("%s/%s.%s.tfst", dirPath, segName, field))
		if err != nil {
			ds.Close()
			return nil, fmt.Errorf("mmap tfst for %s: %w", field, err)
		}
		ds.termFSTFiles[field] = tfst

		if tfst.Length() > 0 {
			termFST, err := fst.FSTFromInput(tfst)
			if err != nil {
				ds.Close()
				return nil, fmt.Errorf("parse FST for %s: %w", field, err)
			}
			ds.termFSTs[field] = termFST
		}

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

	// Mmap numeric doc values files
	for _, field := range meta.NumericDVFields {
		ndv, err := store.OpenMMap(fmt.Sprintf("%s/%s.%s.ndv", dirPath, segName, field))
		if err != nil {
			ds.Close()
			return nil, fmt.Errorf("mmap ndv for %s: %w", field, err)
		}
		ds.numericDV[field] = ndv

		ndvs, err := store.OpenMMap(fmt.Sprintf("%s/%s.%s.ndvs", dirPath, segName, field))
		if err != nil {
			ds.Close()
			return nil, fmt.Errorf("mmap ndvs for %s: %w", field, err)
		}
		ds.dvSkip[field] = ndvs
	}

	// Mmap sorted doc values files
	for _, field := range meta.SortedDVFields {
		sdvo, err := store.OpenMMap(fmt.Sprintf("%s/%s.%s.sdvo", dirPath, segName, field))
		if err != nil {
			ds.Close()
			return nil, fmt.Errorf("mmap sdvo for %s: %w", field, err)
		}
		ds.sortedDVOrd[field] = sdvo

		sdvd, err := store.OpenMMap(fmt.Sprintf("%s/%s.%s.sdvd", dirPath, segName, field))
		if err != nil {
			ds.Close()
			return nil, fmt.Errorf("mmap sdvd for %s: %w", field, err)
		}
		ds.sortedDVDict[field] = sdvd

		sdvs, err := store.OpenMMap(fmt.Sprintf("%s/%s.%s.sdvs", dirPath, segName, field))
		if err != nil {
			ds.Close()
			return nil, fmt.Errorf("mmap sdvs for %s: %w", field, err)
		}
		ds.dvSkip[field] = sdvs
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

	ds.refCount.Store(1)
	return ds, nil
}

// IncRef increments the reference count. The caller must call Close
// (which decrements the count) when it no longer needs this segment.
func (ds *DiskSegment) IncRef() {
	ds.refCount.Add(1)
}

// Close decrements the reference count and unmaps all memory-mapped
// files when the count reaches zero.
func (ds *DiskSegment) Close() error {
	if ds.refCount.Add(-1) > 0 {
		return nil
	}
	for _, m := range ds.termIndex {
		m.Close()
	}
	for _, m := range ds.termFSTFiles {
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
	for _, m := range ds.numericDV {
		m.Close()
	}
	for _, m := range ds.dvSkip {
		m.Close()
	}
	for _, m := range ds.sortedDVOrd {
		m.Close()
	}
	for _, m := range ds.sortedDVDict {
		m.Close()
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
	val, err := ds.deleted.ReadByteAt(byteIdx)
	if err != nil {
		return false
	}
	return val&(1<<bitIdx) != 0
}

// DocFreq looks up the term in the FST and returns doc_freq.
func (ds *DiskSegment) DocFreq(field, term string) int {
	tidx := ds.termIndex[field]
	termFST := ds.termFSTs[field]
	if tidx == nil || termFST == nil {
		return 0
	}

	_, docFreq, _, _ := ds.lookupTerm(tidx, termFST, term)
	return docFreq
}

// PostingsIterator returns an iterator that reads postings from the mmap'd .tdat file.
func (ds *DiskSegment) PostingsIterator(field, term string) PostingsIterator {
	tidx := ds.termIndex[field]
	tdat := ds.termData[field]
	termFST := ds.termFSTs[field]
	if tidx == nil || tdat == nil || termFST == nil {
		return EmptyPostingsIterator{}
	}

	found, docFreq, postingsOffset, postingsLength := ds.lookupTerm(tidx, termFST, term)
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
//
// Trailer format:
//
//	[data: per-doc VInt-encoded fields]
//	[offset_table: doc_count × uint64]
//	[doc_count: uint32]
func (ds *DiskSegment) StoredFields(docID int) (map[string][]byte, error) {
	if ds.stored == nil || docID >= ds.docCount {
		return nil, nil
	}

	// Offset table starts at: fileLen - 4 (doc_count) - docCount*8 (offsets).
	tableStart := ds.stored.Length() - 4 - ds.docCount*8
	offset, err := ds.stored.ReadUint64At(tableStart + docID*8)
	if err != nil {
		return nil, fmt.Errorf("read stored offset: %w", err)
	}

	ds.stored.Seek(int(offset))
	fieldCount, err := ds.stored.ReadVInt()
	if err != nil {
		return nil, fmt.Errorf("read field count: %w", err)
	}

	fields := make(map[string][]byte, fieldCount)
	for range fieldCount {
		nameLen, err := ds.stored.ReadVInt()
		if err != nil {
			return nil, fmt.Errorf("read field name length: %w", err)
		}
		nameBytes, err := ds.stored.ReadBytes(nameLen)
		if err != nil {
			return nil, fmt.Errorf("read field name: %w", err)
		}
		valueLen, err := ds.stored.ReadVInt()
		if err != nil {
			return nil, fmt.Errorf("read field value length: %w", err)
		}
		valueBytes, err := ds.stored.ReadBytes(valueLen)
		if err != nil {
			return nil, fmt.Errorf("read field value: %w", err)
		}
		fields[string(nameBytes)] = valueBytes
	}
	return fields, nil
}

// lookupTerm uses the FST to find a term's ordinal, then reads metadata
// from the flat array in the .tidx file.
// Returns (found, docFreq, postingsOffset, postingsLength).
func (ds *DiskSegment) lookupTerm(tidx *store.MMapIndexInput, termFST *fst.FST, term string) (bool, int, uint64, uint32) {
	ordinal, found := termFST.Get([]byte(term))
	if !found {
		return false, 0, 0, 0
	}

	// Read metadata at: ord*16
	// Each entry: doc_freq(4) + postings_offset(8) + postings_length(4) = 16 bytes
	offset := int(ordinal) * 16

	if offset+16 > tidx.Length() {
		return false, 0, 0, 0
	}

	docFreq32, err := tidx.ReadUint32At(offset)
	if err != nil {
		return false, 0, 0, 0
	}
	postingsOffset, err := tidx.ReadUint64At(offset + 4)
	if err != nil {
		return false, 0, 0, 0
	}
	postingsLength, err := tidx.ReadUint32At(offset + 12)
	if err != nil {
		return false, 0, 0, 0
	}

	return true, int(docFreq32), postingsOffset, postingsLength
}

// Fields returns the list of indexed fields.
func (ds *DiskSegment) Fields() []string {
	return ds.fieldList
}

// NumericDVFields returns the list of numeric doc values fields.
func (ds *DiskSegment) NumericDVFields() []string {
	fields := make([]string, 0, len(ds.numericDV))
	for f := range ds.numericDV {
		fields = append(fields, f)
	}
	return fields
}

// SortedDVFields returns the list of sorted doc values fields.
func (ds *DiskSegment) SortedDVFields() []string {
	fields := make([]string, 0, len(ds.sortedDVOrd))
	for f := range ds.sortedDVOrd {
		fields = append(fields, f)
	}
	return fields
}

func (ds *DiskSegment) DocValuesSkipper(field string) *DocValuesSkipper {
	data := ds.dvSkip[field]
	if data == nil {
		return nil
	}
	skipper, err := NewDocValuesSkipper(data)
	if err != nil {
		return nil
	}
	return skipper
}

func (ds *DiskSegment) NumericDocValues(field string) NumericDocValues {
	data := ds.numericDV[field]
	if data == nil {
		return nil
	}
	return &diskNumericDocValues{data: data, docCount: ds.docCount}
}

func (ds *DiskSegment) SortedDocValues(field string) SortedDocValues {
	ord := ds.sortedDVOrd[field]
	dict := ds.sortedDVDict[field]
	if ord == nil || dict == nil {
		return nil
	}
	sdv, err := newDiskSortedDocValues(ord, dict)
	if err != nil {
		return nil
	}
	return sdv
}

// Compile-time check: DiskSegment implements SegmentReader.
var _ SegmentReader = (*DiskSegment)(nil)
