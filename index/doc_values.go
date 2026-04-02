package index

import (
	"bytes"
	"fmt"
	gosort "sort"

	"gosearch/store"
)

// DocValuesType represents the type of a doc values field.
type DocValuesType int

const (
	DocValuesNone    DocValuesType = iota
	DocValuesNumeric               // per-document int64
	DocValuesSorted                // deduplicated string values with ordinal mapping
)

// NumericDocValues provides random access to per-document int64 values.
type NumericDocValues interface {
	Get(docID int) (int64, error)
	// HasValue reports whether docID has a value for this field.
	// For dense fields (every doc has a value), this always returns true.
	HasValue(docID int) bool
}

// SortedDocValues provides access to deduplicated, sorted byte values per document.
type SortedDocValues interface {
	OrdValue(docID int) (int, error)
	LookupOrd(ord int) ([]byte, error)
	LookupTerm(term []byte) int // returns ord if exact match, or -(insertionPoint+1) if not found
	ValueCount() int
}

const (
	ndvModeEmpty  byte = 0
	ndvModeDense  byte = 1
	ndvModeSparse byte = 2
)

// diskNumericDocValues reads int64 values from an mmap'd .ndv file.
// Format: [mode: 1 byte][data...][docCount: uint32]
type diskNumericDocValues struct {
	data         *store.MMapIndexInput
	docCount     int
	mode         byte
	valuesOffset int
	presence     *Bitset // nil means all docs have values (dense) or no docs have values (empty)
}

func (dv *diskNumericDocValues) Get(docID int) (int64, error) {
	if docID < 0 || docID >= dv.docCount {
		return 0, fmt.Errorf("docID %d out of range [0, %d)", docID, dv.docCount)
	}
	switch dv.mode {
	case ndvModeEmpty:
		return 0, nil
	case ndvModeDense:
		return dv.data.ReadInt64At(dv.valuesOffset + docID*8)
	case ndvModeSparse:
		if !dv.presence.Get(docID) {
			return 0, nil
		}
		rank := dv.presence.Rank(docID)
		return dv.data.ReadInt64At(dv.valuesOffset + rank*8)
	default:
		return 0, fmt.Errorf("unknown ndv mode %d", dv.mode)
	}
}

func (dv *diskNumericDocValues) HasValue(docID int) bool {
	switch dv.mode {
	case ndvModeEmpty:
		return false
	case ndvModeDense:
		return true
	case ndvModeSparse:
		return dv.presence.Get(docID)
	default:
		return false
	}
}

// diskSortedDocValues reads ordinals from .sdvo and dictionary from .sdvd.
type diskSortedDocValues struct {
	ordinals   *store.MMapIndexInput // .sdvo file
	dictionary *store.MMapIndexInput // .sdvd file
	docCount   int
	valueCount int
	dataLen    int // byte length of the value_data section in .sdvd
}

func newDiskSortedDocValues(ordinals, dictionary *store.MMapIndexInput) (*diskSortedDocValues, error) {
	// Read docCount from .sdvo trailer
	sdvoLen := ordinals.Length()
	docCount, err := ordinals.ReadUint32At(sdvoLen - 4)
	if err != nil {
		return nil, fmt.Errorf("read sdvo trailer: %w", err)
	}

	// Read valueCount from .sdvd trailer
	sdvdLen := dictionary.Length()
	valueCount, err := dictionary.ReadUint32At(sdvdLen - 4)
	if err != nil {
		return nil, fmt.Errorf("read sdvd trailer: %w", err)
	}

	// dataLen = total - offset_table - trailer
	// offset_table: valueCount × 8 bytes, trailer: 4 bytes
	dataLen := sdvdLen - int(valueCount)*8 - 4

	return &diskSortedDocValues{
		ordinals:   ordinals,
		dictionary: dictionary,
		docCount:   int(docCount),
		valueCount: int(valueCount),
		dataLen:    dataLen,
	}, nil
}

func (dv *diskSortedDocValues) OrdValue(docID int) (int, error) {
	if docID < 0 || docID >= dv.docCount {
		return -1, fmt.Errorf("docID %d out of range [0, %d)", docID, dv.docCount)
	}
	v, err := dv.ordinals.ReadInt32At(docID * 4)
	if err != nil {
		return -1, err
	}
	return int(v), nil
}

func (dv *diskSortedDocValues) LookupOrd(ord int) ([]byte, error) {
	if ord < 0 || ord >= dv.valueCount {
		return nil, fmt.Errorf("ordinal %d out of range [0, %d)", ord, dv.valueCount)
	}

	// Read offset from the offset table
	// offset_table starts at dataLen, each entry is uint64
	offsetTableStart := dv.dataLen
	offset, err := dv.dictionary.ReadUint64At(offsetTableStart + ord*8)
	if err != nil {
		return nil, fmt.Errorf("read offset for ord %d: %w", ord, err)
	}

	// Determine end of this value
	var end uint64
	if ord+1 < dv.valueCount {
		end, err = dv.dictionary.ReadUint64At(offsetTableStart + (ord+1)*8)
		if err != nil {
			return nil, fmt.Errorf("read end offset for ord %d: %w", ord, err)
		}
	} else {
		end = uint64(dv.dataLen)
	}

	length := int(end - offset)
	slice, err := dv.dictionary.Slice(int(offset), length)
	if err != nil {
		return nil, fmt.Errorf("slice dict for ord %d: %w", ord, err)
	}
	return slice.ReadBytes(length)
}

func (dv *diskSortedDocValues) LookupTerm(term []byte) int {
	lo, hi := 0, dv.valueCount
	for lo < hi {
		mid := lo + (hi-lo)/2
		val, err := dv.LookupOrd(mid)
		if err != nil {
			return -(lo + 1)
		}
		c := bytes.Compare(val, term)
		if c < 0 {
			lo = mid + 1
		} else if c > 0 {
			hi = mid
		} else {
			return mid
		}
	}
	return -(lo + 1)
}

func (dv *diskSortedDocValues) ValueCount() int {
	return dv.valueCount
}

// writeNumericDocValues writes per-doc int64 values with dense/sparse/empty mode.
// Format: [mode: 1 byte][data...][docCount: uint32]
//   - presence=nil → dense mode (all docs have values)
//   - presence non-nil, len==0 → empty mode
//   - presence non-nil, len==docCount → dense mode
//   - presence non-nil, 0 < len < docCount → sparse mode
func writeNumericDocValues(dir store.Directory, segName, field string, values []int64, docCount int, presence map[int]struct{}) error {
	out, err := dir.CreateOutput(fmt.Sprintf("%s.%s.ndv", segName, field))
	if err != nil {
		return err
	}
	defer out.Close()

	// Determine mode
	var mode byte
	switch {
	case presence == nil || len(presence) == docCount:
		mode = ndvModeDense
	case len(presence) == 0:
		mode = ndvModeEmpty
	default:
		mode = ndvModeSparse
	}

	// Write mode byte
	if _, err := out.Write([]byte{mode}); err != nil {
		return fmt.Errorf("write ndv mode: %w", err)
	}

	switch mode {
	case ndvModeEmpty:
		// Nothing to write

	case ndvModeDense:
		for i := range docCount {
			v := int64(0)
			if i < len(values) {
				v = values[i]
			}
			if err := out.WriteUint64(uint64(v)); err != nil {
				return fmt.Errorf("write ndv value: %w", err)
			}
		}

	case ndvModeSparse:
		// Write bitset
		bs := NewBitset(docCount)
		for docID := range presence {
			bs.Set(docID)
		}
		if _, err := out.Write(bs.Bytes()); err != nil {
			return fmt.Errorf("write ndv bitset: %w", err)
		}

		// Write only values for docs in presence, in docID order
		for i := range docCount {
			if _, ok := presence[i]; ok {
				v := int64(0)
				if i < len(values) {
					v = values[i]
				}
				if err := out.WriteUint64(uint64(v)); err != nil {
					return fmt.Errorf("write ndv sparse value: %w", err)
				}
			}
		}
	}

	// Trailer: docCount as uint32
	if err := out.WriteUint32(uint32(docCount)); err != nil {
		return fmt.Errorf("write ndv trailer: %w", err)
	}
	return nil
}

// readNumericDocValues reads a diskNumericDocValues from an mmap'd .ndv file.
func readNumericDocValues(data *store.MMapIndexInput) (*diskNumericDocValues, error) {
	length := data.Length()

	// Read trailer: docCount from last 4 bytes
	docCountU32, err := data.ReadUint32At(length - 4)
	if err != nil {
		return nil, fmt.Errorf("read ndv trailer: %w", err)
	}
	docCount := int(docCountU32)

	// Read mode byte at offset 0
	mode, err := data.ReadByteAt(0)
	if err != nil {
		return nil, fmt.Errorf("read ndv mode: %w", err)
	}

	dv := &diskNumericDocValues{
		data:     data,
		docCount: docCount,
		mode:     mode,
	}

	switch mode {
	case ndvModeEmpty:
		// No values to read

	case ndvModeDense:
		dv.valuesOffset = 1

	case ndvModeSparse:
		bitsetLen := (docCount + 7) / 8
		slice, err := data.Slice(1, bitsetLen)
		if err != nil {
			return nil, fmt.Errorf("slice ndv bitset: %w", err)
		}
		raw, err := slice.ReadBytes(bitsetLen)
		if err != nil {
			return nil, fmt.Errorf("read ndv bitset: %w", err)
		}
		bs := BitsetFromBytes(raw, docCount)
		bs.BuildRankTable()
		dv.presence = bs
		dv.valuesOffset = 1 + bitsetLen

	default:
		return nil, fmt.Errorf("unknown ndv mode %d", mode)
	}

	return dv, nil
}

// writeSortedDocValues writes deduplicated sorted string values.
// Writes two files:
//
//	{seg}.{field}.sdvo: [ordinals: docCount × int32][docCount: uint32]
//	{seg}.{field}.sdvd: [value_data][offset_table: valueCount × uint64][valueCount: uint32]
func writeSortedDocValues(dir store.Directory, segName, field string, values []string, docCount int) error {
	// Build dictionary: sort unique values, assign ordinals
	uniqueSet := make(map[string]struct{})
	var uniqueVals []string
	for _, v := range values {
		if v == "" {
			continue
		}

		if _, exists := uniqueSet[v]; !exists {
			uniqueSet[v] = struct{}{}
			uniqueVals = append(uniqueVals, v)
		}
	}
	gosort.Strings(uniqueVals)

	ordMap := make(map[string]int32, len(uniqueVals))
	for i, v := range uniqueVals {
		ordMap[v] = int32(i)
	}

	// Write .sdvo (ordinals)
	sdvoOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.sdvo", segName, field))
	if err != nil {
		return err
	}
	defer sdvoOut.Close()

	for i := range docCount {
		ord := int32(-1) // missing value
		if i < len(values) && values[i] != "" {
			ord = ordMap[values[i]]
		}
		if err := sdvoOut.WriteUint32(uint32(ord)); err != nil {
			return fmt.Errorf("write sdvo ordinal: %w", err)
		}
	}
	if err := sdvoOut.WriteUint32(uint32(docCount)); err != nil {
		return fmt.Errorf("write sdvo trailer: %w", err)
	}

	// Write .sdvd (dictionary)
	sdvdOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.sdvd", segName, field))
	if err != nil {
		return err
	}
	defer sdvdOut.Close()

	// Write value data and collect offsets
	offsets := make([]uint64, len(uniqueVals))
	var pos uint64
	for i, v := range uniqueVals {
		offsets[i] = pos
		b := []byte(v)
		if _, err := sdvdOut.Write(b); err != nil {
			return fmt.Errorf("write sdvd value: %w", err)
		}
		pos += uint64(len(b))
	}

	// Write offset table
	for _, offset := range offsets {
		if err := sdvdOut.WriteUint64(offset); err != nil {
			return fmt.Errorf("write sdvd offset: %w", err)
		}
	}

	// Write trailer
	if err := sdvdOut.WriteUint32(uint32(len(uniqueVals))); err != nil {
		return fmt.Errorf("write sdvd trailer: %w", err)
	}
	return nil
}
