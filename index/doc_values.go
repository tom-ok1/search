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
}

// SortedDocValues provides access to deduplicated, sorted byte values per document.
type SortedDocValues interface {
	OrdValue(docID int) (int, error)
	LookupOrd(ord int) ([]byte, error)
	LookupTerm(term []byte) int // returns ord if exact match, or -(insertionPoint+1) if not found
	ValueCount() int
}

// diskNumericDocValues reads int64 values from an mmap'd .ndv file.
// Format: [values: docCount × int64][docCount: uint32]
type diskNumericDocValues struct {
	data     *store.MMapIndexInput
	docCount int
}

func (dv *diskNumericDocValues) Get(docID int) (int64, error) {
	if docID < 0 || docID >= dv.docCount {
		return 0, fmt.Errorf("docID %d out of range [0, %d)", docID, dv.docCount)
	}
	return dv.data.ReadInt64At(docID * 8)
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

// writeNumericDocValues writes per-doc int64 values in fixed-width format.
// Format: [values: docCount × int64 (little-endian)][docCount: uint32]
func writeNumericDocValues(dir store.Directory, segName, field string, values []int64, docCount int) error {
	out, err := dir.CreateOutput(fmt.Sprintf("%s.%s.ndv", segName, field))
	if err != nil {
		return err
	}
	defer out.Close()

	for i := range docCount {
		v := int64(0)
		if i < len(values) {
			v = values[i]
		}
		if err := out.WriteUint64(uint64(v)); err != nil {
			return fmt.Errorf("write ndv value: %w", err)
		}
	}

	if err := out.WriteUint32(uint32(docCount)); err != nil {
		return fmt.Errorf("write ndv trailer: %w", err)
	}
	return nil
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
