package index

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"gosearch/fst"
	"gosearch/store"
	"sort"
)

// WriteSegmentV2 persists a Segment to disk using the V2 format with offset tables.
//
// Files written per segment:
//   - {seg}.meta          — JSON metadata with format_version=2
//   - {seg}.{field}.tidx  — sorted term index with offset table
//   - {seg}.{field}.tdat  — delta-encoded postings data
//   - {seg}.{field}.flen  — fixed-width field lengths
//   - {seg}.stored        — stored fields with doc offset table
func WriteSegmentV2(dir store.Directory, seg *InMemorySegment) ([]string, error) {
	var files []string

	// 1. Write metadata
	meta := SegmentMeta{
		Name:          seg.name,
		DocCount:      seg.docCount,
	}
	for fieldName := range seg.fields {
		meta.Fields = append(meta.Fields, fieldName)
	}
	sort.Strings(meta.Fields)

	metaFileName := seg.name + ".meta"
	metaOut, err := dir.CreateOutput(metaFileName)
	if err != nil {
		return nil, err
	}
	metaBytes, _ := json.Marshal(meta)
	metaOut.Write(metaBytes)
	metaOut.Close()
	files = append(files, metaFileName)

	// 2. Write postings (tidx + tdat) for each field
	for _, fieldName := range meta.Fields {
		fi := seg.fields[fieldName]
		if err := writeFieldPostingsV2(dir, seg.name, fieldName, fi); err != nil {
			return nil, err
		}
		files = append(files,
			fmt.Sprintf("%s.%s.tidx", seg.name, fieldName),
			fmt.Sprintf("%s.%s.tdat", seg.name, fieldName),
		)
	}

	// 3. Write stored fields with offset table
	if err := writeStoredFieldsV2(dir, seg); err != nil {
		return nil, err
	}
	files = append(files, seg.name+".stored")

	// 4. Write field lengths (fixed-width)
	for _, fieldName := range meta.Fields {
		lengths := seg.fieldLengths[fieldName]
		if err := writeFieldLengthsV2(dir, seg.name, fieldName, lengths, seg.docCount); err != nil {
			return nil, err
		}
		files = append(files, fmt.Sprintf("%s.%s.flen", seg.name, fieldName))
	}

	return files, nil
}

// writeFieldPostingsV2 writes the term index (.tidx) and postings data (.tdat) files.
//
// .tidx format (FST-based):
//
//	[fst_size: uint32]           — size of serialized FST bytes
//	[fst_bytes: byte[fst_size]]  — FST mapping term → ordinal
//	[term_count: uint32]
//	[term_metadata: term_count × {
//	    doc_freq:         uint32   (4 bytes)
//	    postings_offset:  uint64   (8 bytes)
//	    postings_length:  uint32   (4 bytes)
//	}]                            — 16 bytes per entry, indexed by ordinal
//
// .tdat format:
//
//	[per term's postings:
//	  doc_id_delta: VInt
//	  freq: VInt
//	  position_count: VInt
//	  position_delta: VInt × N
//	]
func writeFieldPostingsV2(dir store.Directory, segName, fieldName string, fi *FieldIndex) error {
	// Sort terms alphabetically
	terms := make([]string, 0, len(fi.postings))
	for term := range fi.postings {
		terms = append(terms, term)
	}
	sort.Strings(terms)

	// First pass: write .tdat (postings data), recording offsets
	type termMeta struct {
		docFreq        int
		postingsOffset uint64
		postingsLength uint32
	}
	termMetas := make([]termMeta, len(terms))

	tdatBuf := &bytes.Buffer{}
	for i, term := range terms {
		pl := fi.postings[term]
		startOffset := uint64(tdatBuf.Len())

		// Write delta-encoded postings
		prevDocID := 0
		for _, posting := range pl.Postings {
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

		termMetas[i] = termMeta{
			docFreq:        len(pl.Postings),
			postingsOffset: startOffset,
			postingsLength: uint32(uint64(tdatBuf.Len()) - startOffset),
		}
	}

	// Write .tdat file
	tdatOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.tdat", segName, fieldName))
	if err != nil {
		return err
	}
	tdatOut.Write(tdatBuf.Bytes())
	tdatOut.Close()

	// Build FST: term → ordinal
	fstBuilder := fst.NewBuilder()
	for i, term := range terms {
		if err := fstBuilder.Add([]byte(term), uint64(i)); err != nil {
			return fmt.Errorf("fst build: %w", err)
		}
	}
	fstObj, err := fstBuilder.Finish()
	if err != nil {
		return fmt.Errorf("fst finish: %w", err)
	}

	// Serialize FST to buffer
	var fstBuf bytes.Buffer
	if _, err := fstObj.WriteTo(&fstBuf); err != nil {
		return fmt.Errorf("fst serialize: %w", err)
	}

	// Build .tidx: [fst_size][fst_bytes][term_count][term_metadata...]
	tidxBuf := &bytes.Buffer{}
	writeUint32ToBuffer(tidxBuf, uint32(fstBuf.Len()))
	tidxBuf.Write(fstBuf.Bytes())
	writeUint32ToBuffer(tidxBuf, uint32(len(terms)))
	for _, tm := range termMetas {
		writeUint32ToBuffer(tidxBuf, uint32(tm.docFreq))
		writeUint64ToBuffer(tidxBuf, tm.postingsOffset)
		writeUint32ToBuffer(tidxBuf, tm.postingsLength)
	}

	// Write .tidx file
	tidxOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.tidx", segName, fieldName))
	if err != nil {
		return err
	}
	tidxOut.Write(tidxBuf.Bytes())
	tidxOut.Close()

	return nil
}

// writeStoredFieldsV2 writes stored fields with a doc offset table.
//
// Format:
//
//	[doc_count: uint32]
//	[offset_table: doc_count × uint64]  — docID → byte offset of stored fields data
//	[stored_data: per doc VInt-encoded fields]
func writeStoredFieldsV2(dir store.Directory, seg *InMemorySegment) error {
	buf := &bytes.Buffer{}

	// Header
	writeUint32ToBuffer(buf, uint32(seg.docCount))

	// Reserve space for offset table
	offsetTableStart := buf.Len()
	for i := 0; i < seg.docCount; i++ {
		writeUint64ToBuffer(buf, 0) // placeholder
	}

	// Write stored fields data for each doc in order
	data := buf.Bytes()
	docOffsets := make([]uint64, seg.docCount)

	storedBuf := &bytes.Buffer{}
	for docID := 0; docID < seg.docCount; docID++ {
		docOffsets[docID] = uint64(len(data) + storedBuf.Len())
		fields := seg.storedFields[docID]
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

	// Fill in offset table
	for i, offset := range docOffsets {
		binary.LittleEndian.PutUint64(data[offsetTableStart+i*8:], offset)
	}

	out, err := dir.CreateOutput(seg.name + ".stored")
	if err != nil {
		return err
	}
	defer out.Close()
	out.Write(data)
	out.Write(storedBuf.Bytes())

	return nil
}

// writeFieldLengthsV2 writes field lengths as fixed-width uint32 values.
//
// Format:
//
//	[doc_count: uint32]
//	[lengths: doc_count × uint32]
func writeFieldLengthsV2(dir store.Directory, segName, fieldName string, lengths []int, docCount int) error {
	out, err := dir.CreateOutput(fmt.Sprintf("%s.%s.flen", segName, fieldName))
	if err != nil {
		return err
	}
	defer out.Close()

	out.WriteUint32(uint32(docCount))
	for i := 0; i < docCount; i++ {
		l := 0
		if i < len(lengths) {
			l = lengths[i]
		}
		out.WriteUint32(uint32(l))
	}

	return nil
}

// --- Buffer write helpers (for building files in memory before writing) ---

func writeVIntToBuffer(buf *bytes.Buffer, v int) {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], uint64(v))
	buf.Write(b[:n])
}

func writeUint16ToBuffer(buf *bytes.Buffer, v uint16) {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], v)
	buf.Write(b[:])
}

func writeUint32ToBuffer(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}

func writeUint64ToBuffer(buf *bytes.Buffer, v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	buf.Write(b[:])
}
