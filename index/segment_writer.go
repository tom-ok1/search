package index

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"

	"gosearch/fst"
	"gosearch/store"
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
		Name:     seg.name,
		DocCount: seg.docCount,
	}
	for fieldName := range seg.fields {
		meta.Fields = append(meta.Fields, fieldName)
	}
	sort.Strings(meta.Fields)

	metaFileName, err := writeSegmentMeta(dir, meta)
	if err != nil {
		return nil, err
	}
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

// termMeta holds per-term metadata used when writing .tidx files.
type termMeta struct {
	term           string
	docFreq        int
	postingsOffset uint64
	postingsLength uint32
}

// writePostingsToBuffer encodes a slice of postings using delta-encoding and
// appends them to buf. Returns the start offset and length written.
func writePostingsToBuffer(buf *bytes.Buffer, postings []Posting) (startOffset uint64, length uint32) {
	startOffset = uint64(buf.Len())
	prevDocID := 0
	for _, posting := range postings {
		writeVIntToBuffer(buf, posting.DocID-prevDocID)
		prevDocID = posting.DocID
		writeVIntToBuffer(buf, posting.Freq)
		writeVIntToBuffer(buf, len(posting.Positions))
		prevPos := 0
		for _, pos := range posting.Positions {
			writeVIntToBuffer(buf, pos-prevPos)
			prevPos = pos
		}
	}
	length = uint32(uint64(buf.Len()) - startOffset)
	return
}

// writeSegmentMeta writes a .meta JSON file and returns the file name.
func writeSegmentMeta(dir store.Directory, meta SegmentMeta) (string, error) {
	fileName := meta.Name + ".meta"
	out, err := dir.CreateOutput(fileName)
	if err != nil {
		return "", err
	}
	metaBytes, _ := json.Marshal(meta)
	out.Write(metaBytes)
	out.Close()
	return fileName, nil
}

// writeTermIndexFile writes the .tdat file from the postings buffer and then
// builds and writes the .tidx file from term metadata.
func writeTermIndexFile(dir store.Directory, segName, field string, tdatBuf *bytes.Buffer, metas []termMeta) error {
	// Write .tdat file.
	tdatOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.tdat", segName, field))
	if err != nil {
		return err
	}
	tdatOut.Write(tdatBuf.Bytes())
	tdatOut.Close()

	return writeTermIndex(dir, segName, field, metas)
}

// writeTermIndex builds the FST and writes the .tidx file from term metadata.
func writeTermIndex(dir store.Directory, segName, field string, metas []termMeta) error {
	// Build FST: term → ordinal.
	fstBuilder := fst.NewBuilder()
	for i, tm := range metas {
		if err := fstBuilder.Add([]byte(tm.term), uint64(i)); err != nil {
			return fmt.Errorf("fst build: %w", err)
		}
	}
	fstBytes, err := fstBuilder.Finish()
	if err != nil {
		return fmt.Errorf("fst finish: %w", err)
	}

	// Build .tidx: [fst_size][fst_bytes][term_count][term_metadata...].
	tidxBuf := &bytes.Buffer{}
	writeUint32ToBuffer(tidxBuf, uint32(len(fstBytes)))
	tidxBuf.Write(fstBytes)
	writeUint32ToBuffer(tidxBuf, uint32(len(metas)))
	for _, tm := range metas {
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

// writeStoredFieldsFromMap writes stored fields using the trailer format.
//
// Format:
//
//	[data: per-doc VInt-encoded fields]
//	[offset_table: doc_count × uint64]  — docID → byte offset into data section
//	[doc_count: uint32]                 — footer
func writeStoredFieldsFromMap(dir store.Directory, segName string, docCount int, storedFields map[int]map[string]string) error {
	out, err := dir.CreateOutput(segName + ".stored")
	if err != nil {
		return err
	}
	defer out.Close()

	offsets := make([]uint64, docCount)
	var pos uint64
	scratch := &bytes.Buffer{}

	for docID := 0; docID < docCount; docID++ {
		offsets[docID] = pos
		fields := storedFields[docID]
		pos += writeStoredFieldsEntry(out, fields, scratch)
	}

	// Trailer: offset table + doc_count.
	for _, offset := range offsets {
		out.WriteUint64(offset)
	}
	out.WriteUint32(uint32(docCount))

	return nil
}

// writeStoredFieldsEntry writes a single document's stored fields to out
// using the provided scratch buffer, and returns the number of bytes written.
// The caller must provide a non-nil scratch buffer; it will be Reset before use.
func writeStoredFieldsEntry(out store.IndexOutput, fields map[string]string, scratch *bytes.Buffer) uint64 {
	scratch.Reset()
	writeVIntToBuffer(scratch, len(fields))
	for name, value := range fields {
		nameBytes := []byte(name)
		writeVIntToBuffer(scratch, len(nameBytes))
		scratch.Write(nameBytes)
		valueBytes := []byte(value)
		writeVIntToBuffer(scratch, len(valueBytes))
		scratch.Write(valueBytes)
	}
	out.Write(scratch.Bytes())
	return uint64(scratch.Len())
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
	terms := make([]string, 0, len(fi.postings))
	for term := range fi.postings {
		terms = append(terms, term)
	}
	sort.Strings(terms)

	tdatBuf := &bytes.Buffer{}
	metas := make([]termMeta, len(terms))

	for i, term := range terms {
		pl := fi.postings[term]
		startOffset, length := writePostingsToBuffer(tdatBuf, pl.Postings)
		metas[i] = termMeta{
			term:           term,
			docFreq:        len(pl.Postings),
			postingsOffset: startOffset,
			postingsLength: length,
		}
	}

	return writeTermIndexFile(dir, segName, fieldName, tdatBuf, metas)
}

// writeStoredFieldsV2 writes stored fields with a doc offset table.
func writeStoredFieldsV2(dir store.Directory, seg *InMemorySegment) error {
	return writeStoredFieldsFromMap(dir, seg.name, seg.docCount, seg.storedFields)
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
