package index

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"

	"gosearch/fst"
	"gosearch/index/bkd"
	"gosearch/store"
)

// WriteSegmentV2 persists a Segment to disk using the V2 format with offset tables.
//
// Files written per segment:
//   - {seg}.meta          — JSON metadata with format_version=2
//   - {seg}.{field}.tidx  — term metadata array (16 bytes per term)
//   - {seg}.{field}.tfst  — FST mapping term → ordinal
//   - {seg}.{field}.tdat  — delta-encoded postings data
//   - {seg}.{field}.flen  — fixed-width field lengths
//   - {seg}.stored        — stored fields with doc offset table
func WriteSegmentV2(dir store.Directory, seg *InMemorySegment) ([]string, []string, error) {
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

	for fieldName := range seg.numericDocValues {
		meta.NumericDVFields = append(meta.NumericDVFields, fieldName)
	}
	sort.Strings(meta.NumericDVFields)

	for fieldName := range seg.sortedDocValues {
		meta.SortedDVFields = append(meta.SortedDVFields, fieldName)
	}
	sort.Strings(meta.SortedDVFields)

	for fieldName := range seg.pointFields {
		meta.PointFields = append(meta.PointFields, fieldName)
	}
	sort.Strings(meta.PointFields)

	metaFileName, err := writeSegmentMeta(dir, meta)
	if err != nil {
		return nil, nil, err
	}
	files = append(files, metaFileName)

	// 2. Write postings (tidx + tfst + tdat) for each field
	for _, fieldName := range meta.Fields {
		fi := seg.fields[fieldName]
		if err := writeFieldPostingsV2(dir, seg.name, fieldName, fi); err != nil {
			return nil, nil, err
		}
		files = append(files,
			fmt.Sprintf("%s.%s.tidx", seg.name, fieldName),
			fmt.Sprintf("%s.%s.tfst", seg.name, fieldName),
			fmt.Sprintf("%s.%s.tdat", seg.name, fieldName),
		)
	}

	// 3. Write stored fields with offset table
	if err := writeStoredFieldsV2(dir, seg); err != nil {
		return nil, nil, err
	}
	files = append(files, seg.name+".stored")

	// 4. Write field lengths (fixed-width) for inverted index fields and point fields
	flenFields := make(map[string]struct{})
	for _, f := range meta.Fields {
		flenFields[f] = struct{}{}
	}
	for _, f := range meta.PointFields {
		flenFields[f] = struct{}{}
	}
	for fieldName := range flenFields {
		lengths := seg.fieldLengths[fieldName]
		if err := writeFieldLengthsV2(dir, seg.name, fieldName, lengths, seg.docCount); err != nil {
			return nil, nil, err
		}
		files = append(files, fmt.Sprintf("%s.%s.flen", seg.name, fieldName))
	}

	// 5. Write numeric doc values
	for _, fieldName := range meta.NumericDVFields {
		values := seg.numericDocValues[fieldName]
		if err := writeNumericDocValues(dir, seg.name, fieldName, values, seg.docCount); err != nil {
			return nil, nil, err
		}
		files = append(files, fmt.Sprintf("%s.%s.ndv", seg.name, fieldName))

		if _, isPoint := seg.pointFields[fieldName]; isPoint {
			w := bkd.NewBKDWriter()
			for docID, val := range values {
				w.Add(docID, val)
			}
			if err := w.Finish(dir, seg.name, fieldName); err != nil {
				return nil, nil, err
			}
			files = append(files, fmt.Sprintf("%s.%s.kd", seg.name, fieldName))
		} else {
			if err := writeNumericDocValuesSkipIndexFromNDV(dir, seg.name, fieldName, len(values)); err != nil {
				return nil, nil, err
			}
			files = append(files, fmt.Sprintf("%s.%s.ndvs", seg.name, fieldName))
		}
	}

	// 6. Write sorted doc values
	for _, fieldName := range meta.SortedDVFields {
		values := seg.sortedDocValues[fieldName]
		if err := writeSortedDocValues(dir, seg.name, fieldName, values, seg.docCount); err != nil {
			return nil, nil, err
		}
		if err := writeSortedDocValuesSkipIndexFromOrd(dir, seg.name, fieldName, seg.docCount); err != nil {
			return nil, nil, err
		}
		files = append(files,
			fmt.Sprintf("%s.%s.sdvo", seg.name, fieldName),
			fmt.Sprintf("%s.%s.sdvd", seg.name, fieldName),
			fmt.Sprintf("%s.%s.sdvs", seg.name, fieldName),
		)
	}

	return files, meta.Fields, nil
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
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		out.Close()
		return "", fmt.Errorf("marshal meta: %w", err)
	}
	if _, err := out.Write(metaBytes); err != nil {
		out.Close()
		return "", fmt.Errorf("write meta: %w", err)
	}
	if err := out.Close(); err != nil {
		return "", fmt.Errorf("close meta: %w", err)
	}
	return fileName, nil
}

// writeStoredFieldsFromMap writes stored fields using the trailer format.
//
// Format:
//
//	[data: per-doc VInt-encoded fields]
//	[offset_table: doc_count × uint64]  — docID → byte offset into data section
//	[doc_count: uint32]                 — footer
func writeStoredFieldsFromMap(dir store.Directory, segName string, docCount int, storedFields map[int]map[string][]byte) error {
	out, err := dir.CreateOutput(segName + ".stored")
	if err != nil {
		return err
	}
	defer out.Close()

	offsets := make([]uint64, docCount)
	var pos uint64
	scratch := &bytes.Buffer{}

	for docID := range docCount {
		offsets[docID] = pos
		fields := storedFields[docID]
		n, err := writeStoredFieldsEntry(out, fields, scratch)
		if err != nil {
			return fmt.Errorf("write stored fields for doc %d: %w", docID, err)
		}
		pos += n
	}

	// Trailer: offset table + doc_count.
	for _, offset := range offsets {
		if err := out.WriteUint64(offset); err != nil {
			return fmt.Errorf("write stored offset: %w", err)
		}
	}
	if err := out.WriteUint32(uint32(docCount)); err != nil {
		return fmt.Errorf("write stored doc count: %w", err)
	}

	return nil
}

// writeStoredFieldsEntry writes a single document's stored fields to out
// using the provided scratch buffer, and returns the number of bytes written.
// The caller must provide a non-nil scratch buffer; it will be Reset before use.
func writeStoredFieldsEntry(out store.IndexOutput, fields map[string][]byte, scratch *bytes.Buffer) (uint64, error) {
	scratch.Reset()
	writeVIntToBuffer(scratch, len(fields))
	for name, value := range fields {
		nameBytes := []byte(name)
		writeVIntToBuffer(scratch, len(nameBytes))
		scratch.Write(nameBytes)
		writeVIntToBuffer(scratch, len(value))
		scratch.Write(value)
	}
	if _, err := out.Write(scratch.Bytes()); err != nil {
		return 0, err
	}
	return uint64(scratch.Len()), nil
}

// writeFieldPostingsV2 writes per-field postings and term index files.
//
// .tidx format (flat metadata array, term count inferred from file size):
//
//	[term_metadata: N × {
//	    doc_freq:         uint32   (4 bytes)
//	    postings_offset:  uint64   (8 bytes)
//	    postings_length:  uint32   (4 bytes)
//	}]                            — 16 bytes per entry, indexed by ordinal
//
// .tfst format (raw FST bytes):
//
//	[fst_bytes]                   — FST mapping term → ordinal
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
	if len(fi.postings) == 0 {
		return nil
	}
	terms := make([]string, 0, len(fi.postings))
	for term := range fi.postings {
		terms = append(terms, term)
	}
	sort.Strings(terms)

	tdatBuf := &bytes.Buffer{}
	tidxOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.tidx", segName, fieldName))
	if err != nil {
		return err
	}
	defer tidxOut.Close()

	// Stream FST bytes directly to .tfst file.
	tfstOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.tfst", segName, fieldName))
	if err != nil {
		return err
	}
	defer tfstOut.Close()

	fstBuilder := fst.NewBuilderWithWriter(tfstOut)

	for i, term := range terms {
		pl := fi.postings[term]
		startOffset, length := writePostingsToBuffer(tdatBuf, pl.Postings)

		// Stream metadata to .tidx
		if err := tidxOut.WriteUint32(uint32(len(pl.Postings))); err != nil {
			return fmt.Errorf("write tidx: %w", err)
		}
		if err := tidxOut.WriteUint64(startOffset); err != nil {
			return fmt.Errorf("write tidx: %w", err)
		}
		if err := tidxOut.WriteUint32(length); err != nil {
			return fmt.Errorf("write tidx: %w", err)
		}

		if err := fstBuilder.Add([]byte(term), uint64(i)); err != nil {
			return fmt.Errorf("fst build: %w", err)
		}
	}

	// Write .tdat
	tdatOut, err := dir.CreateOutput(fmt.Sprintf("%s.%s.tdat", segName, fieldName))
	if err != nil {
		return err
	}
	defer tdatOut.Close()

	if _, err := tdatOut.Write(tdatBuf.Bytes()); err != nil {
		return fmt.Errorf("write tdat: %w", err)
	}

	// Finish writes the trailer to .tfst.
	if err := fstBuilder.Finish(); err != nil {
		return fmt.Errorf("fst finish: %w", err)
	}
	return nil
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

	if err := out.WriteUint32(uint32(docCount)); err != nil {
		return fmt.Errorf("write flen doc count: %w", err)
	}
	for i := range docCount {
		l := 0
		if i < len(lengths) {
			l = lengths[i]
		}
		if err := out.WriteUint32(uint32(l)); err != nil {
			return fmt.Errorf("write flen: %w", err)
		}
	}

	return nil
}

// --- Buffer write helpers (for building files in memory before writing) ---

func writeVIntToBuffer(buf *bytes.Buffer, v int) {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], uint64(v))
	buf.Write(b[:n])
}
