package index

import (
	"encoding/json"
	"fmt"
	"gosearch/store"
)

// SegmentMeta holds segment metadata persisted as JSON.
type SegmentMeta struct {
	Name     string   `json:"name"`
	DocCount int      `json:"doc_count"`
	Fields   []string `json:"fields"`
}

// WriteSegment persists a Segment to disk via the given Directory.
func WriteSegment(dir store.Directory, seg *Segment) error {
	// 1. Write metadata
	meta := SegmentMeta{
		Name:     seg.name,
		DocCount: seg.docCount,
	}
	for fieldName := range seg.fields {
		meta.Fields = append(meta.Fields, fieldName)
	}

	metaOut, err := dir.CreateOutput(seg.name + ".meta")
	if err != nil {
		return err
	}
	metaBytes, _ := json.Marshal(meta)
	metaOut.Write(metaBytes)
	metaOut.Close()

	// 2. Write postings for each field
	for fieldName, fi := range seg.fields {
		if err := writeFieldPostings(dir, seg.name, fieldName, fi); err != nil {
			return err
		}
	}

	// 3. Write stored fields
	if err := writeStoredFields(dir, seg); err != nil {
		return err
	}

	// 4. Write field lengths
	for fieldName, lengths := range seg.fieldLengths {
		if err := writeFieldLengths(dir, seg.name, fieldName, lengths); err != nil {
			return err
		}
	}

	return nil
}

// writeFieldPostings writes the inverted index for a field.
//
// Format:
//
//	[term_count: VInt]
//	for each term:
//	  [term_len: VInt][term_bytes: bytes]
//	  [posting_count: VInt]
//	  for each posting:
//	    [doc_id: VInt][freq: VInt]
//	    [position_count: VInt]
//	    for each position:
//	      [position: VInt]
func writeFieldPostings(dir store.Directory, segName, fieldName string, fi *FieldIndex) error {
	out, err := dir.CreateOutput(fmt.Sprintf("%s.field_%s.postings", segName, fieldName))
	if err != nil {
		return err
	}
	defer out.Close()

	out.WriteVInt(len(fi.postings))

	for term, pl := range fi.postings {
		termBytes := []byte(term)
		out.WriteVInt(len(termBytes))
		out.Write(termBytes)

		out.WriteVInt(len(pl.Postings))
		for _, posting := range pl.Postings {
			out.WriteVInt(posting.DocID)
			out.WriteVInt(posting.Freq)
			out.WriteVInt(len(posting.Positions))
			for _, pos := range posting.Positions {
				out.WriteVInt(pos)
			}
		}
	}

	return nil
}

// writeStoredFields writes stored field data.
func writeStoredFields(dir store.Directory, seg *Segment) error {
	out, err := dir.CreateOutput(seg.name + ".stored")
	if err != nil {
		return err
	}
	defer out.Close()

	out.WriteVInt(len(seg.storedFields))
	for docID, fields := range seg.storedFields {
		out.WriteVInt(docID)
		out.WriteVInt(len(fields))
		for name, value := range fields {
			nameBytes := []byte(name)
			out.WriteVInt(len(nameBytes))
			out.Write(nameBytes)
			valueBytes := []byte(value)
			out.WriteVInt(len(valueBytes))
			out.Write(valueBytes)
		}
	}

	return nil
}

// writeFieldLengths writes field length data (token counts per document).
func writeFieldLengths(dir store.Directory, segName, fieldName string, lengths []int) error {
	out, err := dir.CreateOutput(fmt.Sprintf("%s.field_%s.lengths", segName, fieldName))
	if err != nil {
		return err
	}
	defer out.Close()

	out.WriteVInt(len(lengths))
	for _, l := range lengths {
		out.WriteVInt(l)
	}

	return nil
}
