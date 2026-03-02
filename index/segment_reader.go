package index

import (
	"encoding/json"
	"fmt"
	"gosearch/store"
	"io"
)

// ReadSegment loads a Segment from disk via the given Directory.
func ReadSegment(dir store.Directory, segName string) (*Segment, error) {
	// 1. Read metadata
	metaIn, err := dir.OpenInput(segName + ".meta")
	if err != nil {
		return nil, err
	}
	defer metaIn.Close()

	metaBytes, _ := io.ReadAll(metaIn)
	var meta SegmentMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, err
	}

	seg := newSegment(meta.Name)
	seg.docCount = meta.DocCount

	// 2. Read postings for each field
	for _, fieldName := range meta.Fields {
		fi, err := readFieldPostings(dir, segName, fieldName)
		if err != nil {
			return nil, err
		}
		seg.fields[fieldName] = fi
	}

	// 3. Read stored fields
	if err := readStoredFields(dir, seg); err != nil {
		return nil, err
	}

	// 4. Read field lengths
	for _, fieldName := range meta.Fields {
		lengths, err := readFieldLengths(dir, segName, fieldName)
		if err != nil {
			continue // skip if lengths file doesn't exist
		}
		seg.fieldLengths[fieldName] = lengths
	}

	return seg, nil
}

func readFieldPostings(dir store.Directory, segName, fieldName string) (*FieldIndex, error) {
	in, err := dir.OpenInput(fmt.Sprintf("%s.field_%s.postings", segName, fieldName))
	if err != nil {
		return nil, err
	}
	defer in.Close()

	fi := newFieldIndex()

	termCount, err := in.ReadVInt()
	if err != nil {
		return nil, err
	}

	for i := 0; i < termCount; i++ {
		termLen, _ := in.ReadVInt()
		termBytes := make([]byte, termLen)
		io.ReadFull(in, termBytes)
		term := string(termBytes)

		postingCount, _ := in.ReadVInt()
		pl := &PostingsList{Term: term}

		for j := 0; j < postingCount; j++ {
			docID, _ := in.ReadVInt()
			freq, _ := in.ReadVInt()
			posCount, _ := in.ReadVInt()

			positions := make([]int, posCount)
			for k := 0; k < posCount; k++ {
				positions[k], _ = in.ReadVInt()
			}

			pl.Postings = append(pl.Postings, Posting{
				DocID:     docID,
				Freq:      freq,
				Positions: positions,
			})
		}

		fi.postings[term] = pl
	}

	return fi, nil
}

func readStoredFields(dir store.Directory, seg *Segment) error {
	in, err := dir.OpenInput(seg.name + ".stored")
	if err != nil {
		return err
	}
	defer in.Close()

	docCount, _ := in.ReadVInt()
	for i := 0; i < docCount; i++ {
		docID, _ := in.ReadVInt()
		fieldCount, _ := in.ReadVInt()

		fields := make(map[string]string)
		for j := 0; j < fieldCount; j++ {
			nameLen, _ := in.ReadVInt()
			nameBytes := make([]byte, nameLen)
			io.ReadFull(in, nameBytes)

			valueLen, _ := in.ReadVInt()
			valueBytes := make([]byte, valueLen)
			io.ReadFull(in, valueBytes)

			fields[string(nameBytes)] = string(valueBytes)
		}
		seg.storedFields[docID] = fields
	}

	return nil
}

func readFieldLengths(dir store.Directory, segName, fieldName string) ([]int, error) {
	in, err := dir.OpenInput(fmt.Sprintf("%s.field_%s.lengths", segName, fieldName))
	if err != nil {
		return nil, err
	}
	defer in.Close()

	count, _ := in.ReadVInt()
	lengths := make([]int, count)
	for i := 0; i < count; i++ {
		lengths[i], _ = in.ReadVInt()
	}

	return lengths, nil
}
