package transport

import "encoding/json"

// --- IndexDocument ---

type IndexDocumentRequest struct {
	Index         string
	ID            string
	Source        json.RawMessage
	IfSeqNo       *int64
	IfPrimaryTerm *int64
}

func (r *IndexDocumentRequest) WriteTo(out *StreamOutput) error {
	if err := out.WriteString(r.Index); err != nil {
		return err
	}
	if err := out.WriteString(r.ID); err != nil {
		return err
	}
	if err := out.WriteByteArray(r.Source); err != nil {
		return err
	}
	if err := out.WriteOptionalInt64(r.IfSeqNo); err != nil {
		return err
	}
	return out.WriteOptionalInt64(r.IfPrimaryTerm)
}

func ReadIndexDocumentRequest(in *StreamInput) (*IndexDocumentRequest, error) {
	index, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	id, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	source, err := in.ReadByteArray()
	if err != nil {
		return nil, err
	}
	ifSeqNo, err := in.ReadOptionalInt64()
	if err != nil {
		return nil, err
	}
	ifPrimaryTerm, err := in.ReadOptionalInt64()
	if err != nil {
		return nil, err
	}
	return &IndexDocumentRequest{
		Index:         index,
		ID:            id,
		Source:        json.RawMessage(source),
		IfSeqNo:       ifSeqNo,
		IfPrimaryTerm: ifPrimaryTerm,
	}, nil
}

type IndexDocumentResponse struct {
	Index       string
	ID          string
	SeqNo       int64
	PrimaryTerm int64
	Result      string
}

func (r *IndexDocumentResponse) WriteTo(out *StreamOutput) error {
	if err := out.WriteString(r.Index); err != nil {
		return err
	}
	if err := out.WriteString(r.ID); err != nil {
		return err
	}
	if err := out.WriteVLong(r.SeqNo); err != nil {
		return err
	}
	if err := out.WriteVLong(r.PrimaryTerm); err != nil {
		return err
	}
	return out.WriteString(r.Result)
}

func ReadIndexDocumentResponse(in *StreamInput) (*IndexDocumentResponse, error) {
	index, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	id, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	seqNo, err := in.ReadVLong()
	if err != nil {
		return nil, err
	}
	primaryTerm, err := in.ReadVLong()
	if err != nil {
		return nil, err
	}
	result, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	return &IndexDocumentResponse{
		Index:       index,
		ID:          id,
		SeqNo:       seqNo,
		PrimaryTerm: primaryTerm,
		Result:      result,
	}, nil
}

// --- GetDocument ---

type GetDocumentRequest struct {
	Index string
	ID    string
}

func (r *GetDocumentRequest) WriteTo(out *StreamOutput) error {
	if err := out.WriteString(r.Index); err != nil {
		return err
	}
	return out.WriteString(r.ID)
}

func ReadGetDocumentRequest(in *StreamInput) (*GetDocumentRequest, error) {
	index, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	id, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	return &GetDocumentRequest{
		Index: index,
		ID:    id,
	}, nil
}

type GetDocumentResponse struct {
	Index       string
	ID          string
	SeqNo       int64
	PrimaryTerm int64
	Found       bool
	Source      json.RawMessage
}

func (r *GetDocumentResponse) WriteTo(out *StreamOutput) error {
	if err := out.WriteString(r.Index); err != nil {
		return err
	}
	if err := out.WriteString(r.ID); err != nil {
		return err
	}
	if err := out.WriteVLong(r.SeqNo); err != nil {
		return err
	}
	if err := out.WriteVLong(r.PrimaryTerm); err != nil {
		return err
	}
	if err := out.WriteBool(r.Found); err != nil {
		return err
	}
	if r.Found {
		return out.WriteByteArray(r.Source)
	}
	return nil
}

func ReadGetDocumentResponse(in *StreamInput) (*GetDocumentResponse, error) {
	index, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	id, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	seqNo, err := in.ReadVLong()
	if err != nil {
		return nil, err
	}
	primaryTerm, err := in.ReadVLong()
	if err != nil {
		return nil, err
	}
	found, err := in.ReadBool()
	if err != nil {
		return nil, err
	}
	var source json.RawMessage
	if found {
		src, err := in.ReadByteArray()
		if err != nil {
			return nil, err
		}
		source = json.RawMessage(src)
	}
	return &GetDocumentResponse{
		Index:       index,
		ID:          id,
		SeqNo:       seqNo,
		PrimaryTerm: primaryTerm,
		Found:       found,
		Source:      source,
	}, nil
}

// --- Search ---

type SearchRequestMsg struct {
	Index     string
	QueryJSON map[string]any
	AggsJSON  map[string]any
	Size      int
}

func (r *SearchRequestMsg) WriteTo(out *StreamOutput) error {
	if err := out.WriteString(r.Index); err != nil {
		return err
	}
	if err := out.WriteGenericMap(r.QueryJSON); err != nil {
		return err
	}
	if err := out.WriteGenericMap(r.AggsJSON); err != nil {
		return err
	}
	return out.WriteVInt(int32(r.Size))
}

func ReadSearchRequestMsg(in *StreamInput) (*SearchRequestMsg, error) {
	index, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	queryJSON, err := in.ReadGenericMap()
	if err != nil {
		return nil, err
	}
	aggsJSON, err := in.ReadGenericMap()
	if err != nil {
		return nil, err
	}
	size, err := in.ReadVInt()
	if err != nil {
		return nil, err
	}
	return &SearchRequestMsg{
		Index:     index,
		QueryJSON: queryJSON,
		AggsJSON:  aggsJSON,
		Size:      int(size),
	}, nil
}

type SearchHitMsg struct {
	Index  string
	ID     string
	Score  float64
	Source json.RawMessage
}

type SearchResponseMsg struct {
	Took          int64
	TotalHits     int
	TotalRelation string
	MaxScore      float64
	Hits          []SearchHitMsg
	Aggregations  map[string]any
}

func (r *SearchResponseMsg) WriteTo(out *StreamOutput) error {
	if err := out.WriteVLong(r.Took); err != nil {
		return err
	}
	if err := out.WriteVInt(int32(r.TotalHits)); err != nil {
		return err
	}
	if err := out.WriteString(r.TotalRelation); err != nil {
		return err
	}
	if err := out.WriteFloat64(r.MaxScore); err != nil {
		return err
	}
	if err := out.WriteVInt(int32(len(r.Hits))); err != nil {
		return err
	}
	for _, hit := range r.Hits {
		if err := out.WriteString(hit.Index); err != nil {
			return err
		}
		if err := out.WriteString(hit.ID); err != nil {
			return err
		}
		if err := out.WriteFloat64(hit.Score); err != nil {
			return err
		}
		if err := out.WriteByteArray(hit.Source); err != nil {
			return err
		}
	}
	return out.WriteGenericMap(r.Aggregations)
}

func ReadSearchResponseMsg(in *StreamInput) (*SearchResponseMsg, error) {
	took, err := in.ReadVLong()
	if err != nil {
		return nil, err
	}
	totalHits, err := in.ReadVInt()
	if err != nil {
		return nil, err
	}
	totalRelation, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	maxScore, err := in.ReadFloat64()
	if err != nil {
		return nil, err
	}
	hitCount, err := in.ReadVInt()
	if err != nil {
		return nil, err
	}
	hits := make([]SearchHitMsg, hitCount)
	for i := range hits {
		index, err := in.ReadString()
		if err != nil {
			return nil, err
		}
		id, err := in.ReadString()
		if err != nil {
			return nil, err
		}
		score, err := in.ReadFloat64()
		if err != nil {
			return nil, err
		}
		source, err := in.ReadByteArray()
		if err != nil {
			return nil, err
		}
		hits[i] = SearchHitMsg{
			Index:  index,
			ID:     id,
			Score:  score,
			Source: json.RawMessage(source),
		}
	}
	aggs, err := in.ReadGenericMap()
	if err != nil {
		return nil, err
	}
	return &SearchResponseMsg{
		Took:          took,
		TotalHits:     int(totalHits),
		TotalRelation: totalRelation,
		MaxScore:      maxScore,
		Hits:          hits,
		Aggregations:  aggs,
	}, nil
}
