package transport

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestIndexDocumentRequest_Roundtrip(t *testing.T) {
	seqNo := int64(5)
	primaryTerm := int64(1)
	source := json.RawMessage(`{"field":"value"}`)

	req := &IndexDocumentRequest{
		Index:         "test-index",
		ID:            "doc-1",
		Source:        source,
		IfSeqNo:       &seqNo,
		IfPrimaryTerm: &primaryTerm,
	}

	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := req.WriteTo(out); err != nil {
		t.Fatal(err)
	}

	in := NewStreamInput(&buf)
	got, err := ReadIndexDocumentRequest(in)
	if err != nil {
		t.Fatal(err)
	}

	if got.Index != req.Index {
		t.Errorf("Index: expected %q, got %q", req.Index, got.Index)
	}
	if got.ID != req.ID {
		t.Errorf("ID: expected %q, got %q", req.ID, got.ID)
	}
	if !bytes.Equal(got.Source, req.Source) {
		t.Errorf("Source: expected %s, got %s", req.Source, got.Source)
	}
	if got.IfSeqNo == nil || *got.IfSeqNo != *req.IfSeqNo {
		t.Errorf("IfSeqNo: expected %v, got %v", req.IfSeqNo, got.IfSeqNo)
	}
	if got.IfPrimaryTerm == nil || *got.IfPrimaryTerm != *req.IfPrimaryTerm {
		t.Errorf("IfPrimaryTerm: expected %v, got %v", req.IfPrimaryTerm, got.IfPrimaryTerm)
	}
}

func TestIndexDocumentRequest_Roundtrip_NilOptionals(t *testing.T) {
	source := json.RawMessage(`{"field":"value"}`)

	req := &IndexDocumentRequest{
		Index:         "test-index",
		ID:            "doc-1",
		Source:        source,
		IfSeqNo:       nil,
		IfPrimaryTerm: nil,
	}

	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := req.WriteTo(out); err != nil {
		t.Fatal(err)
	}

	in := NewStreamInput(&buf)
	got, err := ReadIndexDocumentRequest(in)
	if err != nil {
		t.Fatal(err)
	}

	if got.Index != req.Index {
		t.Errorf("Index: expected %q, got %q", req.Index, got.Index)
	}
	if got.ID != req.ID {
		t.Errorf("ID: expected %q, got %q", req.ID, got.ID)
	}
	if !bytes.Equal(got.Source, req.Source) {
		t.Errorf("Source: expected %s, got %s", req.Source, got.Source)
	}
	if got.IfSeqNo != nil {
		t.Errorf("IfSeqNo: expected nil, got %v", *got.IfSeqNo)
	}
	if got.IfPrimaryTerm != nil {
		t.Errorf("IfPrimaryTerm: expected nil, got %v", *got.IfPrimaryTerm)
	}
}

func TestIndexDocumentResponse_Roundtrip(t *testing.T) {
	resp := &IndexDocumentResponse{
		Index:       "test-index",
		ID:          "doc-1",
		SeqNo:       10,
		PrimaryTerm: 1,
		Result:      "created",
	}

	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := resp.WriteTo(out); err != nil {
		t.Fatal(err)
	}

	in := NewStreamInput(&buf)
	got, err := ReadIndexDocumentResponse(in)
	if err != nil {
		t.Fatal(err)
	}

	if got.Index != resp.Index {
		t.Errorf("Index: expected %q, got %q", resp.Index, got.Index)
	}
	if got.ID != resp.ID {
		t.Errorf("ID: expected %q, got %q", resp.ID, got.ID)
	}
	if got.SeqNo != resp.SeqNo {
		t.Errorf("SeqNo: expected %d, got %d", resp.SeqNo, got.SeqNo)
	}
	if got.PrimaryTerm != resp.PrimaryTerm {
		t.Errorf("PrimaryTerm: expected %d, got %d", resp.PrimaryTerm, got.PrimaryTerm)
	}
	if got.Result != resp.Result {
		t.Errorf("Result: expected %q, got %q", resp.Result, got.Result)
	}
}

func TestGetDocumentRequest_Roundtrip(t *testing.T) {
	req := &GetDocumentRequest{
		Index: "test-index",
		ID:    "doc-1",
	}

	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := req.WriteTo(out); err != nil {
		t.Fatal(err)
	}

	in := NewStreamInput(&buf)
	got, err := ReadGetDocumentRequest(in)
	if err != nil {
		t.Fatal(err)
	}

	if got.Index != req.Index {
		t.Errorf("Index: expected %q, got %q", req.Index, got.Index)
	}
	if got.ID != req.ID {
		t.Errorf("ID: expected %q, got %q", req.ID, got.ID)
	}
}

func TestGetDocumentResponse_Roundtrip_Found(t *testing.T) {
	source := json.RawMessage(`{"field":"value"}`)

	resp := &GetDocumentResponse{
		Index:       "test-index",
		ID:          "doc-1",
		SeqNo:       5,
		PrimaryTerm: 1,
		Found:       true,
		Source:      source,
	}

	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := resp.WriteTo(out); err != nil {
		t.Fatal(err)
	}

	in := NewStreamInput(&buf)
	got, err := ReadGetDocumentResponse(in)
	if err != nil {
		t.Fatal(err)
	}

	if got.Index != resp.Index {
		t.Errorf("Index: expected %q, got %q", resp.Index, got.Index)
	}
	if got.ID != resp.ID {
		t.Errorf("ID: expected %q, got %q", resp.ID, got.ID)
	}
	if got.SeqNo != resp.SeqNo {
		t.Errorf("SeqNo: expected %d, got %d", resp.SeqNo, got.SeqNo)
	}
	if got.PrimaryTerm != resp.PrimaryTerm {
		t.Errorf("PrimaryTerm: expected %d, got %d", resp.PrimaryTerm, got.PrimaryTerm)
	}
	if got.Found != resp.Found {
		t.Errorf("Found: expected %v, got %v", resp.Found, got.Found)
	}
	if !bytes.Equal(got.Source, resp.Source) {
		t.Errorf("Source: expected %s, got %s", resp.Source, got.Source)
	}
}

func TestGetDocumentResponse_Roundtrip_NotFound(t *testing.T) {
	resp := &GetDocumentResponse{
		Index:       "test-index",
		ID:          "doc-1",
		SeqNo:       0,
		PrimaryTerm: 0,
		Found:       false,
		Source:      nil,
	}

	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := resp.WriteTo(out); err != nil {
		t.Fatal(err)
	}

	in := NewStreamInput(&buf)
	got, err := ReadGetDocumentResponse(in)
	if err != nil {
		t.Fatal(err)
	}

	if got.Index != resp.Index {
		t.Errorf("Index: expected %q, got %q", resp.Index, got.Index)
	}
	if got.ID != resp.ID {
		t.Errorf("ID: expected %q, got %q", resp.ID, got.ID)
	}
	if got.SeqNo != resp.SeqNo {
		t.Errorf("SeqNo: expected %d, got %d", resp.SeqNo, got.SeqNo)
	}
	if got.PrimaryTerm != resp.PrimaryTerm {
		t.Errorf("PrimaryTerm: expected %d, got %d", resp.PrimaryTerm, got.PrimaryTerm)
	}
	if got.Found != resp.Found {
		t.Errorf("Found: expected %v, got %v", resp.Found, got.Found)
	}
	if got.Source != nil {
		t.Errorf("Source: expected nil, got %s", got.Source)
	}
}

func TestSearchRequest_Roundtrip(t *testing.T) {
	req := &SearchRequestMsg{
		Index: "test-index",
		QueryJSON: map[string]any{
			"match": map[string]any{
				"field": "value",
			},
		},
		AggsJSON: map[string]any{
			"my_agg": map[string]any{
				"terms": map[string]any{
					"field": "category",
				},
			},
		},
		Size: 10,
	}

	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := req.WriteTo(out); err != nil {
		t.Fatal(err)
	}

	in := NewStreamInput(&buf)
	got, err := ReadSearchRequestMsg(in)
	if err != nil {
		t.Fatal(err)
	}

	if got.Index != req.Index {
		t.Errorf("Index: expected %q, got %q", req.Index, got.Index)
	}
	if got.Size != req.Size {
		t.Errorf("Size: expected %d, got %d", req.Size, got.Size)
	}

	// Verify nested query JSON
	queryMatch, ok := got.QueryJSON["match"].(map[string]any)
	if !ok {
		t.Fatalf("QueryJSON[match]: expected map[string]any, got %T", got.QueryJSON["match"])
	}
	if queryMatch["field"] != "value" {
		t.Errorf("QueryJSON[match][field]: expected %q, got %v", "value", queryMatch["field"])
	}

	// Verify nested aggs JSON
	aggDef, ok := got.AggsJSON["my_agg"].(map[string]any)
	if !ok {
		t.Fatalf("AggsJSON[my_agg]: expected map[string]any, got %T", got.AggsJSON["my_agg"])
	}
	terms, ok := aggDef["terms"].(map[string]any)
	if !ok {
		t.Fatalf("AggsJSON[my_agg][terms]: expected map[string]any, got %T", aggDef["terms"])
	}
	if terms["field"] != "category" {
		t.Errorf("AggsJSON[my_agg][terms][field]: expected %q, got %v", "category", terms["field"])
	}
}

func TestSearchResponse_Roundtrip(t *testing.T) {
	resp := &SearchResponseMsg{
		Took:          15,
		TotalHits:     2,
		TotalRelation: "eq",
		MaxScore:      1.5,
		Hits: []SearchHitMsg{
			{
				Index:  "test-index",
				ID:     "doc-1",
				Score:  1.5,
				Source: json.RawMessage(`{"field":"value1"}`),
			},
			{
				Index:  "test-index",
				ID:     "doc-2",
				Score:  1.2,
				Source: json.RawMessage(`{"field":"value2"}`),
			},
		},
		Aggregations: map[string]any{
			"my_agg": map[string]any{
				"buckets": []any{
					map[string]any{
						"key":       "cat1",
						"doc_count": int64(5),
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	out := NewStreamOutput(&buf)
	if err := resp.WriteTo(out); err != nil {
		t.Fatal(err)
	}

	in := NewStreamInput(&buf)
	got, err := ReadSearchResponseMsg(in)
	if err != nil {
		t.Fatal(err)
	}

	if got.Took != resp.Took {
		t.Errorf("Took: expected %d, got %d", resp.Took, got.Took)
	}
	if got.TotalHits != resp.TotalHits {
		t.Errorf("TotalHits: expected %d, got %d", resp.TotalHits, got.TotalHits)
	}
	if got.TotalRelation != resp.TotalRelation {
		t.Errorf("TotalRelation: expected %q, got %q", resp.TotalRelation, got.TotalRelation)
	}
	if got.MaxScore != resp.MaxScore {
		t.Errorf("MaxScore: expected %v, got %v", resp.MaxScore, got.MaxScore)
	}
	if len(got.Hits) != len(resp.Hits) {
		t.Fatalf("Hits length: expected %d, got %d", len(resp.Hits), len(got.Hits))
	}

	for i, hit := range resp.Hits {
		gotHit := got.Hits[i]
		if gotHit.Index != hit.Index {
			t.Errorf("Hit[%d].Index: expected %q, got %q", i, hit.Index, gotHit.Index)
		}
		if gotHit.ID != hit.ID {
			t.Errorf("Hit[%d].ID: expected %q, got %q", i, hit.ID, gotHit.ID)
		}
		if gotHit.Score != hit.Score {
			t.Errorf("Hit[%d].Score: expected %v, got %v", i, hit.Score, gotHit.Score)
		}
		if !bytes.Equal(gotHit.Source, hit.Source) {
			t.Errorf("Hit[%d].Source: expected %s, got %s", i, hit.Source, gotHit.Source)
		}
	}

	// Verify aggregations
	aggDef, ok := got.Aggregations["my_agg"].(map[string]any)
	if !ok {
		t.Fatalf("Aggregations[my_agg]: expected map[string]any, got %T", got.Aggregations["my_agg"])
	}
	buckets, ok := aggDef["buckets"].([]any)
	if !ok {
		t.Fatalf("Aggregations[my_agg][buckets]: expected []any, got %T", aggDef["buckets"])
	}
	if len(buckets) != 1 {
		t.Fatalf("Aggregations[my_agg][buckets] length: expected 1, got %d", len(buckets))
	}
	bucket, ok := buckets[0].(map[string]any)
	if !ok {
		t.Fatalf("Aggregations[my_agg][buckets][0]: expected map[string]any, got %T", buckets[0])
	}
	if bucket["key"] != "cat1" {
		t.Errorf("Aggregations[my_agg][buckets][0][key]: expected %q, got %v", "cat1", bucket["key"])
	}
	if bucket["doc_count"] != int64(5) {
		t.Errorf("Aggregations[my_agg][buckets][0][doc_count]: expected 5, got %v", bucket["doc_count"])
	}
}
