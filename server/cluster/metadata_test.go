package cluster

import (
	"encoding/json"
	"testing"
	"time"

	"gosearch/server/mapping"
)

func TestMetadataJSONRoundtrip(t *testing.T) {
	original := &Metadata{
		Indices: map[string]*IndexMetadata{
			"test_index": {
				Name: "test_index",
				Settings: IndexSettings{
					NumberOfShards:   2,
					NumberOfReplicas: 1,
					RefreshInterval:  5 * time.Second,
				},
				Mapping: &mapping.MappingDefinition{
					Properties: map[string]mapping.FieldMapping{
						"title": {Type: mapping.FieldTypeText, Analyzer: "standard"},
						"count": {Type: mapping.FieldTypeLong},
					},
				},
				State: IndexStateOpen,
			},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var restored Metadata
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	idx := restored.Indices["test_index"]
	if idx == nil {
		t.Fatal("index 'test_index' not found after roundtrip")
	}
	if idx.Name != "test_index" {
		t.Errorf("name = %q, want %q", idx.Name, "test_index")
	}
	if idx.Settings.NumberOfShards != 2 {
		t.Errorf("shards = %d, want 2", idx.Settings.NumberOfShards)
	}
	if idx.Settings.NumberOfReplicas != 1 {
		t.Errorf("replicas = %d, want 1", idx.Settings.NumberOfReplicas)
	}
	if idx.Settings.RefreshInterval != 5*time.Second {
		t.Errorf("refresh_interval = %v, want 5s", idx.Settings.RefreshInterval)
	}
	if idx.State != IndexStateOpen {
		t.Errorf("state = %v, want IndexStateOpen", idx.State)
	}
	if idx.Mapping == nil || len(idx.Mapping.Properties) != 2 {
		t.Fatalf("mapping properties count = %d, want 2", len(idx.Mapping.Properties))
	}
	if idx.Mapping.Properties["title"].Type != mapping.FieldTypeText {
		t.Errorf("title type = %q, want %q", idx.Mapping.Properties["title"].Type, mapping.FieldTypeText)
	}
	if idx.Mapping.Properties["title"].Analyzer != "standard" {
		t.Errorf("title analyzer = %q, want %q", idx.Mapping.Properties["title"].Analyzer, "standard")
	}
}

func TestIndexStateJSON(t *testing.T) {
	tests := []struct {
		state IndexState
		want  string
	}{
		{IndexStateOpen, `"open"`},
		{IndexStateClosed, `"closed"`},
	}
	for _, tt := range tests {
		data, err := json.Marshal(tt.state)
		if err != nil {
			t.Fatalf("marshal %v: %v", tt.state, err)
		}
		if string(data) != tt.want {
			t.Errorf("marshal %v = %s, want %s", tt.state, data, tt.want)
		}

		var got IndexState
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("unmarshal %s: %v", data, err)
		}
		if got != tt.state {
			t.Errorf("unmarshal %s = %v, want %v", data, got, tt.state)
		}
	}
}

func TestRefreshIntervalJSON(t *testing.T) {
	tests := []struct {
		interval time.Duration
		want     string
	}{
		{1 * time.Second, `"1s"`},
		{5 * time.Second, `"5s"`},
		{-1, `"-1"`},
		{0, `"0s"`},
		{500 * time.Millisecond, `"500ms"`},
	}
	for _, tt := range tests {
		settings := IndexSettings{RefreshInterval: tt.interval}
		data, err := json.Marshal(settings)
		if err != nil {
			t.Fatalf("marshal %v: %v", tt.interval, err)
		}

		var got IndexSettings
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("unmarshal %s: %v", data, err)
		}
		if got.RefreshInterval != tt.interval {
			t.Errorf("roundtrip %v: got %v", tt.interval, got.RefreshInterval)
		}
	}
}
