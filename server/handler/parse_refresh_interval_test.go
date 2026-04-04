package handler

import (
	"testing"
	"time"
)

func TestParseRefreshInterval(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{"-1", -1, false},
		{"1s", 1 * time.Second, false},
		{"5s", 5 * time.Second, false},
		{"500ms", 500 * time.Millisecond, false},
		{"1m", 1 * time.Minute, false},
		{"bad", 0, true},
		{"", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseRefreshInterval(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseRefreshInterval(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseRefreshInterval(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
