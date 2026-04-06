package index

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// TestNoOpInfoStreamIsDisabled verifies that NoOpInfoStream always returns false for IsEnabled.
func TestNoOpInfoStreamIsDisabled(t *testing.T) {
	is := NewNoOpInfoStream()

	components := []string{"IW", "DW", "FC", "DWPT", "BD", ""}
	for _, component := range components {
		if is.IsEnabled(component) {
			t.Errorf("NoOpInfoStream.IsEnabled(%q) should return false, got true", component)
		}
	}
}

// TestPrintInfoStreamWritesOutput verifies that PrintInfoStream writes correctly formatted output.
func TestPrintInfoStreamWritesOutput(t *testing.T) {
	var buf bytes.Buffer
	is := NewPrintInfoStream(&buf)

	if !is.IsEnabled("IW") {
		t.Error("PrintInfoStream.IsEnabled should return true for all components")
	}

	// Freeze time for testing
	now := time.Date(2026, 4, 6, 12, 34, 56, 789000000, time.UTC)
	is.(*PrintInfoStream).timeNow = func() time.Time { return now }

	is.Message("IW", "test message")

	output := buf.String()
	expected := "2026-04-06T12:34:56.789Z IW: test message\n"
	if output != expected {
		t.Errorf("PrintInfoStream output mismatch\ngot:  %q\nwant: %q", output, expected)
	}
}

// TestPrintInfoStreamMultipleComponents verifies that multiple components write correctly.
func TestPrintInfoStreamMultipleComponents(t *testing.T) {
	var buf bytes.Buffer
	is := NewPrintInfoStream(&buf)

	// Freeze time for testing
	now := time.Date(2026, 4, 6, 12, 34, 56, 789000000, time.UTC)
	is.(*PrintInfoStream).timeNow = func() time.Time { return now }

	is.Message("IW", "index writer message")
	is.Message("DW", "documents writer message")
	is.Message("DWPT", "per-thread message")

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}

	expectedLines := []string{
		"2026-04-06T12:34:56.789Z IW: index writer message",
		"2026-04-06T12:34:56.789Z DW: documents writer message",
		"2026-04-06T12:34:56.789Z DWPT: per-thread message",
	}

	for i, expected := range expectedLines {
		if lines[i] != expected {
			t.Errorf("line %d mismatch\ngot:  %q\nwant: %q", i, lines[i], expected)
		}
	}
}

// TestCapturingInfoStream verifies the test helper implementation.
func TestCapturingInfoStream(t *testing.T) {
	is := newCapturingInfoStream("IW", "DW")

	if !is.IsEnabled("IW") {
		t.Error("IsEnabled(IW) should return true")
	}
	if !is.IsEnabled("DW") {
		t.Error("IsEnabled(DW) should return true")
	}
	if is.IsEnabled("FC") {
		t.Error("IsEnabled(FC) should return false")
	}

	is.Message("IW", "message one")
	is.Message("DW", "message two")
	is.Message("IW", "message three")

	messages := is.Messages()
	if len(messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(messages))
	}

	expected := []string{
		"IW: message one",
		"DW: message two",
		"IW: message three",
	}

	for i, exp := range expected {
		if messages[i] != exp {
			t.Errorf("message %d mismatch\ngot:  %q\nwant: %q", i, messages[i], exp)
		}
	}

	if !is.HasMessageContaining("message one") {
		t.Error("HasMessageContaining should find 'message one'")
	}
	if !is.HasMessageContaining("message two") {
		t.Error("HasMessageContaining should find 'message two'")
	}
	if !is.HasMessageContaining("IW:") {
		t.Error("HasMessageContaining should find 'IW:'")
	}
	if is.HasMessageContaining("nonexistent") {
		t.Error("HasMessageContaining should not find 'nonexistent'")
	}
}
