package index

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// InfoStream is a diagnostic logging interface for the indexing pipeline.
// It allows components like IndexWriter, DocumentsWriter, FlushControl, and DWPT
// to emit diagnostic messages that can be enabled/disabled per component.
type InfoStream interface {
	// Message emits a diagnostic message for the given component.
	Message(component, message string)

	// IsEnabled returns true if diagnostic messages are enabled for the given component.
	IsEnabled(component string) bool
}

// NoOpInfoStream is the default InfoStream implementation that discards all messages.
// IsEnabled always returns false, and Message is a no-op.
type NoOpInfoStream struct{}

// NewNoOpInfoStream creates a new NoOpInfoStream.
func NewNoOpInfoStream() InfoStream {
	return &NoOpInfoStream{}
}

// Message is a no-op for NoOpInfoStream.
func (n *NoOpInfoStream) Message(component, message string) {
	// no-op
}

// IsEnabled always returns false for NoOpInfoStream.
func (n *NoOpInfoStream) IsEnabled(component string) bool {
	return false
}

// PrintInfoStream writes diagnostic messages to an io.Writer with RFC3339Nano
// timestamp and component prefix. Format: "2026-04-06T12:34:56.789Z IW: message\n"
type PrintInfoStream struct {
	writer  io.Writer
	mu      sync.Mutex
	timeNow func() time.Time // for testing
}

// NewPrintInfoStream creates a new PrintInfoStream that writes to the given writer.
func NewPrintInfoStream(w io.Writer) InfoStream {
	return &PrintInfoStream{
		writer:  w,
		timeNow: time.Now,
	}
}

// Message writes a formatted diagnostic message to the writer.
func (p *PrintInfoStream) Message(component, message string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	timestamp := p.timeNow().UTC().Format(time.RFC3339Nano)
	// RFC3339Nano uses milliseconds format like 2026-04-06T12:34:56.789Z
	// We need to truncate to milliseconds (3 decimal places)
	if len(timestamp) > 24 && timestamp[23] == 'Z' {
		// Keep only 3 decimal places: 2026-04-06T12:34:56.789Z
		timestamp = timestamp[:23] + "Z"
	}

	fmt.Fprintf(p.writer, "%s %s: %s\n", timestamp, component, message)
}

// IsEnabled always returns true for PrintInfoStream (all components enabled).
func (p *PrintInfoStream) IsEnabled(component string) bool {
	return true
}

// capturingInfoStream is a test helper that captures messages for later inspection.
type capturingInfoStream struct {
	mu       sync.Mutex
	messages []string
	enabled  map[string]bool
}

// newCapturingInfoStream creates a new capturingInfoStream with the specified
// components enabled. All other components will be disabled.
func newCapturingInfoStream(components ...string) *capturingInfoStream {
	enabled := make(map[string]bool)
	for _, component := range components {
		enabled[component] = true
	}
	return &capturingInfoStream{
		messages: make([]string, 0),
		enabled:  enabled,
	}
}

// Message captures a message if the component is enabled.
func (c *capturingInfoStream) Message(component, message string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, fmt.Sprintf("%s: %s", component, message))
}

// IsEnabled returns true if the component is in the enabled set.
func (c *capturingInfoStream) IsEnabled(component string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enabled[component]
}

// Messages returns all captured messages.
func (c *capturingInfoStream) Messages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]string, len(c.messages))
	copy(result, c.messages)
	return result
}

// HasMessageContaining returns true if any captured message contains the substring.
func (c *capturingInfoStream) HasMessageContaining(substring string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, msg := range c.messages {
		if strings.Contains(msg, substring) {
			return true
		}
	}
	return false
}
