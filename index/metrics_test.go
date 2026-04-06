package index

import (
	"sync"
	"testing"
)

func TestMetricsZeroInitialized(t *testing.T) {
	m := &IndexWriterMetrics{}

	// Flush counters
	if got := m.FlushCount.Load(); got != 0 {
		t.Errorf("FlushCount = %d, want 0", got)
	}
	if got := m.FlushBytes.Load(); got != 0 {
		t.Errorf("FlushBytes = %d, want 0", got)
	}
	if got := m.FlushTimeNanos.Load(); got != 0 {
		t.Errorf("FlushTimeNanos = %d, want 0", got)
	}

	// Stall counters
	if got := m.StallCount.Load(); got != 0 {
		t.Errorf("StallCount = %d, want 0", got)
	}
	if got := m.StallTimeNanos.Load(); got != 0 {
		t.Errorf("StallTimeNanos = %d, want 0", got)
	}

	// Merge counters
	if got := m.MergeCount.Load(); got != 0 {
		t.Errorf("MergeCount = %d, want 0", got)
	}
	if got := m.MergeDocCount.Load(); got != 0 {
		t.Errorf("MergeDocCount = %d, want 0", got)
	}
	if got := m.MergeTimeNanos.Load(); got != 0 {
		t.Errorf("MergeTimeNanos = %d, want 0", got)
	}

	// Document counters
	if got := m.DocsAdded.Load(); got != 0 {
		t.Errorf("DocsAdded = %d, want 0", got)
	}
	if got := m.DocsDeleted.Load(); got != 0 {
		t.Errorf("DocsDeleted = %d, want 0", got)
	}

	// Gauges
	if got := m.ActiveBytes.Load(); got != 0 {
		t.Errorf("ActiveBytes = %d, want 0", got)
	}
	if got := m.FlushPendingBytes.Load(); got != 0 {
		t.Errorf("FlushPendingBytes = %d, want 0", got)
	}
	if got := m.SegmentCount.Load(); got != 0 {
		t.Errorf("SegmentCount = %d, want 0", got)
	}
}

func TestMetricsAtomicIncrement(t *testing.T) {
	m := &IndexWriterMetrics{}

	// Test Add and Load for each counter
	tests := []struct {
		name     string
		addFunc  func(int64)
		loadFunc func() int64
		delta    int64
	}{
		{"FlushCount", func(d int64) { m.FlushCount.Add(d) }, func() int64 { return m.FlushCount.Load() }, 5},
		{"FlushBytes", func(d int64) { m.FlushBytes.Add(d) }, func() int64 { return m.FlushBytes.Load() }, 1024},
		{"FlushTimeNanos", func(d int64) { m.FlushTimeNanos.Add(d) }, func() int64 { return m.FlushTimeNanos.Load() }, 123456},
		{"StallCount", func(d int64) { m.StallCount.Add(d) }, func() int64 { return m.StallCount.Load() }, 3},
		{"StallTimeNanos", func(d int64) { m.StallTimeNanos.Add(d) }, func() int64 { return m.StallTimeNanos.Load() }, 987654},
		{"MergeCount", func(d int64) { m.MergeCount.Add(d) }, func() int64 { return m.MergeCount.Load() }, 2},
		{"MergeDocCount", func(d int64) { m.MergeDocCount.Add(d) }, func() int64 { return m.MergeDocCount.Load() }, 1000},
		{"MergeTimeNanos", func(d int64) { m.MergeTimeNanos.Add(d) }, func() int64 { return m.MergeTimeNanos.Load() }, 555555},
		{"DocsAdded", func(d int64) { m.DocsAdded.Add(d) }, func() int64 { return m.DocsAdded.Load() }, 100},
		{"DocsDeleted", func(d int64) { m.DocsDeleted.Add(d) }, func() int64 { return m.DocsDeleted.Load() }, 10},
		{"ActiveBytes", func(d int64) { m.ActiveBytes.Add(d) }, func() int64 { return m.ActiveBytes.Load() }, 2048},
		{"FlushPendingBytes", func(d int64) { m.FlushPendingBytes.Add(d) }, func() int64 { return m.FlushPendingBytes.Load() }, 512},
		{"SegmentCount", func(d int64) { m.SegmentCount.Add(d) }, func() int64 { return m.SegmentCount.Load() }, 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Add delta
			tt.addFunc(tt.delta)

			// Load and verify
			got := tt.loadFunc()
			if got != tt.delta {
				t.Errorf("%s.Load() = %d, want %d", tt.name, got, tt.delta)
			}

			// Add again
			tt.addFunc(tt.delta)
			got = tt.loadFunc()
			want := tt.delta * 2
			if got != want {
				t.Errorf("%s.Load() after second add = %d, want %d", tt.name, got, want)
			}
		})
	}
}

// TestMetricsConcurrentIncrement verifies thread-safety of atomic operations
func TestMetricsConcurrentIncrement(t *testing.T) {
	m := &IndexWriterMetrics{}
	const numGoroutines = 100
	const incrementsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrently increment DocsAdded counter
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerGoroutine; j++ {
				m.DocsAdded.Add(1)
			}
		}()
	}

	wg.Wait()

	want := int64(numGoroutines * incrementsPerGoroutine)
	got := m.DocsAdded.Load()
	if got != want {
		t.Errorf("DocsAdded after concurrent increments = %d, want %d", got, want)
	}
}

// TestMetricsStore verifies Store operation
func TestMetricsStore(t *testing.T) {
	m := &IndexWriterMetrics{}

	// Store specific values
	m.FlushCount.Store(42)
	m.ActiveBytes.Store(8192)

	if got := m.FlushCount.Load(); got != 42 {
		t.Errorf("FlushCount.Load() = %d, want 42", got)
	}
	if got := m.ActiveBytes.Load(); got != 8192 {
		t.Errorf("ActiveBytes.Load() = %d, want 8192", got)
	}
}
