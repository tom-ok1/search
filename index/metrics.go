package index

import "sync/atomic"

// IndexWriterMetrics tracks diagnostic information about IndexWriter operations.
// All fields use atomic operations for lock-free concurrent access.
// This struct is shared from IndexWriter → DocumentsWriter → FlushControl via pointer.
type IndexWriterMetrics struct {
	// Flush counters
	FlushCount     atomic.Int64 // Number of flushes completed
	FlushBytes     atomic.Int64 // Total bytes flushed to disk
	FlushTimeNanos atomic.Int64 // Total time spent flushing (nanoseconds)

	// Stall counters
	StallCount     atomic.Int64 // Number of times indexing was stalled
	StallTimeNanos atomic.Int64 // Total time spent stalled (nanoseconds)

	// Merge counters
	MergeCount     atomic.Int64 // Number of merges completed
	MergeDocCount  atomic.Int64 // Total documents merged
	MergeTimeNanos atomic.Int64 // Total time spent merging (nanoseconds)

	// Document counters
	DocsAdded   atomic.Int64 // Total documents added
	DocsDeleted atomic.Int64 // Total documents deleted

	// Gauges (current state)
	ActiveBytes       atomic.Int64 // Current active memory usage
	FlushPendingBytes atomic.Int64 // Bytes pending flush
	SegmentCount      atomic.Int64 // Current number of segments
}
