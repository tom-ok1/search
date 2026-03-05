package index

import "gosearch/store"

// OpenDirectoryReader opens an IndexReader from a committed index on disk.
// It reads the latest segments_N file and opens each segment as a DiskSegment.
func OpenDirectoryReader(dir store.Directory) (*IndexReader, error) {
	si, err := ReadLatestSegmentInfos(dir)
	if err != nil {
		return nil, err
	}

	segments := make([]SegmentReader, 0, len(si.Segments))
	for _, info := range si.Segments {
		seg, err := OpenDiskSegment(dir.FilePath(""), info.Name)
		if err != nil {
			// Close already opened segments
			for _, s := range segments {
				if c, ok := s.(interface{ Close() error }); ok {
					c.Close()
				}
			}
			return nil, err
		}
		segments = append(segments, seg)
	}

	return NewIndexReader(segments), nil
}

// OpenNRTReader opens a near-real-time IndexReader from a writer.
// The in-memory buffer is flushed and pending deletes are resolved,
// producing a point-in-time snapshot. Subsequent writes to the writer
// are not visible through the returned reader.
func OpenNRTReader(w *IndexWriter) (*IndexReader, error) {
	segs, err := w.nrtSegments()
	if err != nil {
		return nil, err
	}
	return NewIndexReader(segs), nil
}
