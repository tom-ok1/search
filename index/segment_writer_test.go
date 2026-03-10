package index

import (
	"encoding/json"
	"io"
	"os"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

func TestWriteSegmentV2(t *testing.T) {
	tmpDir := t.TempDir()
	dir, _ := store.NewFSDirectory(tmpDir)

	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	writer := NewIndexWriter(dir, analyzer, 100)

	doc0 := document.NewDocument()
	doc0.AddField("title", "The Quick Brown Fox", document.FieldTypeText)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("title", "The Lazy Dog", document.FieldTypeText)
	writer.AddDocument(doc1)

	doc2 := document.NewDocument()
	doc2.AddField("title", "Brown Fox Lazy", document.FieldTypeText)
	writer.AddDocument(doc2)

	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seg := reader.Leaves()[0].Segment

	// Verify files exist (written by Flush)
	expectedFiles := []string{
		seg.Name() + ".meta",
		seg.Name() + ".title.tidx",
		seg.Name() + ".title.tdat",
		seg.Name() + ".title.flen",
		seg.Name() + ".stored",
	}
	for _, f := range expectedFiles {
		if !dir.FileExists(f) {
			t.Errorf("expected file %s to exist", f)
		}
	}

	// Verify metadata
	metaIn, _ := dir.OpenInput(seg.Name() + ".meta")
	metaBytes, _ := io.ReadAll(metaIn)
	metaIn.Close()

	var meta SegmentMeta
	json.Unmarshal(metaBytes, &meta)

	if meta.DocCount != 3 {
		t.Errorf("expected doc_count 3, got %d", meta.DocCount)
	}

	// Verify field lengths file structure (fixed-width uint32)
	flenPath := dir.FilePath(seg.Name() + ".title.flen")
	flenData, _ := os.ReadFile(flenPath)
	// Should be: uint32(docCount=3) + 3 × uint32(lengths)
	expectedFlenSize := 4 + 3*4
	if len(flenData) != expectedFlenSize {
		t.Errorf("flen file size: expected %d, got %d", expectedFlenSize, len(flenData))
	}

	// Verify stored fields file structure (trailer format)
	storedPath := dir.FilePath(seg.Name() + ".stored")
	storedData, _ := os.ReadFile(storedPath)
	// Should end with: 3 × uint64(offsets) + uint32(docCount=3)
	storedTrailerSize := 3*8 + 4
	if len(storedData) < storedTrailerSize {
		t.Fatalf("stored file too small: %d bytes", len(storedData))
	}
}
