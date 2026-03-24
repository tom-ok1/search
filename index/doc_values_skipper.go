package index

import (
	"fmt"
	"math"

	"gosearch/store"
)

const (
	skipBlockSize  = 128
	skipJumpFactor = 8
	skipMaxLevels  = 3

	skipHeaderSize     = 32
	skipLevelDirEntry  = 12
	skipBlockEntrySize = 28
)

// skipLevelMeta holds the offset and block count for a single level in the skip index.
type skipLevelMeta struct {
	offset     uint64
	blockCount int
}

// DocValuesSkipper provides block-level min/max metadata for doc values,
// enabling efficient skipping of non-competitive blocks during sorted collection.
// For numeric doc values, min/max are raw int64 values.
// For sorted doc values, min/max are ordinal values.
type DocValuesSkipper struct {
	data       *store.MMapIndexInput
	blockSize  int
	jumpFactor int
	numLevels  int
	docCount   int
	globalMin  int64
	globalMax  int64
	levels     []skipLevelMeta
	divisors   []int // precomputed jumpFactor^level for each level

	pos int // current level-0 block index

	// Cached current block entry (valid when cachedPos == pos and pos >= 0)
	cachedPos      int
	cachedMinDocID int
	cachedMaxDocID int
	cachedDocCount int
	cachedMinValue int64
	cachedMaxValue int64
}

// NewDocValuesSkipper creates a skipper from a memory-mapped skip index file (.ndvs or .sdvs).
func NewDocValuesSkipper(data *store.MMapIndexInput) (*DocValuesSkipper, error) {
	if data.Length() < skipHeaderSize {
		return nil, fmt.Errorf("skip index file too small: %d bytes", data.Length())
	}

	blockSize, err := data.ReadUint32At(0)
	if err != nil {
		return nil, fmt.Errorf("read blockSize: %w", err)
	}
	jumpFactor, err := data.ReadUint32At(4)
	if err != nil {
		return nil, fmt.Errorf("read jumpFactor: %w", err)
	}
	numLevels, err := data.ReadUint32At(8)
	if err != nil {
		return nil, fmt.Errorf("read numLevels: %w", err)
	}
	docCount, err := data.ReadUint32At(12)
	if err != nil {
		return nil, fmt.Errorf("read docCount: %w", err)
	}
	globalMin, err := data.ReadInt64At(16)
	if err != nil {
		return nil, fmt.Errorf("read globalMin: %w", err)
	}
	globalMax, err := data.ReadInt64At(24)
	if err != nil {
		return nil, fmt.Errorf("read globalMax: %w", err)
	}

	levels := make([]skipLevelMeta, numLevels)
	for i := range int(numLevels) {
		base := skipHeaderSize + i*skipLevelDirEntry
		offset, err := data.ReadUint64At(base)
		if err != nil {
			return nil, fmt.Errorf("read level %d offset: %w", i, err)
		}
		bc, err := data.ReadUint32At(base + 8)
		if err != nil {
			return nil, fmt.Errorf("read level %d blockCount: %w", i, err)
		}
		levels[i] = skipLevelMeta{offset: offset, blockCount: int(bc)}
	}

	// Precompute divisors: divisors[i] = jumpFactor^i
	divs := make([]int, int(numLevels))
	d := 1
	for i := range divs {
		divs[i] = d
		d *= int(jumpFactor)
	}

	return &DocValuesSkipper{
		data:       data,
		blockSize:  int(blockSize),
		jumpFactor: int(jumpFactor),
		numLevels:  int(numLevels),
		docCount:   int(docCount),
		globalMin:  globalMin,
		globalMax:  globalMax,
		levels:     levels,
		divisors:   divs,
		pos:        -1,
		cachedPos:  -2,
	}, nil
}

// NumLevels returns the number of skip levels.
func (s *DocValuesSkipper) NumLevels() int { return s.numLevels }

// GlobalMin returns the global minimum value.
func (s *DocValuesSkipper) GlobalMin() int64 { return s.globalMin }

// GlobalMax returns the global maximum value.
func (s *DocValuesSkipper) GlobalMax() int64 { return s.globalMax }

// DocCount returns the total document count.
func (s *DocValuesSkipper) DocCount() int { return s.docCount }

// BlockCount returns the number of blocks at the given level.
func (s *DocValuesSkipper) BlockCount(level int) int {
	if level < 0 || level >= s.numLevels {
		return 0
	}
	return s.levels[level].blockCount
}

func (s *DocValuesSkipper) readBlockEntry(level, blockIdx int) (minDocID, maxDocID, docCount int, minValue, maxValue int64, err error) {
	if level < 0 || level >= s.numLevels || blockIdx < 0 || blockIdx >= s.levels[level].blockCount {
		return 0, 0, 0, 0, 0, fmt.Errorf("block index out of range: level=%d, blockIdx=%d", level, blockIdx)
	}
	offset := int(s.levels[level].offset) + blockIdx*skipBlockEntrySize
	minDoc, err := s.data.ReadUint32At(offset)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	maxDoc, err := s.data.ReadUint32At(offset + 4)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	dc, err := s.data.ReadUint32At(offset + 8)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	minV, err := s.data.ReadInt64At(offset + 12)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	maxV, err := s.data.ReadInt64At(offset + 20)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	return int(minDoc), int(maxDoc), int(dc), minV, maxV, nil
}

func (s *DocValuesSkipper) loadCache() {
	if s.cachedPos == s.pos {
		return
	}
	s.cachedPos = s.pos
	if s.pos < 0 || s.pos >= s.levels[0].blockCount {
		s.cachedMinDocID = -1
		s.cachedMaxDocID = -1
		s.cachedDocCount = 0
		s.cachedMinValue = 0
		s.cachedMaxValue = 0
		return
	}
	minDoc, maxDoc, dc, minV, maxV, err := s.readBlockEntry(0, s.pos)
	if err != nil {
		s.cachedMinDocID = -1
		s.cachedMaxDocID = -1
		s.cachedDocCount = 0
		s.cachedMinValue = 0
		s.cachedMaxValue = 0
		return
	}
	s.cachedMinDocID = minDoc
	s.cachedMaxDocID = maxDoc
	s.cachedDocCount = dc
	s.cachedMinValue = minV
	s.cachedMaxValue = maxV
}

// MinDocID returns the minimum docID for the current level-0 block.
func (s *DocValuesSkipper) MinDocID() int {
	s.loadCache()
	return s.cachedMinDocID
}

// MaxDocID returns the maximum docID for the current level-0 block.
func (s *DocValuesSkipper) MaxDocID() int {
	s.loadCache()
	return s.cachedMaxDocID
}

// BlockDocCount returns the number of docs with values in the current level-0 block.
func (s *DocValuesSkipper) BlockDocCount() int {
	s.loadCache()
	return s.cachedDocCount
}

// MinValue returns the minimum value for the current level-0 block.
func (s *DocValuesSkipper) MinValue() int64 {
	s.loadCache()
	return s.cachedMinValue
}

// MaxValue returns the maximum value for the current level-0 block.
func (s *DocValuesSkipper) MaxValue() int64 {
	s.loadCache()
	return s.cachedMaxValue
}

// Advance moves to the level-0 block containing targetDocID.
// Uses hierarchical search from the highest level down to narrow
// the search range at each level, reducing I/O for large indices.
// On consecutive calls with increasing targetDocIDs, the current
// position is used as a lower bound to skip already-visited blocks.
// Returns true if such a block exists.
func (s *DocValuesSkipper) Advance(targetDocID int) bool {
	if s.numLevels == 0 || s.levels[0].blockCount == 0 {
		return false
	}

	// Use current position as lower bound for consecutive advancing
	lo := max(s.pos, 0)
	hi := s.levels[0].blockCount

	// Hierarchical search: start from the highest level and narrow down
	for level := s.numLevels - 1; level >= 0; level-- {
		divisor := s.divisors[level]

		// Map level-0 range to this level's block indices
		levelLo := lo / divisor
		levelHi := min((hi+divisor-1)/divisor, s.levels[level].blockCount)

		// Binary search at this level for the first block with maxDocID >= targetDocID
		sLo, sHi := levelLo, levelHi
		for sLo < sHi {
			mid := sLo + (sHi-sLo)/2
			_, maxDoc, _, _, _, err := s.readBlockEntry(level, mid)
			if err != nil {
				return false
			}
			if maxDoc < targetDocID {
				sLo = mid + 1
			} else {
				sHi = mid
			}
		}

		if sLo >= levelHi {
			s.pos = s.levels[0].blockCount
			return false
		}

		// Narrow level-0 range to the children of the found block
		lo = sLo * divisor
		hi = min((sLo+1)*divisor, s.levels[0].blockCount)
	}

	s.pos = lo
	return true
}

// AdvanceToValue advances from the current position to find the next level-0 block
// whose value range overlaps with [minValue, maxValue].
// Uses hierarchical skipping through higher levels when possible.
// Returns true if such a block is found.
// min <= max is assumed
func (s *DocValuesSkipper) AdvanceToValue(minValue, maxValue int64) bool {
	if s.numLevels == 0 || s.levels[0].blockCount == 0 {
		return false
	}
	if s.pos < 0 {
		s.pos = 0
	}

	for s.pos < s.levels[0].blockCount {
		// Try to skip using higher levels
		if s.trySkipHigherLevels(minValue, maxValue) {
			continue
		}

		// Check current level-0 block
		_, _, _, blockMin, blockMax, err := s.readBlockEntry(0, s.pos)
		if err != nil {
			return false
		}
		if blockMax >= minValue && blockMin <= maxValue {
			return true
		}
		s.pos++
	}
	return false
}

// trySkipHigherLevels attempts to skip non-overlapping blocks using higher-level metadata.
// Returns true if it advanced the position (caller should re-check).
func (s *DocValuesSkipper) trySkipHigherLevels(minValue, maxValue int64) bool {
	for level := s.numLevels - 1; level >= 1; level-- {
		divisor := s.divisors[level]
		blockIdx := s.pos / divisor

		if blockIdx >= s.levels[level].blockCount {
			continue
		}

		_, _, _, blockMin, blockMax, err := s.readBlockEntry(level, blockIdx)
		if err != nil {
			continue
		}

		// If entire higher-level block doesn't overlap, skip it
		if blockMax < minValue || blockMin > maxValue {
			s.pos = (blockIdx + 1) * divisor
			return true
		}
	}
	return false
}

// --- Write functions ---

// skipBlockAccumulator streams doc/value pairs into level-0 skipBlocks
// without requiring all values to be held in memory at once.
type skipBlockAccumulator struct {
	blocks  []skipBlock
	current skipBlock
	started bool
}

func (a *skipBlockAccumulator) Add(docID int, value int64) {
	if !a.started {
		a.current = skipBlock{
			minDocID: docID,
			minValue: math.MaxInt64,
			maxValue: math.MinInt64,
		}
		a.started = true
	}
	a.current.maxDocID = docID
	a.current.docCount++
	if value < a.current.minValue {
		a.current.minValue = value
	}
	if value > a.current.maxValue {
		a.current.maxValue = value
	}
	if a.current.docCount == skipBlockSize {
		a.blocks = append(a.blocks, a.current)
		a.started = false
	}
}

func (a *skipBlockAccumulator) Finish() []skipBlock {
	if a.started {
		a.blocks = append(a.blocks, a.current)
	}
	return a.blocks
}

type skipBlock struct {
	minDocID int
	maxDocID int
	docCount int
	minValue int64
	maxValue int64
}

// writeDocValuesSkipIndex writes a skip index file for a doc values field.
// ext is the file extension (e.g., "ndvs" for numeric, "sdvs" for sorted).
// docIDs and values are parallel slices representing which docs have values.
func writeDocValuesSkipIndex(dir store.Directory, segName, field, ext string, docIDs []int, values []int64) error {
	if len(docIDs) == 0 {
		return writeEmptySkipIndex(dir, segName, field, ext)
	}

	var acc skipBlockAccumulator
	for i, docID := range docIDs {
		acc.Add(docID, values[i])
	}
	return buildAndWriteSkipIndex(dir, segName, field, ext, acc.Finish(), len(docIDs))
}

func buildAndWriteSkipIndex(dir store.Directory, segName, field, ext string, l0Blocks []skipBlock, docCount int) error {
	// Build higher levels
	allLevels := [][]skipBlock{l0Blocks}
	for len(allLevels) < skipMaxLevels {
		prev := allLevels[len(allLevels)-1]
		if len(prev) <= 1 {
			break
		}
		next := buildHigherLevel(prev)
		allLevels = append(allLevels, next)
	}

	// Compute global min/max
	globalMin := int64(math.MaxInt64)
	globalMax := int64(math.MinInt64)
	for _, b := range l0Blocks {
		if b.minValue < globalMin {
			globalMin = b.minValue
		}
		if b.maxValue > globalMax {
			globalMax = b.maxValue
		}
	}

	return writeSkipIndexFile(dir, segName, field, ext, allLevels, docCount, globalMin, globalMax)
}

// writeNumericDocValuesSkipIndexFromNDV builds the skip index from an existing .ndv file.
func writeNumericDocValuesSkipIndexFromNDV(dir store.Directory, segName, field string, docCount int) error {
	if docCount == 0 {
		return writeEmptySkipIndex(dir, segName, field, "ndvs")
	}

	ndvPath := dir.FilePath(fmt.Sprintf("%s.%s.ndv", segName, field))
	ndv, err := store.OpenMMap(ndvPath)
	if err != nil {
		return fmt.Errorf("open ndv for skip index: %w", err)
	}
	defer ndv.Close()

	var acc skipBlockAccumulator
	for i := range docCount {
		v, err := ndv.ReadInt64At(i * 8)
		if err != nil {
			return fmt.Errorf("read ndv value %d: %w", i, err)
		}
		acc.Add(i, v)
	}

	return buildAndWriteSkipIndex(dir, segName, field, "ndvs", acc.Finish(), docCount)
}

// writeSortedDocValuesSkipIndexFromOrd builds the .sdvs skip index by streaming
// over an existing .sdvo file, using ordinals as values and skipping missing docs (ord == -1).
func writeSortedDocValuesSkipIndexFromOrd(dir store.Directory, segName, field string, docCount int) error {
	if docCount == 0 {
		return writeEmptySkipIndex(dir, segName, field, "sdvs")
	}

	sdvoPath := dir.FilePath(fmt.Sprintf("%s.%s.sdvo", segName, field))
	sdvo, err := store.OpenMMap(sdvoPath)
	if err != nil {
		return fmt.Errorf("open sdvo for skip index: %w", err)
	}
	defer sdvo.Close()

	var acc skipBlockAccumulator
	valueCount := 0
	for i := range docCount {
		raw, err := sdvo.ReadUint32At(i * 4)
		if err != nil {
			return fmt.Errorf("read sdvo ordinal %d: %w", i, err)
		}
		ord := int32(raw)
		if ord < 0 {
			continue // missing value
		}
		acc.Add(i, int64(ord))
		valueCount++
	}

	return buildAndWriteSkipIndex(dir, segName, field, "sdvs", acc.Finish(), valueCount)
}

func writeSkipIndexHeader(out store.IndexOutput, numLevels, docCount int, globalMin, globalMax int64) error {
	if err := out.WriteUint32(uint32(skipBlockSize)); err != nil {
		return err
	}
	if err := out.WriteUint32(uint32(skipJumpFactor)); err != nil {
		return err
	}
	if err := out.WriteUint32(uint32(numLevels)); err != nil {
		return err
	}
	if err := out.WriteUint32(uint32(docCount)); err != nil {
		return err
	}
	if err := out.WriteUint64(uint64(globalMin)); err != nil {
		return err
	}
	return out.WriteUint64(uint64(globalMax))
}

func writeEmptySkipIndex(dir store.Directory, segName, field, ext string) error {
	out, err := dir.CreateOutput(fmt.Sprintf("%s.%s.%s", segName, field, ext))
	if err != nil {
		return err
	}
	defer out.Close()
	return writeSkipIndexHeader(out, 0, 0, 0, 0)
}

func buildHigherLevel(prev []skipBlock) []skipBlock {
	numBlocks := (len(prev) + skipJumpFactor - 1) / skipJumpFactor
	blocks := make([]skipBlock, numBlocks)

	for i := range numBlocks {
		start := i * skipJumpFactor
		end := min(start+skipJumpFactor, len(prev))

		minV := int64(math.MaxInt64)
		maxV := int64(math.MinInt64)
		minDoc := prev[start].minDocID
		maxDoc := prev[start].maxDocID
		dc := 0

		for j := start; j < end; j++ {
			if prev[j].minValue < minV {
				minV = prev[j].minValue
			}
			if prev[j].maxValue > maxV {
				maxV = prev[j].maxValue
			}
			if prev[j].maxDocID > maxDoc {
				maxDoc = prev[j].maxDocID
			}
			dc += prev[j].docCount
		}

		blocks[i] = skipBlock{
			minDocID: minDoc,
			maxDocID: maxDoc,
			docCount: dc,
			minValue: minV,
			maxValue: maxV,
		}
	}
	return blocks
}

func writeSkipIndexFile(dir store.Directory, segName, field, ext string, allLevels [][]skipBlock, docCount int, globalMin, globalMax int64) error {
	out, err := dir.CreateOutput(fmt.Sprintf("%s.%s.%s", segName, field, ext))
	if err != nil {
		return err
	}
	defer out.Close()

	numLevels := len(allLevels)

	// Compute level data offsets
	// Data starts after header + level directory
	dataStart := uint64(skipHeaderSize + numLevels*skipLevelDirEntry)
	offsets := make([]uint64, numLevels)
	current := dataStart
	for i, blocks := range allLevels {
		offsets[i] = current
		current += uint64(len(blocks) * skipBlockEntrySize)
	}

	if err := writeSkipIndexHeader(out, numLevels, docCount, globalMin, globalMax); err != nil {
		return err
	}

	// Write level directory
	for i, blocks := range allLevels {
		if err := out.WriteUint64(offsets[i]); err != nil {
			return err
		}
		if err := out.WriteUint32(uint32(len(blocks))); err != nil {
			return err
		}
	}

	// Write level data
	for _, blocks := range allLevels {
		for _, b := range blocks {
			if err := out.WriteUint32(uint32(b.minDocID)); err != nil {
				return err
			}
			if err := out.WriteUint32(uint32(b.maxDocID)); err != nil {
				return err
			}
			if err := out.WriteUint32(uint32(b.docCount)); err != nil {
				return err
			}
			if err := out.WriteUint64(uint64(b.minValue)); err != nil {
				return err
			}
			if err := out.WriteUint64(uint64(b.maxValue)); err != nil {
				return err
			}
		}
	}

	return nil
}
