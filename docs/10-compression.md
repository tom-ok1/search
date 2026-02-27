# Step 10: Postings 圧縮 & Skip List

## 学ぶ概念

ここまでの実装では PostingsList をそのまま保存していましたが、大規模データでは **圧縮** と **高速走査（Skip List）** が不可欠です。このステップでは Lucene が使う2つの核心技術を学びます。

### なぜ圧縮が必要か？

100万ドキュメントのインデックスで `"the"` のような一般的な term は80万件以上の Posting を持ちます。各 Posting に DocID (4バイト) + Freq (4バイト) を保存すると：

```
未圧縮: 800,000 × 8バイト = 6.4MB（1つの term だけで！）
圧縮後: 数百KB〜1MB 程度
```

### Delta Encoding（差分符号化）

PostingsList の DocID は昇順です。この性質を利用して **差分（delta）** を保存します：

```
DocIDs:  [3, 7, 12, 15, 100, 105]
Deltas:  [3, 4,  5,  3,  85,   5]  ← 差分は元の値より小さい
```

差分は元の値より小さくなる傾向があるため、後述の可変長符号化と組み合わせると大幅にサイズが減ります。

### Variable Byte Encoding（VByte / VInt）

Step 6 で紹介した VInt は最も基本的な圧縮手法です：

```
値 0〜127:      1バイト
値 128〜16383:  2バイト
値 16384〜:     3バイト以上

Delta + VByte の効果:
  DocID=100,000 → 固定長なら 4バイト
  Delta=5       → VByte なら 1バイト（95%以上の削減！）
```

### Frame of Reference (FOR) / PForDelta

Lucene が実際に使っている圧縮方式です。VByte より高速に圧縮・解凍できます。

```
FOR の仕組み:
  1. DocID を 128 個ずつのブロックに分ける
  2. 各ブロック内で delta を計算
  3. ブロック内の最大 delta に必要なビット数を求める
  4. 全 delta をそのビット数でビットパッキング

例: ブロック内の delta が全て 0〜15 なら 4ビットで表現
  128 個 × 4ビット = 64バイト（固定長なら 128×4=512バイト）

PForDelta:
  大きな外れ値だけ例外（exception）として別に保存
  → 残りをより少ないビットで表現できる
```

### Skip List

AND 検索で `advance(targetDocID)` を高速にするためのデータ構造です。

```
PostingsList (1000 件):
  [3, 7, 12, 15, 20, 25, 30, ..., 5000]

Skip List なし:
  advance(4500) → 先頭から順に走査 → O(n)

Skip List あり:
  Level 2: [3,          500,          2000,         5000]
  Level 1: [3,   100,   500,   1000,  2000,  3500,  5000]
  Level 0: [3, 7, 12, 15, 20, 25, 30, ...]  ← 全エントリ

  advance(4500):
    Level 2: 3 → 500 → 2000 → 5000 (行き過ぎ)
    Level 1: 2000 → 3500 → 5000 (行き過ぎ)
    Level 0: 3500 → ... → 4500 (数十エントリの走査で到達)

  → O(√n) 〜 O(log n) に改善
```

### ブロックベースのスキップ（Lucene の方式）

Lucene は純粋な Skip List ではなく、**128 個ずつの FOR ブロック + ブロック間スキップ** を組み合わせています：

```
Block 0: DocID [0..127]   ← FOR 圧縮
Block 1: DocID [128..255] ← FOR 圧縮
Block 2: DocID [256..383] ← FOR 圧縮
...

Skip Data:
  Block 0: minDocID=0,   fileOffset=0
  Block 1: minDocID=200, fileOffset=512
  Block 2: minDocID=450, fileOffset=1024
  ...

advance(300):
  Skip Data で Block 2 を特定（minDocID=256 ≤ 300 < minDocID=450）
  Block 2 を decode して DocID=300 を探す
```

---

## Lucene ソースを読む

### 読むべきファイル

| ファイル | ポイント |
|----------|----------|
| `codecs/lucene104/Lucene104PostingsWriter.java` | Postings の書き出し。FOR ブロック単位で圧縮 |
| `codecs/lucene104/Lucene104PostingsReader.java` | Postings の読み込み。ブロック decode + skip |
| `codecs/lucene104/ForUtil.java` | FOR (Frame of Reference) のビットパッキング実装 |
| `codecs/lucene104/PostingsUtil.java` | Postings のユーティリティ |
| `codecs/lucene104/Lucene104PostingsFormat.java` | フォーマット全体の定義。BLOCK_SIZE=128 |
| `codecs/lucene104/Lucene104ScoreSkipReader.java` | スコア情報付きスキップリーダー |
| `index/PostingsEnum.java` | `advance(target)` の API 定義 |
| `index/ImpactsEnum.java` | Impact（スコア上限）付き Postings。WAND 最適化に使用 |
| `search/WANDScorer.java` | WAND アルゴリズム。スキップとスコア上限を利用した枝刈り |
| `store/DataOutput.java` | `writeVInt()`, `writeVLong()` の実装 |
| `store/DataInput.java` | `readVInt()`, `readVLong()` の実装 |
| `util/packed/PackedInts.java` | ビットパッキングの低レベル実装 |

### Lucene の Postings フォーマット概要

```
.doc ファイル:
  [Block 0: 128 DocIDs in FOR] [Block 0: 128 Freqs in FOR]
  [Block 1: 128 DocIDs in FOR] [Block 1: 128 Freqs in FOR]
  ...
  [残り（128未満）: VInt エンコード]

.pos ファイル:
  [Block 0: 128 Positions in FOR]
  [Block 1: 128 Positions in FOR]
  ...

.tim ファイル (Term Dictionary):
  [term1: metadata (docFreq, totalTermFreq, offset to .doc, offset to .pos)]
  [term2: ...]
  ...
  → FST (Finite State Transducer) で term → metadata をルックアップ

.tip ファイル (Term Index):
  FST のインデックス部分（高速ルックアップ用）
```

---

## Go で実装する

### 1. Delta Encoding

```go
// codec/delta.go

package codec

// DeltaEncode は整数列を差分符号化する。
// 入力は昇順にソートされている必要がある。
func DeltaEncode(values []int) []int {
    if len(values) == 0 {
        return nil
    }

    deltas := make([]int, len(values))
    deltas[0] = values[0]
    for i := 1; i < len(values); i++ {
        deltas[i] = values[i] - values[i-1]
    }
    return deltas
}

// DeltaDecode は差分符号化された列を復元する。
func DeltaDecode(deltas []int) []int {
    if len(deltas) == 0 {
        return nil
    }

    values := make([]int, len(deltas))
    values[0] = deltas[0]
    for i := 1; i < len(deltas); i++ {
        values[i] = values[i-1] + deltas[i]
    }
    return values
}
```

### 2. VByte Encoding

```go
// codec/vbyte.go

package codec

// VByteEncode は整数列を VByte（可変長バイト）で符号化する。
func VByteEncode(values []int) []byte {
    var buf []byte
    for _, v := range values {
        buf = appendVByte(buf, v)
    }
    return buf
}

func appendVByte(buf []byte, v int) []byte {
    uv := uint64(v)
    for uv >= 0x80 {
        buf = append(buf, byte(uv)|0x80)
        uv >>= 7
    }
    buf = append(buf, byte(uv))
    return buf
}

// VByteDecode は VByte 符号化されたバイト列から整数列を復元する。
func VByteDecode(data []byte, count int) []int {
    values := make([]int, 0, count)
    pos := 0
    for i := 0; i < count && pos < len(data); i++ {
        v, n := readVByte(data[pos:])
        values = append(values, v)
        pos += n
    }
    return values
}

func readVByte(data []byte) (int, int) {
    var result uint64
    var shift uint
    for i, b := range data {
        result |= uint64(b&0x7F) << shift
        if b < 0x80 {
            return int(result), i + 1
        }
        shift += 7
    }
    return int(result), len(data)
}
```

### 3. FOR (Frame of Reference) ブロック圧縮

```go
// codec/for.go

package codec

import "math/bits"

const BlockSize = 128 // Lucene と同じブロックサイズ

// FORBlock は Frame of Reference で圧縮された1ブロック。
type FORBlock struct {
    BitsPerValue int    // 各値に使うビット数
    Data         []byte // ビットパッキングされたデータ
    MinValue     int    // ブロック内の最小値（基準値）
    NumValues    int    // 値の数
}

// FOREncode は整数列を FOR ブロックに圧縮する。
// 入力は delta encoding 済みであることを期待する。
func FOREncode(deltas []int) FORBlock {
    if len(deltas) == 0 {
        return FORBlock{}
    }

    // 最大値に必要なビット数を計算
    maxVal := 0
    for _, d := range deltas {
        if d > maxVal {
            maxVal = d
        }
    }

    bitsPerValue := 0
    if maxVal > 0 {
        bitsPerValue = bits.Len(uint(maxVal))
    }

    // ビットパッキング
    totalBits := bitsPerValue * len(deltas)
    totalBytes := (totalBits + 7) / 8
    data := make([]byte, totalBytes)

    for i, d := range deltas {
        packBits(data, i*bitsPerValue, bitsPerValue, uint64(d))
    }

    return FORBlock{
        BitsPerValue: bitsPerValue,
        Data:         data,
        NumValues:    len(deltas),
    }
}

// FORDecode は FOR ブロックから整数列を復元する。
func FORDecode(block FORBlock) []int {
    values := make([]int, block.NumValues)
    for i := 0; i < block.NumValues; i++ {
        values[i] = int(unpackBits(block.Data, i*block.BitsPerValue, block.BitsPerValue))
    }
    return values
}

// packBits は値をビット配列の指定位置に書き込む。
func packBits(data []byte, bitOffset, bitsPerValue int, value uint64) {
    for b := 0; b < bitsPerValue; b++ {
        if value&(1<<uint(b)) != 0 {
            byteIdx := (bitOffset + b) / 8
            bitIdx := uint((bitOffset + b) % 8)
            data[byteIdx] |= 1 << bitIdx
        }
    }
}

// unpackBits はビット配列の指定位置から値を読み出す。
func unpackBits(data []byte, bitOffset, bitsPerValue int) uint64 {
    var value uint64
    for b := 0; b < bitsPerValue; b++ {
        byteIdx := (bitOffset + b) / 8
        bitIdx := uint((bitOffset + b) % 8)
        if byteIdx < len(data) && data[byteIdx]&(1<<bitIdx) != 0 {
            value |= 1 << uint(b)
        }
    }
    return value
}
```

### 4. Skip List

```go
// codec/skiplist.go

package codec

// SkipEntry はスキップリストの1エントリ。
// 各ブロックの先頭 DocID とファイル内オフセットを保持する。
type SkipEntry struct {
    DocID      int // ブロックの先頭 DocID
    FileOffset int // .doc ファイル内のブロック開始位置
}

// SkipList はブロックベースのスキップリスト。
type SkipList struct {
    Entries []SkipEntry
}

// NewSkipList はブロック情報からスキップリストを構築する。
func NewSkipList(blockStartDocIDs []int, blockOffsets []int) *SkipList {
    entries := make([]SkipEntry, len(blockStartDocIDs))
    for i := range blockStartDocIDs {
        entries[i] = SkipEntry{
            DocID:      blockStartDocIDs[i],
            FileOffset: blockOffsets[i],
        }
    }
    return &SkipList{Entries: entries}
}

// SkipTo はtarget 以上の DocID を含むブロックのインデックスを返す。
// 二分探索を使用。
func (sl *SkipList) SkipTo(target int) int {
    lo, hi := 0, len(sl.Entries)-1

    // target 以下の最大エントリを探す
    result := 0
    for lo <= hi {
        mid := (lo + hi) / 2
        if sl.Entries[mid].DocID <= target {
            result = mid
            lo = mid + 1
        } else {
            hi = mid - 1
        }
    }
    return result
}
```

### 5. 圧縮 Postings のリーダー・ライター

```go
// codec/postings_codec.go

package codec

import "gosearch/index"

// CompressedPostings は圧縮された PostingsList。
type CompressedPostings struct {
    Term       string
    DocFreq    int         // ドキュメント頻度
    Blocks     []FORBlock  // DocID の FOR ブロック列
    FreqBlocks []FORBlock  // Freq の FOR ブロック列
    SkipList   *SkipList   // ブロックスキップ用
}

// CompressPostings は PostingsList を圧縮する。
func CompressPostings(pl *index.PostingsList) *CompressedPostings {
    if len(pl.Postings) == 0 {
        return nil
    }

    // DocID と Freq を分離
    docIDs := make([]int, len(pl.Postings))
    freqs := make([]int, len(pl.Postings))
    for i, p := range pl.Postings {
        docIDs[i] = p.DocID
        freqs[i] = p.Freq
    }

    // DocID を delta encoding
    deltas := DeltaEncode(docIDs)

    // ブロックに分割して FOR 圧縮
    var docBlocks []FORBlock
    var freqBlocks []FORBlock
    var blockStartDocIDs []int
    var blockOffsets []int
    offset := 0

    for i := 0; i < len(deltas); i += BlockSize {
        end := i + BlockSize
        if end > len(deltas) {
            end = len(deltas)
        }

        // このブロックの先頭 DocID を記録
        blockStartDocIDs = append(blockStartDocIDs, docIDs[i])
        blockOffsets = append(blockOffsets, offset)

        docBlock := FOREncode(deltas[i:end])
        freqBlock := FOREncode(freqs[i:end])

        docBlocks = append(docBlocks, docBlock)
        freqBlocks = append(freqBlocks, freqBlock)

        offset += len(docBlock.Data) + len(freqBlock.Data)
    }

    return &CompressedPostings{
        Term:       pl.Term,
        DocFreq:    len(pl.Postings),
        Blocks:     docBlocks,
        FreqBlocks: freqBlocks,
        SkipList:   NewSkipList(blockStartDocIDs, blockOffsets),
    }
}

// DecompressPostings は圧縮された Postings を復元する。
func DecompressPostings(cp *CompressedPostings) *index.PostingsList {
    var allDocIDs []int
    var allFreqs []int

    for i, block := range cp.Blocks {
        deltas := FORDecode(block)
        docIDs := DeltaDecode(deltas)

        // 前のブロックの最後の DocID を加算（ブロック間のオフセット）
        if i > 0 && len(allDocIDs) > 0 {
            base := allDocIDs[len(allDocIDs)-1]
            for j := range docIDs {
                docIDs[j] += base
            }
        }
        allDocIDs = append(allDocIDs, docIDs...)

        freqs := FORDecode(cp.FreqBlocks[i])
        allFreqs = append(allFreqs, freqs...)
    }

    pl := &index.PostingsList{Term: cp.Term}
    for i := range allDocIDs {
        pl.Postings = append(pl.Postings, index.Posting{
            DocID: allDocIDs[i],
            Freq:  allFreqs[i],
        })
    }

    return pl
}
```

---

## 確認・テスト

```go
// codec/codec_test.go

package codec

import (
    "testing"

    "gosearch/index"
)

func TestDeltaEncoding(t *testing.T) {
    original := []int{3, 7, 12, 15, 100, 105}
    deltas := DeltaEncode(original)

    expected := []int{3, 4, 5, 3, 85, 5}
    for i, d := range deltas {
        if d != expected[i] {
            t.Errorf("delta[%d]: expected %d, got %d", i, expected[i], d)
        }
    }

    // 復元
    restored := DeltaDecode(deltas)
    for i, v := range restored {
        if v != original[i] {
            t.Errorf("restored[%d]: expected %d, got %d", i, original[i], v)
        }
    }
}

func TestVByteEncoding(t *testing.T) {
    values := []int{0, 1, 127, 128, 300, 16384, 100000}
    encoded := VByteEncode(values)
    decoded := VByteDecode(encoded, len(values))

    for i, v := range decoded {
        if v != values[i] {
            t.Errorf("value[%d]: expected %d, got %d", i, values[i], v)
        }
    }

    // VByte のサイズ確認
    // 0〜127: 1バイト, 128〜16383: 2バイト, 16384〜: 3バイト
    t.Logf("Encoded %d values into %d bytes (vs %d bytes fixed-width)",
        len(values), len(encoded), len(values)*4)
}

func TestFOREncoding(t *testing.T) {
    // 全て 0〜15 の値 → 4ビットで済むはず
    deltas := make([]int, 128)
    for i := range deltas {
        deltas[i] = i % 16
    }

    block := FOREncode(deltas)
    if block.BitsPerValue != 4 {
        t.Errorf("expected 4 bits per value, got %d", block.BitsPerValue)
    }

    // 128 × 4ビット = 64バイト
    expectedBytes := (128 * 4 + 7) / 8
    if len(block.Data) != expectedBytes {
        t.Errorf("expected %d bytes, got %d", expectedBytes, len(block.Data))
    }

    // 復元
    decoded := FORDecode(block)
    for i, v := range decoded {
        if v != deltas[i] {
            t.Errorf("decoded[%d]: expected %d, got %d", i, deltas[i], v)
        }
    }
}

func TestSkipList(t *testing.T) {
    // 5ブロック: DocID [0, 200, 500, 800, 1000]
    sl := NewSkipList(
        []int{0, 200, 500, 800, 1000},
        []int{0, 100, 200, 300, 400},
    )

    tests := []struct {
        target    int
        wantBlock int
    }{
        {0, 0},
        {100, 0},
        {200, 1},
        {300, 1},
        {500, 2},
        {750, 2},
        {800, 3},
        {999, 3},
        {1000, 4},
        {1500, 4},
    }

    for _, tt := range tests {
        got := sl.SkipTo(tt.target)
        if got != tt.wantBlock {
            t.Errorf("SkipTo(%d): expected block %d, got %d",
                tt.target, tt.wantBlock, got)
        }
    }
}

func TestCompressDecompress(t *testing.T) {
    // テスト用の PostingsList を作成
    pl := &index.PostingsList{Term: "test"}
    for i := 0; i < 300; i++ {
        pl.Postings = append(pl.Postings, index.Posting{
            DocID: i * 3, // 0, 3, 6, 9, ...
            Freq:  1 + (i % 5),
        })
    }

    // 圧縮
    compressed := CompressPostings(pl)
    if compressed == nil {
        t.Fatal("compression failed")
    }

    t.Logf("Original: %d postings", len(pl.Postings))
    t.Logf("Compressed: %d blocks", len(compressed.Blocks))
    t.Logf("Skip entries: %d", len(compressed.SkipList.Entries))

    // 解凍
    restored := DecompressPostings(compressed)
    if len(restored.Postings) != len(pl.Postings) {
        t.Fatalf("expected %d postings, got %d",
            len(pl.Postings), len(restored.Postings))
    }

    for i := range pl.Postings {
        if restored.Postings[i].DocID != pl.Postings[i].DocID {
            t.Errorf("posting[%d] DocID: expected %d, got %d",
                i, pl.Postings[i].DocID, restored.Postings[i].DocID)
            break
        }
        if restored.Postings[i].Freq != pl.Postings[i].Freq {
            t.Errorf("posting[%d] Freq: expected %d, got %d",
                i, pl.Postings[i].Freq, restored.Postings[i].Freq)
            break
        }
    }
}
```

---

## 深掘り：なぜこう設計されているのか

### Q: なぜ Lucene は VByte ではなく FOR を使うのか？

VByte は **1値ずつ** エンコード・デコードします。一方 FOR は **128値を一括** でビットパッキングします。CPU の SIMD 命令（128ビット単位の並列処理）と相性が良く、decode が VByte の数倍高速です。

```
VByte:  1値ずつ条件分岐 → 分岐予測ミスが多発
FOR:    ブロック単位で一括ビット操作 → パイプラインに優しい
```

### Q: Lucene の BlockSize はなぜ 128 なのか？

128 は SIMD レジスタ（128ビット = 16バイト × 8 = 128ビット）と整合性が良い値です。また、スキップの粒度としても適切で、128件スキップは十分に粗く（大きすぎるとスキップの意味がなくなる）、十分に細かい（小さすぎるとメタデータが増える）バランスです。

### Q: WAND アルゴリズムとは何か？

**WAND (Weak AND)** は OR 検索で TopK 件を効率的に取得するアルゴリズムです。

```
検索: "quick" OR "brown" OR "fox"  (Top 3)

各 term の最大スコア貢献:
  "quick": maxScore = 2.5
  "brown": maxScore = 1.8
  "fox":   maxScore = 3.0

現在の3位のスコア: threshold = 4.0

あるドキュメントが "quick" と "brown" だけにマッチ:
  最大可能スコア = 2.5 + 1.8 = 4.3 > 4.0 → 計算する

あるドキュメントが "brown" だけにマッチ:
  最大可能スコア = 1.8 < 4.0 → スキップ！
```

Skip List と組み合わせることで、スコアが足りないドキュメントを大量にスキップできます。Lucene の `WANDScorer` と `ImpactsEnum` がこれを実装しています。

### Q: 圧縮はメモリとディスクの両方に効くのか？

はい。圧縮されたデータは：

1. **ディスク**: ファイルサイズが小さい → I/O が減る
2. **メモリ**: OS のページキャッシュに載るデータが増える → キャッシュヒット率向上
3. **帯域幅**: ディスク→メモリの転送量が減る → SSD/HDD の帯域を節約

圧縮のオーバーヘッド（CPU 時間）よりも I/O 削減のメリットが大きいため、結果的に**圧縮したほうが速い**というケースがほとんどです。

---

## まとめ：全体を振り返る

10ステップを通じて、以下の Lucene / ES の核心を Go で実装しながら学びました：

| Step | 概念 | ES での対応 |
|------|------|------------|
| 1 | Analyzer & Tokenizer | mapping の `analyzer` |
| 2 | 転置インデックス | `text` / `keyword` フィールド |
| 3 | BM25 スコアリング | `_score`、`explain` API |
| 4 | Boolean & Phrase Query | `bool`, `match_phrase` クエリ |
| 5 | Segment アーキテクチャ | `_segments` API、segment 数の監視 |
| 6 | ディスク永続化 | index の data path |
| 7 | Segment Merge | `_forcemerge`、merge policy |
| 8 | Doc Values & 集計 | `doc_values`, aggregations |
| 9 | NRT 検索 | `refresh_interval`, translog |
| 10 | Postings 圧縮 & Skip | 検索速度の根拠 |

これらの知識があれば、ES の挙動の「なぜ」に対して、実装レベルで説明できるようになっています。
