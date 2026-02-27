# Step 5: Segment アーキテクチャ

## 学ぶ概念

ここまでは1つの大きなインメモリインデックスにドキュメントを追加してきました。しかし Lucene のインデックスは **Segment** と呼ばれる不変（immutable）な単位に分かれています。これは Lucene の設計の最も重要な概念の1つです。

### なぜ Segment か？

従来のデータベースのように「インデックス全体を更新する」アプローチには問題があります：

1. **並行性**: 書き込みと読み込みが同時に起きると排他制御が必要
2. **パフォーマンス**: 巨大なインデックスの一部を変更するコストが大きい
3. **耐障害性**: 書き込み途中にクラッシュするとインデックスが壊れる

Lucene の解決策は **Write-once（一度書いたら変更しない）** です：

```
                    時間の流れ →

  batch1 の追加    → [Segment 0] (immutable)
  batch2 の追加    → [Segment 0] [Segment 1] (immutable)
  batch3 の追加    → [Segment 0] [Segment 1] [Segment 2]
  マージ           → [Segment 0+1] [Segment 2]
```

### Segment のライフサイクル

```
1. IndexWriter にドキュメントが追加される
   → インメモリバッファに蓄積

2. flush（またはバッファが一杯になる）
   → バッファの内容が新しい Segment としてディスクに書き出される
   → この Segment は二度と変更されない（immutable）

3. 検索時
   → 全 Segment を検索し、結果をマージする

4. マージ（バックグラウンド）
   → 小さい Segment を統合して大きな Segment にする
   → 古い Segment は削除される

5. 削除
   → ドキュメントの削除は「削除マーク」で表現
   → 実際の削除はマージ時に行われる
```

### ES との関連

ES で観察できる以下の挙動は、全てこの Segment 設計に起因します：

| ES の挙動 | Segment での説明 |
|-----------|-----------------|
| `refresh_interval: 1s` | 1秒ごとにインメモリバッファを Segment にする |
| ドキュメント追加後すぐに検索できない | 新しい Segment がまだ作られていない |
| `_forcemerge` | 明示的に Segment をマージする |
| 削除後もディスク使用量が減らない | 削除マークがついただけで、マージまで実体が残る |
| Segment 数が多いと遅い | 検索が全 Segment を走査するため |

---

## Lucene ソースを読む

### 読むべきファイル（`lucene/lucene/core/src/java/org/apache/lucene/index/`）

| ファイル | ポイント |
|----------|----------|
| `SegmentInfo.java` | Segment のメタデータ（名前、ドキュメント数、使用 Codec など） |
| `SegmentInfos.java` | 現在のインデックスを構成する全 Segment のリスト。commit point |
| `SegmentCommitInfo.java` | Segment + 削除情報 + 更新情報をまとめたもの |
| `SegmentReader.java` | 1つの Segment を読む Reader |
| `IndexWriter.java` | `addDocument()` → `flush()` → Segment 生成の流れ |
| `DocumentsWriter.java` | インメモリバッファの管理。スレッドごとに独立したバッファを持つ |
| `DocumentsWriterPerThread.java` | スレッドローカルなドキュメント処理 |
| `DirectoryReader.java` | 複数 Segment の Reader を束ねる |
| `LeafReader.java` | 1つの Segment に対応する Reader の抽象 |
| `LeafReaderContext.java` | LeafReader + その Segment のオフセット情報 |
| `IndexReader.java` | 全体の Reader の抽象基底 |

### Lucene のドキュメント追加フロー

```
IndexWriter.addDocument(doc)
  → DocumentsWriter.updateDocument()
    → DocumentsWriterPerThread.updateDocument()
      → indexingChain で転置インデックスをインメモリに構築
      → バッファが一杯 or flush 要求 → flush
        → Segment を書き出し
        → SegmentInfos に追加
```

### 複数 Segment の検索

```
IndexSearcher.search(query)
  → DirectoryReader.leaves() で全 LeafReaderContext を取得
  → 各 LeafReaderContext に対して:
    → weight.scorer(leafCtx) でその Segment の Scorer を取得
    → Scorer で DocID を走査
      → ※DocID は Segment 内でのローカルID
      → グローバルID = leaf.docBase + localDocID
  → Collector で結果を統合
```

---

## Go で実装する

### 1. Segment 構造体

```go
// index/segment.go

package index

// Segment はインデックスの不変な単位。
// 一度作成されたら変更されない。
type Segment struct {
    name     string
    index    *FieldIndex           // フィールド別の転置インデックス
    fields   map[string]*FieldIndex
    docCount int
    // stored fields: segment 内ローカル docID → fieldName → value
    storedFields map[int]map[string]string
    fieldLengths map[string][]int
    // 削除マーク: segment 内ローカル docID → 削除済みか
    deletedDocs map[int]bool
}

func newSegment(name string) *Segment {
    return &Segment{
        name:         name,
        fields:       make(map[string]*FieldIndex),
        storedFields: make(map[int]map[string]string),
        fieldLengths: make(map[string][]int),
        deletedDocs:  make(map[int]bool),
    }
}

// IsDeleted はドキュメントが削除済みかどうかを返す。
func (s *Segment) IsDeleted(localDocID int) bool {
    return s.deletedDocs[localDocID]
}

// LiveDocCount は削除されていないドキュメント数を返す。
func (s *Segment) LiveDocCount() int {
    return s.docCount - len(s.deletedDocs)
}

// MarkDeleted はドキュメントに削除マークを付ける。
// Segment 自体は immutable だが、削除情報は別途管理される。
func (s *Segment) MarkDeleted(localDocID int) {
    s.deletedDocs[localDocID] = true
}
```

### 2. IndexWriter（Segment ベース）

```go
// index/writer.go

package index

import (
    "fmt"
    "gosearch/analysis"
    "gosearch/document"
)

// IndexWriter はドキュメントをインデックスに追加し、Segment を管理する。
type IndexWriter struct {
    analyzer   *analysis.Analyzer
    segments   []*Segment
    // インメモリバッファ（まだ Segment になっていないドキュメント）
    buffer     *Segment
    bufferSize int // flush するドキュメント数の閾値
    segmentCounter int
}

func NewIndexWriter(analyzer *analysis.Analyzer, bufferSize int) *IndexWriter {
    w := &IndexWriter{
        analyzer:   analyzer,
        bufferSize: bufferSize,
    }
    w.buffer = newSegment(w.nextSegmentName())
    return w
}

func (w *IndexWriter) nextSegmentName() string {
    name := fmt.Sprintf("_seg%d", w.segmentCounter)
    w.segmentCounter++
    return name
}

// AddDocument はドキュメントをインメモリバッファに追加する。
// バッファが閾値に達したら自動で flush される。
func (w *IndexWriter) AddDocument(doc *document.Document) error {
    docID := w.buffer.docCount
    w.buffer.docCount++

    for _, field := range doc.Fields {
        switch field.Type {
        case document.FieldTypeText:
            tokens, err := w.analyzer.Analyze(field.Value)
            if err != nil {
                return err
            }
            fi := w.getOrCreateFieldIndex(w.buffer, field.Name)

            // フィールド長の記録
            if w.buffer.fieldLengths[field.Name] == nil {
                w.buffer.fieldLengths[field.Name] = make([]int, 0)
            }
            for len(w.buffer.fieldLengths[field.Name]) <= docID {
                w.buffer.fieldLengths[field.Name] = append(w.buffer.fieldLengths[field.Name], 0)
            }
            w.buffer.fieldLengths[field.Name][docID] = len(tokens)

            // Postings の構築
            termInfo := make(map[string]*Posting)
            for _, token := range tokens {
                posting, exists := termInfo[token.Term]
                if !exists {
                    posting = &Posting{DocID: docID}
                    termInfo[token.Term] = posting
                }
                posting.Freq++
                posting.Positions = append(posting.Positions, token.Position)
            }

            for term, posting := range termInfo {
                pl, exists := fi.postings[term]
                if !exists {
                    pl = &PostingsList{Term: term}
                    fi.postings[term] = pl
                }
                pl.Postings = append(pl.Postings, *posting)
            }

        case document.FieldTypeKeyword:
            fi := w.getOrCreateFieldIndex(w.buffer, field.Name)
            pl, exists := fi.postings[field.Value]
            if !exists {
                pl = &PostingsList{Term: field.Value}
                fi.postings[field.Value] = pl
            }
            pl.Postings = append(pl.Postings, Posting{
                DocID: docID, Freq: 1, Positions: []int{0},
            })
        }

        // Stored fields
        if field.Type == document.FieldTypeStored || field.Type == document.FieldTypeText {
            if w.buffer.storedFields[docID] == nil {
                w.buffer.storedFields[docID] = make(map[string]string)
            }
            w.buffer.storedFields[docID][field.Name] = field.Value
        }
    }

    // 自動 flush
    if w.buffer.docCount >= w.bufferSize {
        w.Flush()
    }

    return nil
}

// Flush はインメモリバッファを Segment として確定する。
func (w *IndexWriter) Flush() {
    if w.buffer.docCount == 0 {
        return
    }
    w.segments = append(w.segments, w.buffer)
    w.buffer = newSegment(w.nextSegmentName())
}

// Segments は現在の全 Segment を返す（バッファ含む）。
func (w *IndexWriter) Segments() []*Segment {
    if w.buffer.docCount > 0 {
        return append(w.segments, w.buffer)
    }
    return w.segments
}

// DeleteDocuments は指定 field/term にマッチするドキュメントに削除マークを付ける。
func (w *IndexWriter) DeleteDocuments(field, term string) {
    for _, seg := range w.Segments() {
        fi, exists := seg.fields[field]
        if !exists {
            continue
        }
        pl, exists := fi.postings[term]
        if !exists {
            continue
        }
        for _, posting := range pl.Postings {
            seg.MarkDeleted(posting.DocID)
        }
    }
}

func (w *IndexWriter) getOrCreateFieldIndex(seg *Segment, fieldName string) *FieldIndex {
    fi, exists := seg.fields[fieldName]
    if !exists {
        fi = newFieldIndex()
        seg.fields[fieldName] = fi
    }
    return fi
}
```

### 3. IndexReader（複数 Segment 対応）

```go
// index/reader.go

package index

// IndexReader は複数の Segment をまたいで検索するための Reader。
type IndexReader struct {
    segments []*Segment
}

// LeafReaderContext は1つの Segment と、そのグローバル DocID のベースオフセット。
type LeafReaderContext struct {
    Segment *Segment
    DocBase int // この Segment の最初のドキュメントのグローバル DocID
}

func NewIndexReader(segments []*Segment) *IndexReader {
    return &IndexReader{segments: segments}
}

// Leaves は各 Segment の LeafReaderContext を返す。
func (r *IndexReader) Leaves() []LeafReaderContext {
    var leaves []LeafReaderContext
    docBase := 0
    for _, seg := range r.segments {
        leaves = append(leaves, LeafReaderContext{
            Segment: seg,
            DocBase: docBase,
        })
        docBase += seg.docCount
    }
    return leaves
}

// TotalDocCount は全 Segment の合計ドキュメント数を返す。
func (r *IndexReader) TotalDocCount() int {
    total := 0
    for _, seg := range r.segments {
        total += seg.docCount
    }
    return total
}

// LiveDocCount は削除されていないドキュメントの合計数を返す。
func (r *IndexReader) LiveDocCount() int {
    total := 0
    for _, seg := range r.segments {
        total += seg.LiveDocCount()
    }
    return total
}

// GetStoredFields はグローバル DocID から stored fields を取得する。
func (r *IndexReader) GetStoredFields(globalDocID int) map[string]string {
    for _, leaf := range r.Leaves() {
        localDocID := globalDocID - leaf.DocBase
        if localDocID >= 0 && localDocID < leaf.Segment.docCount {
            return leaf.Segment.storedFields[localDocID]
        }
    }
    return nil
}
```

### 4. IndexSearcher（複数 Segment 対応）

```go
// search/searcher.go （Step 5 版：複数 Segment 対応）

package search

import "gosearch/index"

// SearchResult は検索結果の1件を表す。
type SearchResult struct {
    DocID  int               // グローバル DocID
    Score  float64
    Fields map[string]string // stored fields
}

// IndexSearcher は複数 Segment にまたがって検索を実行する。
type IndexSearcher struct {
    reader *index.IndexReader
}

func NewIndexSearcher(reader *index.IndexReader) *IndexSearcher {
    return &IndexSearcher{reader: reader}
}

// Search はクエリを実行し、上位K件の結果を返す。
func (s *IndexSearcher) Search(field, term string, topK int) []SearchResult {
    scorer := NewBM25Scorer()
    collector := NewTopKCollector(topK)

    totalDocCount := s.reader.TotalDocCount()

    // 全 Segment の docFreq を先に計算（IDF に必要）
    docFreq := 0
    for _, leaf := range s.reader.Leaves() {
        fi, exists := leaf.Segment.fields[field]
        if !exists {
            continue
        }
        pl, exists := fi.postings[term]
        if !exists {
            continue
        }
        docFreq += len(pl.Postings)
    }

    if docFreq == 0 {
        return nil
    }

    idf := scorer.IDF(totalDocCount, docFreq)

    // 全 Segment の avgDocLen を計算
    totalLen := 0
    totalDocs := 0
    for _, leaf := range s.reader.Leaves() {
        lengths, exists := leaf.Segment.fieldLengths[field]
        if !exists {
            continue
        }
        for _, l := range lengths {
            totalLen += l
            totalDocs++
        }
    }
    avgDocLen := 0.0
    if totalDocs > 0 {
        avgDocLen = float64(totalLen) / float64(totalDocs)
    }

    // 各 Segment を検索
    for _, leaf := range s.reader.Leaves() {
        fi, exists := leaf.Segment.fields[field]
        if !exists {
            continue
        }
        pl, exists := fi.postings[term]
        if !exists {
            continue
        }

        for _, posting := range pl.Postings {
            // 削除済みドキュメントをスキップ
            if leaf.Segment.IsDeleted(posting.DocID) {
                continue
            }

            docLen := 0.0
            if lengths, ok := leaf.Segment.fieldLengths[field]; ok && posting.DocID < len(lengths) {
                docLen = float64(lengths[posting.DocID])
            }

            score := scorer.Score(float64(posting.Freq), docLen, avgDocLen, idf)
            globalDocID := leaf.DocBase + posting.DocID

            collector.Collect(SearchResult{
                DocID:  globalDocID,
                Score:  score,
                Fields: leaf.Segment.storedFields[posting.DocID],
            })
        }
    }

    return collector.Results()
}
```

---

## 確認・テスト

```go
// index/segment_test.go

package index

import (
    "testing"

    "gosearch/analysis"
    "gosearch/document"
)

func TestSegmentFlush(t *testing.T) {
    analyzer := analysis.NewAnalyzer(
        analysis.NewWhitespaceTokenizer(),
        &analysis.LowerCaseFilter{},
    )
    // バッファサイズ 2 で flush
    writer := NewIndexWriter(analyzer, 2)

    doc0 := document.NewDocument()
    doc0.AddField("body", "hello world", document.FieldTypeText)
    writer.AddDocument(doc0)

    doc1 := document.NewDocument()
    doc1.AddField("body", "hello go", document.FieldTypeText)
    writer.AddDocument(doc1) // ここで自動 flush

    doc2 := document.NewDocument()
    doc2.AddField("body", "world go", document.FieldTypeText)
    writer.AddDocument(doc2)

    writer.Flush()

    segments := writer.Segments()
    if len(segments) != 2 {
        t.Fatalf("expected 2 segments, got %d", len(segments))
    }
    if segments[0].docCount != 2 {
        t.Errorf("segment 0: expected 2 docs, got %d", segments[0].docCount)
    }
    if segments[1].docCount != 1 {
        t.Errorf("segment 1: expected 1 doc, got %d", segments[1].docCount)
    }
}

func TestDeleteDocument(t *testing.T) {
    analyzer := analysis.NewAnalyzer(
        analysis.NewWhitespaceTokenizer(),
        &analysis.LowerCaseFilter{},
    )
    writer := NewIndexWriter(analyzer, 100)

    doc0 := document.NewDocument()
    doc0.AddField("id", "1", document.FieldTypeKeyword)
    doc0.AddField("body", "hello world", document.FieldTypeText)
    writer.AddDocument(doc0)

    doc1 := document.NewDocument()
    doc1.AddField("id", "2", document.FieldTypeKeyword)
    doc1.AddField("body", "hello go", document.FieldTypeText)
    writer.AddDocument(doc1)

    writer.Flush()

    // id=1 のドキュメントを削除
    writer.DeleteDocuments("id", "1")

    reader := NewIndexReader(writer.Segments())
    if reader.LiveDocCount() != 1 {
        t.Errorf("expected 1 live doc, got %d", reader.LiveDocCount())
    }
}

func TestMultiSegmentSearch(t *testing.T) {
    analyzer := analysis.NewAnalyzer(
        analysis.NewWhitespaceTokenizer(),
        &analysis.LowerCaseFilter{},
    )
    writer := NewIndexWriter(analyzer, 2)

    // 3つのドキュメントを追加（2つ目で flush → 2 segment に分かれる）
    texts := []string{"hello world", "hello go", "world go"}
    for _, text := range texts {
        doc := document.NewDocument()
        doc.AddField("body", text, document.FieldTypeText)
        writer.AddDocument(doc)
    }
    writer.Flush()

    reader := NewIndexReader(writer.Segments())
    leaves := reader.Leaves()

    // Segment が2つあること
    if len(leaves) != 2 {
        t.Fatalf("expected 2 leaves, got %d", len(leaves))
    }

    // DocBase が正しいこと
    if leaves[0].DocBase != 0 {
        t.Errorf("leaf 0 docBase: expected 0, got %d", leaves[0].DocBase)
    }
    if leaves[1].DocBase != 2 {
        t.Errorf("leaf 1 docBase: expected 2, got %d", leaves[1].DocBase)
    }
}
```

---

## 深掘り：なぜこう設計されているのか

### Q: なぜ Segment は immutable なのか？

Immutable にすることで以下のメリットがあります：

1. **ロック不要**: 読み込みと書き込みが衝突しない。新しい Segment を追加するだけ
2. **キャッシュフレンドリー**: 一度キャッシュに乗ったデータが変わらない
3. **シンプルな障害復旧**: 書き込み中にクラッシュしても、既存 Segment は壊れない
4. **OS ページキャッシュの効率**: ファイルが変わらないので OS のページキャッシュが活きる

### Q: 削除はどう実装されるのか？

Segment は immutable なので、削除は「削除ビット」を別に管理することで表現されます。Lucene では `.liv` ファイル（liveDocs）に削除されていないドキュメントのビットセットを保存します。

実際の物理削除はマージ時に行われます。マージでは、削除マーク付きのドキュメントを除外して新しい Segment を作ります。

### Q: DocID のローカルとグローバルとは？

- **ローカル DocID**: Segment 内での連番（0始まり）
- **グローバル DocID**: 全 Segment を通じた ID（`docBase + localDocID`）

検索時は各 Segment 内でローカル DocID を使い、結果を返すときにグローバル DocID に変換します。

### Q: Lucene の DocumentsWriterPerThread は何をしている？

Lucene では複数スレッドが同時にドキュメントを追加できるように、スレッドごとに独立したインメモリバッファ（`DocumentsWriterPerThread`）を持ちます。flush 時にそれぞれが独立した Segment になります。

我々の実装では単一スレッドなので、この最適化は不要です。

---

## 次のステップ

Segment ベースの設計が理解できたので、次は [Step 6: ディスク永続化](06-persistence.md) で、Segment をファイルに書き出して永続化する方法を学びます。
