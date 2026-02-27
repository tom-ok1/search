# Step 9: Near Real-Time (NRT) 検索

## 学ぶ概念

ES を使っていると「ドキュメントを追加してもすぐに検索に出てこない」という経験をします。これは Lucene の **Near Real-Time (NRT)** 検索の仕組みに起因します。

### リアルタイムではない理由

```
時刻 T+0: IndexWriter.addDocument(doc)
  → インメモリバッファに追加（ディスクには書かない）

時刻 T+0〜T+1s: 検索
  → まだ Segment になっていないのでヒットしない！

時刻 T+1s: refresh（ES のデフォルト: 1秒間隔）
  → インメモリバッファ → 新しい Segment 生成
  → IndexReader を再オープン

時刻 T+1s〜: 検索
  → 新しい Segment が見えるのでヒットする
```

### Refresh とは

**Refresh** は「インメモリバッファを Segment にして検索可能にする」操作です。
ただし、この時点ではディスクに `fsync` しない（OS のページキャッシュには書くが、永続化保証はない）。

```
refresh の流れ:
  1. インメモリバッファの内容で新しい Segment を作る
  2. 新しい IndexReader を開く（既存の Segment + 新 Segment）
  3. 古い IndexReader を閉じる

commit（永続化）の流れ:
  1. 全 Segment を fsync（ディスクに強制書き出し）
  2. segments_N ファイルを更新
  3. 古い commit point のファイルを削除
```

### ES の refresh 関連設定

| 設定 | デフォルト | 意味 |
|------|-----------|------|
| `refresh_interval` | `1s` | refresh の間隔 |
| `index.refresh_interval: -1` | - | 自動 refresh を無効化 |
| `_refresh` API | - | 手動 refresh |
| `refresh=wait_for` | - | index API で次の refresh まで待つ |

### Translog（トランザクションログ）

Refresh は高頻度に行われますが、`fsync` を伴う commit は重い操作です。では refresh 後〜commit 前にクラッシュしたら？

ES では **Translog** で対策します：

```
1. ドキュメント追加時:
   → インメモリバッファに追加
   → Translog にも書き込み（fsync あり → 永続化保証）

2. クラッシュ時:
   → 最後の commit point（segments_N）から Segment を復元
   → Translog を再生して、commit 後に追加されたドキュメントを復旧

3. Flush（= commit）時:
   → 全 Segment を fsync
   → segments_N を更新
   → Translog をクリア
```

---

## Lucene ソースを読む

### 読むべきファイル（`lucene/lucene/core/src/java/org/apache/lucene/index/`）

| ファイル | ポイント |
|----------|----------|
| `DirectoryReader.java` | `open(IndexWriter)` で NRT Reader を取得。`openIfChanged()` で refresh |
| `IndexWriter.java` | `flush()` でインメモリ→Segment。`commit()` で永続化 |
| `StandardDirectoryReader.java` | `doOpenIfChanged(IndexWriter)` が NRT refresh の実体 |
| `DocumentsWriter.java` | `flushAllThreads()` でバッファを flush |
| `ReadersAndUpdates.java` | Segment ごとの Reader と更新情報を管理 |
| `SegmentInfos.java` | `commit()` で segments_N を書き出し |
| `IndexFileDeleter.java` | 古いファイルの削除を安全に行う（参照カウント） |

### NRT の核心: `DirectoryReader.openIfChanged()`

```java
// NRT Reader の取得
IndexWriter writer = new IndexWriter(dir, config);
DirectoryReader reader = DirectoryReader.open(writer); // NRT!

// Refresh
DirectoryReader newReader = DirectoryReader.openIfChanged(reader, writer);
if (newReader != null) {
    reader.close();
    reader = newReader;
    // 新しいドキュメントが見えるようになった
}
```

ポイント: `DirectoryReader.open(writer)` は `open(directory)` とは異なり、**まだ commit されていないデータも含む** Reader を返します。

### IndexWriter の内部状態遷移

```
                    addDocument()
                         │
                         ▼
                ┌─────────────────┐
                │ インメモリバッファ │ ← 検索不可
                └────────┬────────┘
                         │ flush / refresh
                         ▼
                ┌─────────────────┐
                │ Segment (RAM)   │ ← NRT Reader で検索可
                │ ページキャッシュ  │    永続化保証なし
                └────────┬────────┘
                         │ commit (fsync)
                         ▼
                ┌─────────────────┐
                │ Segment (Disk)  │ ← 永続化済み
                │ segments_N 更新  │    クラッシュ耐性あり
                └─────────────────┘
```

---

## Go で実装する

### 1. NRT 対応の IndexWriter

```go
// index/writer_nrt.go

package index

import (
    "sync"
    "sync/atomic"
)

// NRTIndexWriter は Near Real-Time 対応の IndexWriter。
// 既存の IndexWriter を拡張し、Reader の管理と refresh 機能を追加する。
type NRTIndexWriter struct {
    *IndexWriter
    mu            sync.RWMutex
    currentReader atomic.Pointer[IndexReader]
    generation    int64 // refresh ごとにインクリメント
}

func NewNRTIndexWriter(analyzer *analysis.Analyzer, bufferSize int) *NRTIndexWriter {
    w := &NRTIndexWriter{
        IndexWriter: NewIndexWriter(analyzer, bufferSize),
    }
    return w
}

// Refresh はインメモリバッファを Segment にして、新しい Reader を作成する。
// 戻り値は新しい Reader が作られたか（変更があったか）。
func (w *NRTIndexWriter) Refresh() bool {
    w.mu.Lock()
    defer w.mu.Unlock()

    // バッファに何もなければ変更なし
    if w.buffer.docCount == 0 && w.currentReader.Load() != nil {
        return false
    }

    // バッファを flush して Segment にする
    w.Flush()

    // 新しい Reader を作成
    reader := NewIndexReader(w.Segments())
    w.currentReader.Store(reader)
    w.generation++

    return true
}

// AcquireReader は現在の Reader を取得する。
// Reader がまだない場合は Refresh を実行する。
func (w *NRTIndexWriter) AcquireReader() *IndexReader {
    reader := w.currentReader.Load()
    if reader == nil {
        w.Refresh()
        reader = w.currentReader.Load()
    }
    return reader
}

// Generation は現在の refresh 世代番号を返す。
func (w *NRTIndexWriter) Generation() int64 {
    return w.generation
}
```

### 2. SearcherManager（Reader のライフサイクル管理）

```go
// search/searcher_manager.go

package search

import (
    "gosearch/index"
    "sync"
)

// SearcherManager は IndexSearcher のライフサイクルを管理する。
// Refresh によって新しい Searcher に切り替わる。
type SearcherManager struct {
    writer  *index.NRTIndexWriter
    mu      sync.RWMutex
    current *IndexSearcher
}

func NewSearcherManager(writer *index.NRTIndexWriter) *SearcherManager {
    return &SearcherManager{
        writer: writer,
    }
}

// MaybeRefresh は必要に応じて Refresh を実行し、Searcher を更新する。
func (sm *SearcherManager) MaybeRefresh() bool {
    refreshed := sm.writer.Refresh()
    if refreshed {
        reader := sm.writer.AcquireReader()
        newSearcher := NewIndexSearcher(reader)
        sm.mu.Lock()
        sm.current = newSearcher
        sm.mu.Unlock()
    }
    return refreshed
}

// Acquire は現在の IndexSearcher を取得する。
func (sm *SearcherManager) Acquire() *IndexSearcher {
    sm.mu.RLock()
    defer sm.mu.RUnlock()

    if sm.current == nil {
        sm.mu.RUnlock()
        sm.MaybeRefresh()
        sm.mu.RLock()
    }
    return sm.current
}
```

### 3. Translog（簡易版）

```go
// index/translog.go

package index

import (
    "encoding/json"
    "gosearch/document"
    "gosearch/store"
    "io"
)

// TranslogEntry はトランザクションログの1エントリ。
type TranslogEntry struct {
    Op       string            `json:"op"`       // "index" or "delete"
    DocFields []TranslogField  `json:"fields,omitempty"`
    // 削除用
    DeleteField string `json:"delete_field,omitempty"`
    DeleteTerm  string `json:"delete_term,omitempty"`
}

type TranslogField struct {
    Name  string `json:"name"`
    Value string `json:"value"`
    Type  int    `json:"type"`
}

// Translog はクラッシュ復旧のためのトランザクションログ。
type Translog struct {
    dir  store.Directory
    file string
    entries []TranslogEntry
}

func NewTranslog(dir store.Directory) *Translog {
    return &Translog{
        dir:  dir,
        file: "translog.json",
    }
}

// LogIndex はドキュメント追加をログに記録する。
func (t *Translog) LogIndex(doc *document.Document) error {
    entry := TranslogEntry{Op: "index"}
    for _, f := range doc.Fields {
        entry.DocFields = append(entry.DocFields, TranslogField{
            Name:  f.Name,
            Value: f.Value,
            Type:  int(f.Type),
        })
    }
    t.entries = append(t.entries, entry)
    return t.sync()
}

// LogDelete は削除をログに記録する。
func (t *Translog) LogDelete(field, term string) error {
    t.entries = append(t.entries, TranslogEntry{
        Op:          "delete",
        DeleteField: field,
        DeleteTerm:  term,
    })
    return t.sync()
}

// Replay はトランザクションログを再生する。
func (t *Translog) Replay() ([]TranslogEntry, error) {
    in, err := t.dir.OpenInput(t.file)
    if err != nil {
        return nil, nil // ファイルがなければ何もしない
    }
    defer in.Close()

    data, _ := io.ReadAll(in)
    var entries []TranslogEntry
    if err := json.Unmarshal(data, &entries); err != nil {
        return nil, err
    }
    return entries, nil
}

// Clear はトランザクションログをクリアする（commit 後に呼ぶ）。
func (t *Translog) Clear() error {
    t.entries = nil
    if t.dir.FileExists(t.file) {
        return t.dir.DeleteFile(t.file)
    }
    return nil
}

// sync はログをディスクに書き出す。
func (t *Translog) sync() error {
    data, err := json.Marshal(t.entries)
    if err != nil {
        return err
    }
    out, err := t.dir.CreateOutput(t.file)
    if err != nil {
        return err
    }
    defer out.Close()
    _, err = out.Write(data)
    return err
}
```

---

## 確認・テスト

```go
// index/nrt_test.go

package index

import (
    "testing"

    "gosearch/analysis"
    "gosearch/document"
)

func TestNRTRefresh(t *testing.T) {
    analyzer := analysis.NewAnalyzer(
        analysis.NewWhitespaceTokenizer(),
        &analysis.LowerCaseFilter{},
    )
    writer := NewNRTIndexWriter(analyzer, 100)

    // ドキュメント追加
    doc := document.NewDocument()
    doc.AddField("body", "hello world", document.FieldTypeText)
    writer.AddDocument(doc)

    // Refresh 前: Reader がない
    gen1 := writer.Generation()

    // Refresh
    changed := writer.Refresh()
    if !changed {
        t.Error("expected refresh to return true")
    }

    gen2 := writer.Generation()
    if gen2 <= gen1 {
        t.Error("expected generation to increase after refresh")
    }

    // Refresh 後: Reader がある
    reader := writer.AcquireReader()
    if reader == nil {
        t.Fatal("expected reader after refresh")
    }
    if reader.TotalDocCount() != 1 {
        t.Errorf("expected 1 doc, got %d", reader.TotalDocCount())
    }

    // 新しいドキュメント追加（まだ検索不可）
    doc2 := document.NewDocument()
    doc2.AddField("body", "hello go", document.FieldTypeText)
    writer.AddDocument(doc2)

    // Refresh 前: まだ1件
    reader = writer.AcquireReader()
    if reader.TotalDocCount() != 1 {
        t.Errorf("expected 1 doc before refresh, got %d", reader.TotalDocCount())
    }

    // 再 Refresh: 2件に
    writer.Refresh()
    reader = writer.AcquireReader()
    if reader.TotalDocCount() != 2 {
        t.Errorf("expected 2 docs after refresh, got %d", reader.TotalDocCount())
    }
}

func TestNRTNoChangeRefresh(t *testing.T) {
    analyzer := analysis.NewAnalyzer(
        analysis.NewWhitespaceTokenizer(),
        &analysis.LowerCaseFilter{},
    )
    writer := NewNRTIndexWriter(analyzer, 100)

    doc := document.NewDocument()
    doc.AddField("body", "hello", document.FieldTypeText)
    writer.AddDocument(doc)
    writer.Refresh()

    // 変更なしの refresh
    changed := writer.Refresh()
    if changed {
        t.Error("expected no change on second refresh without new docs")
    }
}
```

---

## 深掘り：なぜこう設計されているのか

### Q: なぜリアルタイムではなく Near Real-Time なのか？

完全なリアルタイム（addDocument 直後に検索可能）にするには、ドキュメント追加のたびに新しい Segment を作って Reader を更新する必要があります。これは以下の理由で非効率です：

1. **Segment 数の爆発**: 1件ずつ Segment を作ると大量のセグメントが生まれる
2. **Reader の再オープンコスト**: 全 Segment の Reader を毎回更新するのは重い
3. **マージの増加**: Segment が増えるとマージも増える

1秒間隔の refresh なら、その間に追加された全ドキュメントを1つの Segment にまとめられます。

### Q: ES の `refresh_interval: -1` はいつ使うのか？

バルクインデクシング時です。大量のドキュメントを投入する際、refresh を無効にして全件追加後に1回だけ refresh すると、中間 Segment が生まれず効率的です。

```
手動 refresh の流れ:
  PUT /my-index/_settings {"index.refresh_interval": "-1"}
  POST /_bulk { ... 大量のドキュメント ... }
  POST /my-index/_refresh
  PUT /my-index/_settings {"index.refresh_interval": "1s"}
```

### Q: Translog は Lucene の機能？

**Translog は ES の機能** であり、Lucene には含まれていません。Lucene には `IndexWriter.commit()` があるだけで、commit 間のクラッシュ復旧は上位レイヤに任されています。ES が Translog を実装して、Lucene の commit 間の永続化保証を追加しています。

### Q: Refresh と Commit の違いを図でまとめると？

```
          データ追加                   検索可能         永続化
          ─────────                   ──────         ────
addDocument → バッファ                   ✗               ✗
refresh     → Segment (RAM)             ✓               ✗
commit      → Segment (Disk + fsync)    ✓               ✓

ES の Flush ≈ Lucene の Commit
ES の Refresh ≈ Lucene の NRT Reader reopen
```

---

## 次のステップ

NRT 検索が理解できたので、最後の [Step 10: Postings 圧縮 & Skip List](10-compression.md) で、大規模データでの高速化技術を学びます。
