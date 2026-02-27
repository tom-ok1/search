# Step 8: Doc Values & 集計

## 学ぶ概念

転置インデックスは「term → doc」の検索に最適化されています。しかし、**ソートや集計** では逆に「doc → value」のアクセスが必要です。これが **Doc Values**（列指向ストレージ）の役割です。

### 転置インデックス vs Doc Values

```
転置インデックス（行指向的）:
  "tokyo"  → [doc0, doc2, doc5]
  "osaka"  → [doc1, doc3]
  用途: 「tokyo を含むドキュメントは？」

Doc Values（列指向）:
  doc0 → "tokyo"
  doc1 → "osaka"
  doc2 → "tokyo"
  doc3 → "osaka"
  doc4 → "nagoya"
  doc5 → "tokyo"
  用途: 「各ドキュメントの city は？」→ ソート・集計
```

### ES での対応関係

| ES の機能 | 使うストレージ |
|-----------|---------------|
| `match`, `term` クエリ | 転置インデックス |
| `sort` | Doc Values |
| `terms` aggregation | Doc Values |
| `range` aggregation | Doc Values |
| `avg`, `sum`, `max`, `min` | Doc Values |
| `fielddata` (text フィールドの集計) | Doc Values の代替（非推奨） |

### Doc Values の種類（Lucene）

| 種類 | 値の型 | 例 |
|------|--------|-----|
| `NUMERIC` | long | 価格、年齢、タイムスタンプ |
| `BINARY` | byte[] | 任意のバイナリ |
| `SORTED` | 辞書順のバイト列（1値） | keyword フィールド |
| `SORTED_SET` | 辞書順のバイト列（複数値） | タグ |
| `SORTED_NUMERIC` | 複数の long 値 | 複数の数値 |

### なぜ Doc Values が必要か？

転置インデックスから集計を行うのは非常に非効率です：

```
転置インデックスで terms agg（city フィールド）:
  全 term をスキャンして PostingsList を読む
  → term 数が膨大だと遅い（全 term の走査が必要）

Doc Values で terms agg:
  マッチした doc の docID リストを走査
  → 各 doc の value を読んでカウント
  → マッチした doc 数に比例（term 数と無関係）
```

特にフィルタ条件付きの集計（「2024年の売上の合計」など）では、マッチしたドキュメントだけの値を読めばよいので Doc Values が圧倒的に効率的です。

---

## Lucene ソースを読む

### 読むべきファイル（`lucene/lucene/core/src/java/org/apache/lucene/`）

| ファイル | ポイント |
|----------|----------|
| `index/DocValues.java` | Doc Values のファクトリ。各種 DocValues の取得メソッド |
| `index/NumericDocValues.java` | `longValue()` で値を取得。`advance(docID)` で走査 |
| `index/SortedDocValues.java` | `ordValue()` で ord を取得、`lookupOrd()` で値に変換 |
| `index/SortedSetDocValues.java` | 複数値版の SortedDocValues |
| `index/SortedNumericDocValues.java` | 複数値版の NumericDocValues |
| `index/DocValuesType.java` | Doc Values の種類の enum |
| `document/NumericDocValuesField.java` | 数値 Doc Values フィールド |
| `document/SortedDocValuesField.java` | ソート済み Doc Values フィールド |
| `search/SortField.java` | ソート条件の定義 |
| `search/Sort.java` | ソートの組み合わせ |
| `search/TopFieldCollector.java` | ソート条件付きの TopK Collector |
| `search/comparators/NumericComparator.java` | 数値フィールドの比較 |

### SortedDocValues の ord（序数）パターン

Lucene の `SortedDocValues` は文字列値を直接保存せず、**ord（序数）** を使って間接参照します：

```
辞書テーブル:
  ord 0 → "nagoya"
  ord 1 → "osaka"
  ord 2 → "tokyo"

ドキュメント → ord マッピング:
  doc0 → ord 2 (tokyo)
  doc1 → ord 1 (osaka)
  doc2 → ord 2 (tokyo)

メリット:
- ソートは整数（ord）の比較だけでよい
- terms agg は ord でカウントするだけ
- メモリ効率が良い（文字列を繰り返し保存しない）
```

---

## Go で実装する

### 1. Doc Values の型定義

```go
// index/docvalues.go

package index

// NumericDocValues は数値型の Doc Values。
// docID → int64 のマッピング。
type NumericDocValues struct {
    values map[int]int64
}

func NewNumericDocValues() *NumericDocValues {
    return &NumericDocValues{values: make(map[int]int64)}
}

func (dv *NumericDocValues) Set(docID int, value int64) {
    dv.values[docID] = value
}

func (dv *NumericDocValues) Get(docID int) (int64, bool) {
    v, ok := dv.values[docID]
    return v, ok
}

// SortedDocValues は文字列型の Doc Values（ord パターン）。
type SortedDocValues struct {
    // 辞書テーブル: ord → 値
    ordToValue []string
    // 値 → ord の逆引き
    valueToOrd map[string]int
    // docID → ord
    docToOrd map[int]int
}

func NewSortedDocValues() *SortedDocValues {
    return &SortedDocValues{
        valueToOrd: make(map[string]int),
        docToOrd:   make(map[int]int),
    }
}

func (dv *SortedDocValues) Set(docID int, value string) {
    ord, exists := dv.valueToOrd[value]
    if !exists {
        ord = len(dv.ordToValue)
        dv.ordToValue = append(dv.ordToValue, value)
        dv.valueToOrd[value] = ord
    }
    dv.docToOrd[docID] = ord
}

func (dv *SortedDocValues) GetOrd(docID int) (int, bool) {
    ord, ok := dv.docToOrd[docID]
    return ord, ok
}

func (dv *SortedDocValues) LookupOrd(ord int) string {
    if ord < 0 || ord >= len(dv.ordToValue) {
        return ""
    }
    return dv.ordToValue[ord]
}

func (dv *SortedDocValues) GetValue(docID int) (string, bool) {
    ord, ok := dv.docToOrd[docID]
    if !ok {
        return "", false
    }
    return dv.ordToValue[ord], true
}

func (dv *SortedDocValues) OrdCount() int {
    return len(dv.ordToValue)
}
```

### 2. Segment に Doc Values を追加

```go
// index/segment.go に追加

type Segment struct {
    // ... 既存フィールド ...

    // Doc Values
    numericDocValues map[string]*NumericDocValues // fieldName → NumericDocValues
    sortedDocValues  map[string]*SortedDocValues  // fieldName → SortedDocValues
}

// NumericDocValues はフィールドの NumericDocValues を返す。
func (s *Segment) NumericDocValues(field string) (*NumericDocValues, bool) {
    dv, ok := s.numericDocValues[field]
    return dv, ok
}

// SortedDocValues はフィールドの SortedDocValues を返す。
func (s *Segment) SortedDocValues(field string) (*SortedDocValues, bool) {
    dv, ok := s.sortedDocValues[field]
    return dv, ok
}

// DocCount は Segment 内のドキュメント数を返す。
func (s *Segment) DocCount() int {
    return s.docCount
}
```

### 3. IndexWriter に Doc Values 付きドキュメント追加

```go
// index/writer.go に追加

// AddDocumentWithDocValues は Doc Values 付きでドキュメントを追加する。
func (w *IndexWriter) AddDocumentWithDocValues(
    doc *document.Document,
    sortedDVs map[string]string,
    numericDVs map[string]int64,
) error {
    // 通常のドキュメント追加
    docID := w.buffer.docCount
    if err := w.AddDocument(doc); err != nil {
        return err
    }
    // docCount は AddDocument 内で increment されるので、
    // docID はその前の値を使う

    // Sorted Doc Values
    for field, value := range sortedDVs {
        dv, exists := w.buffer.sortedDocValues[field]
        if !exists {
            dv = NewSortedDocValues()
            w.buffer.sortedDocValues[field] = dv
        }
        dv.Set(docID, value)
    }

    // Numeric Doc Values
    for field, value := range numericDVs {
        dv, exists := w.buffer.numericDocValues[field]
        if !exists {
            dv = NewNumericDocValues()
            w.buffer.numericDocValues[field] = dv
        }
        dv.Set(docID, value)
    }

    return nil
}
```

### 4. 集計（Aggregation）

```go
// search/aggregation.go

package search

import (
    "gosearch/index"
    "sort"
)

// AggResult は集計結果。
type AggResult struct {
    Buckets []Bucket
}

// Bucket は集計のバケット。
type Bucket struct {
    Key      string
    DocCount int
}

// TermsAgg は terms aggregation を実行する。
// matchedDocIDs はフィルタ済みのドキュメントIDリスト（グローバルID）。
func TermsAgg(reader *index.IndexReader, field string, matchedDocIDs []int, size int) AggResult {
    counts := make(map[string]int)

    for _, globalDocID := range matchedDocIDs {
        for _, leaf := range reader.Leaves() {
            localDocID := globalDocID - leaf.DocBase
            if localDocID < 0 || localDocID >= leaf.Segment.DocCount() {
                continue
            }

            dv, exists := leaf.Segment.SortedDocValues(field)
            if !exists {
                continue
            }

            value, ok := dv.GetValue(localDocID)
            if ok {
                counts[value]++
            }
            break // この DocID は見つかったので次へ
        }
    }

    var buckets []Bucket
    for key, count := range counts {
        buckets = append(buckets, Bucket{Key: key, DocCount: count})
    }
    // DocCount 降順でソート
    sort.Slice(buckets, func(i, j int) bool {
        return buckets[i].DocCount > buckets[j].DocCount
    })

    if len(buckets) > size {
        buckets = buckets[:size]
    }

    return AggResult{Buckets: buckets}
}

// SumAgg は数値フィールドの合計を計算する。
func SumAgg(reader *index.IndexReader, field string, matchedDocIDs []int) float64 {
    total := 0.0
    for _, globalDocID := range matchedDocIDs {
        for _, leaf := range reader.Leaves() {
            localDocID := globalDocID - leaf.DocBase
            if localDocID < 0 || localDocID >= leaf.Segment.DocCount() {
                continue
            }
            dv, exists := leaf.Segment.NumericDocValues(field)
            if !exists {
                continue
            }
            v, ok := dv.Get(localDocID)
            if ok {
                total += float64(v)
            }
            break
        }
    }
    return total
}

// AvgAgg は数値フィールドの平均を計算する。
func AvgAgg(reader *index.IndexReader, field string, matchedDocIDs []int) float64 {
    if len(matchedDocIDs) == 0 {
        return 0
    }
    return SumAgg(reader, field, matchedDocIDs) / float64(len(matchedDocIDs))
}
```

### 5. ソート付き検索

```go
// search/sort.go

package search

import (
    "gosearch/index"
    "sort"
)

// SortFieldType はソート条件の種類。
type SortFieldType int

const (
    SortFieldScore   SortFieldType = iota // スコア順（デフォルト）
    SortFieldNumeric                       // 数値フィールド
    SortFieldString                        // 文字列フィールド
)

// SortField はソート条件。
type SortField struct {
    Field   string
    Type    SortFieldType
    Reverse bool // true なら降順
}

// SortedSearch はソート条件付きで検索する。
func (s *IndexSearcher) SortedSearch(queryField, queryTerm string, topK int, sortBy SortField) []SearchResult {
    // まず全マッチを取得
    allResults := s.Search(queryField, queryTerm, 100000)

    // ソート
    switch sortBy.Type {
    case SortFieldScore:
        sort.Slice(allResults, func(i, j int) bool {
            if sortBy.Reverse {
                return allResults[i].Score < allResults[j].Score
            }
            return allResults[i].Score > allResults[j].Score
        })

    case SortFieldNumeric:
        sort.Slice(allResults, func(i, j int) bool {
            vi := getNumericValue(s.reader, sortBy.Field, allResults[i].DocID)
            vj := getNumericValue(s.reader, sortBy.Field, allResults[j].DocID)
            if sortBy.Reverse {
                return vi > vj
            }
            return vi < vj
        })

    case SortFieldString:
        sort.Slice(allResults, func(i, j int) bool {
            vi := getSortedValue(s.reader, sortBy.Field, allResults[i].DocID)
            vj := getSortedValue(s.reader, sortBy.Field, allResults[j].DocID)
            if sortBy.Reverse {
                return vi > vj
            }
            return vi < vj
        })
    }

    if len(allResults) > topK {
        allResults = allResults[:topK]
    }
    return allResults
}

func getNumericValue(reader *index.IndexReader, field string, globalDocID int) int64 {
    for _, leaf := range reader.Leaves() {
        localDocID := globalDocID - leaf.DocBase
        if localDocID >= 0 && localDocID < leaf.Segment.DocCount() {
            dv, exists := leaf.Segment.NumericDocValues(field)
            if !exists {
                return 0
            }
            v, _ := dv.Get(localDocID)
            return v
        }
    }
    return 0
}

func getSortedValue(reader *index.IndexReader, field string, globalDocID int) string {
    for _, leaf := range reader.Leaves() {
        localDocID := globalDocID - leaf.DocBase
        if localDocID >= 0 && localDocID < leaf.Segment.DocCount() {
            dv, exists := leaf.Segment.SortedDocValues(field)
            if !exists {
                return ""
            }
            v, _ := dv.GetValue(localDocID)
            return v
        }
    }
    return ""
}
```

---

## 確認・テスト

```go
// search/aggregation_test.go

package search

import (
    "testing"

    "gosearch/analysis"
    "gosearch/document"
    "gosearch/index"
)

func TestTermsAggregation(t *testing.T) {
    analyzer := analysis.NewAnalyzer(
        analysis.NewWhitespaceTokenizer(),
        &analysis.LowerCaseFilter{},
    )
    writer := index.NewIndexWriter(analyzer, 100)

    data := []struct {
        body  string
        city  string
        price int64
    }{
        {"laptop computer", "tokyo", 1000},
        {"desktop computer", "osaka", 800},
        {"laptop bag", "tokyo", 50},
        {"mouse pad", "nagoya", 10},
        {"keyboard accessory", "tokyo", 100},
    }

    for _, d := range data {
        doc := document.NewDocument()
        doc.AddField("body", d.body, document.FieldTypeText)
        writer.AddDocumentWithDocValues(
            doc,
            map[string]string{"city": d.city},
            map[string]int64{"price": d.price},
        )
    }
    writer.Flush()

    reader := index.NewIndexReader(writer.Segments())

    // 全ドキュメントで terms agg
    allDocs := []int{0, 1, 2, 3, 4}
    result := TermsAgg(reader, "city", allDocs, 10)

    if result.Buckets[0].Key != "tokyo" || result.Buckets[0].DocCount != 3 {
        t.Errorf("expected tokyo:3, got %s:%d",
            result.Buckets[0].Key, result.Buckets[0].DocCount)
    }

    // 価格の合計
    sum := SumAgg(reader, "price", allDocs)
    if sum != 1960 {
        t.Errorf("expected sum=1960, got %f", sum)
    }

    // 価格の平均
    avg := AvgAgg(reader, "price", allDocs)
    expected := 1960.0 / 5.0
    if avg != expected {
        t.Errorf("expected avg=%f, got %f", expected, avg)
    }
}
```

---

## 深掘り：なぜこう設計されているのか

### Q: なぜ転置インデックスで集計しないのか？

転置インデックスで `terms agg` を行うと：

1. フィールドの **全ユニーク term を走査** する必要がある
2. 各 term の PostingsList とマッチ対象 DocID の交差を取る必要がある

Doc Values なら、マッチした DocID を順に走査して値を読むだけ。マッチ数に比例した計算量で済みます。

### Q: ES の `fielddata` とは何か？

ES の古いバージョンでは、`text` フィールドの集計に `fielddata` を使っていました。これは転置インデックスをメモリ上で反転させて Doc Values 的な構造を動的に構築するものです。メモリを大量消費するため非推奨になり、現在は `keyword` フィールド + Doc Values が推奨されています。

### Q: SortedDocValues の ord パターンはなぜ効率的か？

1. **メモリ**: 同じ文字列を複数回保存しない（辞書テーブルに1回だけ）
2. **比較**: 文字列比較（O(n)）ではなく整数比較（O(1)）
3. **圧縮**: ord は連番 → ビットパッキングで少ないビット数に収まる

例: 100万ドキュメント × 都道府県（47種類）なら ord は 6ビットで表現可能。100万 × 6ビット ≈ 750KB。

### Q: Doc Values とカラムナーストレージの関係

Doc Values の発想は BigQuery や ClickHouse などの **列指向データベース** と同じです。列指向は「特定カラムの全行を読む」操作に最適化されており、集計に圧倒的に強いです。

---

## 次のステップ

Doc Values と集計が実装できたので、次は [Step 9: Near Real-Time 検索](09-nrt.md) で、ドキュメント追加後すぐに検索可能になる仕組みを学びます。
