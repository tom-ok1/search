# Step 3: BM25 スコアリング

## 学ぶ概念

転置インデックスで「どのドキュメントがマッチするか」は分かるようになりました。しかし、100件マッチした中で **どれが最も関連性が高いか** を決める仕組みが必要です。これが **スコアリング** です。

### TF-IDF から BM25 へ

歴史的には **TF-IDF** が使われてきましたが、Lucene 6.0 以降は **BM25** がデフォルトです（ES も同様）。

#### TF-IDF の直感

- **TF（Term Frequency）**: ドキュメント内で多く出現する term ほど関連性が高い
- **IDF（Inverse Document Frequency）**: 全ドキュメント中でレアな term ほど価値が高い

例: `"the"` はほぼ全ドキュメントに出現するので IDF が低い（価値が低い）。`"lucene"` はレアなので IDF が高い（価値が高い）。

#### BM25 の改良点

TF-IDF の問題点は、term が100回出現するドキュメントが10回出現するドキュメントの10倍のスコアになってしまうこと。BM25 は **TF の飽和（saturation）** を導入して、出現回数が一定以上になるとスコアの伸びが鈍化するようにしました。

### BM25 の数式

```
score(D, Q) = Σ IDF(qi) × (tf(qi, D) × (k1 + 1)) / (tf(qi, D) + k1 × (1 - b + b × |D| / avgDL))
```

各変数の意味：

| 変数 | 意味 | 典型値 |
|------|------|--------|
| `tf(qi, D)` | term qi がドキュメント D に出現する回数 | - |
| `IDF(qi)` | term qi の逆文書頻度 | - |
| `k1` | TF の飽和をコントロール。大きいほど TF の影響が大きい | 1.2 |
| `b` | 文書長の正規化の度合い。0だと無視、1だと完全に正規化 | 0.75 |
| `\|D\|` | ドキュメント D のトークン数（フィールド長） | - |
| `avgDL` | 全ドキュメントの平均トークン数 | - |

IDF の計算式（Lucene の実装）:

```
IDF(qi) = ln(1 + (N - n + 0.5) / (n + 0.5))
```

- `N`: 全ドキュメント数
- `n`: term qi を含むドキュメント数

---

## Lucene ソースを読む

### 読むべきファイル（`lucene/lucene/core/src/java/org/apache/lucene/`）

| ファイル | ポイント |
|----------|----------|
| `search/similarities/BM25Similarity.java` | BM25 の実装本体。`score()` メソッドを追う |
| `search/Similarity.java` | スコアリングの抽象基底クラス。`SimScorer` が実際のスコア計算 |
| `search/TermStatistics.java` | term のドキュメント頻度（df）とコレクション内総出現回数（totalTermFreq）を保持 |
| `search/CollectionStatistics.java` | コレクション全体の統計情報（docCount, sumTotalTermFreq など） |
| `search/TermQuery.java` | 単一 term 検索。`createWeight()` で BM25 のスコア計算準備 |
| `search/TermScorer.java` | PostingsEnum を走査しながらスコアを計算 |
| `search/IndexSearcher.java` | 検索のエントリーポイント。`search()` メソッド |
| `search/TopScoreDocCollector.java` | 上位N件のスコアを持つドキュメントを収集 |
| `index/FieldInvertState.java` | フィールドのトークン数（length）を保持。norms の計算に使う |

### Lucene の検索フロー

```
IndexSearcher.search(query, collector)
  → query.createWeight(searcher)
    → BM25Similarity.scorer(stats)  // IDF などの事前計算
  → weight.scorer(leafReaderContext)
    → TermScorer(postingsEnum, simScorer)
  → scorer.iterator() で PostingsEnum を走査
    → 各 doc に対して scorer.score() を呼ぶ
      → BM25 の score 計算
  → collector で上位N件を収集
```

---

## Go で実装する

### 1. BM25 スコアラー

```go
// search/bm25.go

package search

import "math"

// BM25 のデフォルトパラメータ
const (
    DefaultK1 = 1.2
    DefaultB  = 0.75
)

// BM25Scorer は BM25 スコアリングを実装する。
type BM25Scorer struct {
    K1 float64
    B  float64
}

func NewBM25Scorer() *BM25Scorer {
    return &BM25Scorer{
        K1: DefaultK1,
        B:  DefaultB,
    }
}

// IDF は逆文書頻度を計算する。
// docCount: 全ドキュメント数
// docFreq: その term を含むドキュメント数
func (s *BM25Scorer) IDF(docCount, docFreq int) float64 {
    return math.Log(1 + float64(docCount-docFreq+0.5)/float64(docFreq+0.5))
}

// Score は1つの term に対するドキュメントのスコアを計算する。
// tf: そのドキュメント内での term の出現回数
// docLen: そのドキュメントのトークン数
// avgDocLen: 全ドキュメントの平均トークン数
// idf: 事前計算済みの IDF 値
func (s *BM25Scorer) Score(tf float64, docLen float64, avgDocLen float64, idf float64) float64 {
    tfNorm := (tf * (s.K1 + 1)) / (tf + s.K1*(1-s.B+s.B*docLen/avgDocLen))
    return idf * tfNorm
}
```

### 2. TopK Collector

```go
// search/collector.go

package search

import (
    "container/heap"
)

// TopKCollector は上位K件のスコアを持つドキュメントを収集する。
// min-heap を使い、最もスコアの低いドキュメントを常に把握する。
type TopKCollector struct {
    k       int
    results minHeap
}

func NewTopKCollector(k int) *TopKCollector {
    return &TopKCollector{
        k:       k,
        results: make(minHeap, 0, k),
    }
}

// Collect はドキュメントを収集する。
// 上位K件に入る場合のみ保持する。
func (c *TopKCollector) Collect(result SearchResult) {
    if len(c.results) < c.k {
        heap.Push(&c.results, result)
    } else if result.Score > c.results[0].Score {
        c.results[0] = result
        heap.Fix(&c.results, 0)
    }
}

// Results は収集したドキュメントをスコアの降順で返す。
func (c *TopKCollector) Results() []SearchResult {
    sorted := make([]SearchResult, len(c.results))
    for i := len(c.results) - 1; i >= 0; i-- {
        sorted[i] = heap.Pop(&c.results).(SearchResult)
    }
    return sorted
}

// min-heap の実装（スコアが低い順）
type minHeap []SearchResult

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x interface{}) {
    *h = append(*h, x.(SearchResult))
}

func (h *minHeap) Pop() interface{} {
    old := *h
    n := len(old)
    result := old[n-1]
    *h = old[:n-1]
    return result
}
```

### 3. BM25 を使った検索

```go
// search/searcher.go

package search

import "gosearch/index"

// SearchResult は検索結果の1件を表す。
type SearchResult struct {
    DocID  int
    Score  float64
    Fields map[string]string
}

// TermSearch は単一 term で BM25 スコアリング付き検索を行う。
func TermSearch(idx *index.InMemoryIndex, field, term string, topK int) []SearchResult {
    pl := idx.GetPostings(field, term)
    if pl == nil {
        return nil
    }

    scorer := NewBM25Scorer()

    // 統計情報の計算
    docCount := idx.DocCount()
    docFreq := len(pl.Postings)
    idf := scorer.IDF(docCount, docFreq)
    avgDocLen := idx.AvgFieldLength(field)

    collector := NewTopKCollector(topK)

    for _, posting := range pl.Postings {
        docLen := float64(idx.FieldLength(field, posting.DocID))
        score := scorer.Score(float64(posting.Freq), docLen, avgDocLen, idf)

        collector.Collect(SearchResult{
            DocID:  posting.DocID,
            Score:  score,
            Fields: idx.GetStoredFields(posting.DocID),
        })
    }

    return collector.Results()
}
```

### 4. InMemoryIndex に統計情報を追加

Step 2 の `InMemoryIndex` に以下のメソッドを追加する必要があります：

```go
// index/index.go に追加

// FieldLength はドキュメントのフィールドのトークン数を返す。
// BM25 の文書長正規化に使用。
func (idx *InMemoryIndex) FieldLength(fieldName string, docID int) int {
    length, exists := idx.fieldLengths[fieldName]
    if !exists {
        return 0
    }
    if docID >= len(length) {
        return 0
    }
    return length[docID]
}

// AvgFieldLength はフィールドの平均トークン数を返す。
func (idx *InMemoryIndex) AvgFieldLength(fieldName string) float64 {
    lengths, exists := idx.fieldLengths[fieldName]
    if !exists {
        return 0
    }
    total := 0
    count := 0
    for _, l := range lengths {
        if l > 0 {
            total += l
            count++
        }
    }
    if count == 0 {
        return 0
    }
    return float64(total) / float64(count)
}
```

`InMemoryIndex` の struct に `fieldLengths` を追加：

```go
type InMemoryIndex struct {
    analyzer     *analysis.Analyzer
    fields       map[string]*FieldIndex
    docCount     int
    storedFields map[int]map[string]string
    fieldLengths map[string][]int // fieldName → docID → token count
}
```

`indexTextField` 内でフィールド長を記録：

```go
func (idx *InMemoryIndex) indexTextField(docID int, field document.Field) error {
    tokens, err := idx.analyzer.Analyze(field.Value)
    if err != nil {
        return err
    }

    // フィールド長の記録
    if idx.fieldLengths[field.Name] == nil {
        idx.fieldLengths[field.Name] = make([]int, 0)
    }
    for len(idx.fieldLengths[field.Name]) <= docID {
        idx.fieldLengths[field.Name] = append(idx.fieldLengths[field.Name], 0)
    }
    idx.fieldLengths[field.Name][docID] = len(tokens)

    // ... 以下は Step 2 と同じ
}
```

---

## 確認・テスト

```go
// search/bm25_test.go

package search

import (
    "math"
    "testing"

    "gosearch/analysis"
    "gosearch/document"
    "gosearch/index"
)

func TestBM25IDF(t *testing.T) {
    scorer := NewBM25Scorer()

    // レアな term ほど IDF が高い
    idfRare := scorer.IDF(1000, 5)    // 1000件中5件にしか出ない
    idfCommon := scorer.IDF(1000, 500) // 1000件中500件に出る
    if idfRare <= idfCommon {
        t.Errorf("rare term should have higher IDF: rare=%f, common=%f", idfRare, idfCommon)
    }

    // 全ドキュメントに出現する term の IDF はほぼ 0
    idfAll := scorer.IDF(1000, 1000)
    if idfAll > 0.1 {
        t.Errorf("term in all docs should have near-zero IDF: %f", idfAll)
    }
}

func TestBM25Scoring(t *testing.T) {
    analyzer := analysis.NewAnalyzer(
        analysis.NewWhitespaceTokenizer(),
        &analysis.LowerCaseFilter{},
    )
    idx := index.NewInMemoryIndex(analyzer)

    // doc0: "fox" が2回出現する短いドキュメント
    doc0 := document.NewDocument()
    doc0.AddField("body", "fox fox", document.FieldTypeText)
    idx.AddDocument(doc0)

    // doc1: "fox" が1回出現する長いドキュメント
    doc1 := document.NewDocument()
    doc1.AddField("body", "the quick brown fox jumps over the lazy dog", document.FieldTypeText)
    idx.AddDocument(doc1)

    // doc2: "fox" を含まない
    doc2 := document.NewDocument()
    doc2.AddField("body", "the lazy dog sleeps all day", document.FieldTypeText)
    idx.AddDocument(doc2)

    results := TermSearch(idx, "body", "fox", 10)

    // doc0 と doc1 のみがマッチ
    if len(results) != 2 {
        t.Fatalf("expected 2 results, got %d", len(results))
    }

    // doc0 のほうが高スコア（短いドキュメントで fox が多い）
    if results[0].DocID != 0 {
        t.Errorf("expected doc0 first, got doc%d", results[0].DocID)
    }

    // スコアは正の値
    for _, r := range results {
        if r.Score <= 0 {
            t.Errorf("expected positive score, got %f", r.Score)
        }
    }
}

func TestTopKCollector(t *testing.T) {
    collector := NewTopKCollector(2)

    collector.Collect(SearchResult{DocID: 0, Score: 1.0})
    collector.Collect(SearchResult{DocID: 1, Score: 3.0})
    collector.Collect(SearchResult{DocID: 2, Score: 2.0})

    results := collector.Results()
    if len(results) != 2 {
        t.Fatalf("expected 2 results, got %d", len(results))
    }
    // スコア降順
    if results[0].DocID != 1 || results[1].DocID != 2 {
        t.Errorf("expected [doc1, doc2], got [doc%d, doc%d]",
            results[0].DocID, results[1].DocID)
    }
}
```

---

## 深掘り：なぜこう設計されているのか

### Q: なぜ BM25 が TF-IDF より良いのか？

TF-IDF では `tf` がそのままスコアに反映されるため、スパムのように同じ単語を大量に含むドキュメントが不当に高スコアになります。BM25 の `k1` パラメータにより、tf が一定以上になると飽和するため、この問題が軽減されます。

```
TF-IDF:  tf=10 → score=10,  tf=100 → score=100
BM25:    tf=10 → score≈5.2, tf=100 → score≈5.9 （k1=1.2の場合）
```

### Q: Lucene の norms とは何か？

Lucene ではフィールド長（トークン数）をそのまま保存するのではなく、**norms** という1バイトにエンコードした値を保持します。これにより「ドキュメントのフィールドが長い → 1つの term の重要度は下がる」という文書長正規化を効率よく行えます。

我々の実装では `fieldLengths` に正確な値を保存していますが、Lucene ではメモリ節約のため精度を落としています。

### Q: なぜ Collector パターンを使うのか？

Lucene は「結果の収集方法」を `Collector` インターフェースで抽象化しています。これにより：

- **TopScoreDocCollector**: 上位N件をスコア順で収集
- **TopFieldCollector**: ソート条件指定で収集
- **TotalHitCountCollector**: マッチ件数だけカウント

など、同じ検索エンジンで異なる収集戦略を取れます。ES の `size: 0` で集計だけする場合などはスコア計算自体をスキップできます。

---

## 次のステップ

スコアリングができたので、次は [Step 4: Boolean & Phrase Query](04-queries.md) で、AND/OR/NOT の組み合わせ検索とフレーズ検索を実装します。
