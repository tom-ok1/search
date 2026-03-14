# gosearch vs Lucene: 検索パイプライン設計差分

本ドキュメントでは、gosearch の検索パイプラインと Apache Lucene (Java) の設計を比較し、主要な差分と修正方針を整理する。

## 参照ソース

### gosearch (Go)

| ファイル | 概要 |
|---------|------|
| `search/query.go` | Query インターフェース、DocScore 型 |
| `search/term_query.go` | TermQuery 実装 |
| `search/boolean_query.go` | BooleanQuery 実装 |
| `search/phrase_query.go` | PhraseQuery 実装 |
| `search/searcher.go` | IndexSearcher (Search / SearchWithSort) |
| `search/collector.go` | TopKCollector (スコア順) |
| `search/top_field_collector.go` | TopFieldCollector (フィールドソート) |
| `search/field_comparator.go` | FieldComparator インターフェースと実装 |
| `search/sort.go` | Sort / SortField 定義 |
| `search/bm25.go` | BM25Scorer |
| `index/segment.go` | SegmentReader インターフェース |

### Lucene (Java)

| ファイル | 概要 |
|---------|------|
| `lucene/lucene/core/src/java/org/apache/lucene/search/Query.java` | Query 抽象クラス (createWeight, rewrite) |
| `lucene/lucene/core/src/java/org/apache/lucene/search/Weight.java` | Weight 抽象クラス (scorer, scorerSupplier) |
| `lucene/lucene/core/src/java/org/apache/lucene/search/Scorer.java` | Scorer 抽象クラス (iterator, score) |
| `lucene/lucene/core/src/java/org/apache/lucene/search/IndexSearcher.java` | IndexSearcher (search, searchLeaf) |
| `lucene/lucene/core/src/java/org/apache/lucene/search/Collector.java` | Collector インターフェース |
| `lucene/lucene/core/src/java/org/apache/lucene/search/LeafCollector.java` | LeafCollector インターフェース |
| `lucene/lucene/core/src/java/org/apache/lucene/search/TopDocsCollector.java` | TopDocsCollector 抽象クラス |
| `lucene/lucene/core/src/java/org/apache/lucene/search/TopScoreDocCollector.java` | スコア順 Collector |
| `lucene/lucene/core/src/java/org/apache/lucene/search/TopFieldCollector.java` | フィールドソート Collector |
| `lucene/lucene/core/src/java/org/apache/lucene/search/FieldComparator.java` | FieldComparator 抽象クラス (グローバル) |
| `lucene/lucene/core/src/java/org/apache/lucene/search/LeafFieldComparator.java` | LeafFieldComparator (セグメント別) |
| `lucene/lucene/core/src/java/org/apache/lucene/search/Sort.java` | Sort 定義 |
| `lucene/lucene/core/src/java/org/apache/lucene/search/SortField.java` | SortField 定義 |
| `lucene/lucene/core/src/java/org/apache/lucene/search/CollectorManager.java` | 並列検索用 CollectorManager |

---

## パイプライン全体像の比較

### Lucene

```
Query
  │ rewrite()
  ▼
Query (primitive form)
  │ createWeight(IndexSearcher, ScoreMode, boost)
  ▼
Weight                          ← IndexSearcher 依存の状態を保持
  │ scorerSupplier(LeafReaderContext)
  ▼
Scorer                          ← セグメント依存の状態を保持
  │ iterator() → DocIdSetIterator (1件ずつイテレート)
  ▼
LeafCollector.collect(int doc)  ← doc 単位で呼ばれる
  │
  ▼
TopDocs (結果)
```

- `IndexSearcher.search(Query, Collector)` が**唯一のエントリポイント**
- score 順 / field ソートの違いは Collector の差し替えで吸収
- `searchLeaf()` 内で `BulkScorer.score(leafCollector, liveDocs, min, max)` を呼ぶ

参照: `lucene/.../IndexSearcher.java:627-634` (deprecated search), `lucene/.../IndexSearcher.java:820-852` (searchLeaf)

### gosearch

```
Query
  │ Execute(SegmentReader) → []DocScore (全マッチをスライスで返す)
  ▼
IndexSearcher のループ
  │ IsDeleted チェック、StoredFields 取得
  ▼
Collector.Collect(...)          ← TopKCollector と TopFieldCollector で API が異なる
  │
  ▼
[]SearchResult (結果)
```

- `Search()` と `SearchWithSort()` の**2つのエントリポイント**がある
- Query が直接マッチング＋スコアリングを行い、結果をバッチで返す

参照: `search/searcher.go:23-43` (Search), `search/searcher.go:47-63` (SearchWithSort)

---

## 差分1: Query が `[]DocScore` をバッチで返す

### 現状 (gosearch)

```go
// search/query.go:6-9
type Query interface {
    Execute(seg index.SegmentReader) []DocScore
}
```

`Execute()` は全マッチを `[]DocScore` スライスとして一括で返す。
各 Query 実装 (`search/term_query.go:15-39`, `search/boolean_query.go:37`, `search/phrase_query.go:20`) が
マッチング・スコアリング・結果のメモリ確保をすべて自分で行う。

### Lucene の設計

```java
// Query.java:67-70
public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)

// Weight.java:129-135
public final Scorer scorer(LeafReaderContext context)

// Scorer.java:43
public abstract DocIdSetIterator iterator()
```

3層に分離されている:
- **Query**: 不変。検索条件の定義のみ
- **Weight**: IndexSearcher 依存の状態 (IDF 等の統計値) を保持
- **Scorer**: セグメント依存の状態を保持し、**DocIdSetIterator** でドキュメントを1件ずつ返す

### 影響

| 項目 | 説明 |
|------|------|
| メモリ | 全マッチを `[]DocScore` に展開するため、大量マッチ時にメモリ消費が大きい |
| Early termination | Collector が「もう十分」と判断しても Query の実行を止められない |
| スコア計算の省略 | `ScoreMode` の概念がないため、ソートのみの場合もスコアを計算する |

### 修正方針

```go
type Query interface {
    CreateWeight(searcher *IndexSearcher, scoreMode ScoreMode) Weight
}

type Weight interface {
    Scorer(leaf LeafReaderContext) Scorer
}

type Scorer interface {
    Iterator() DocIdSetIterator
    Score() float64
}

type DocIdSetIterator interface {
    NextDoc() int
    Advance(target int) int
    DocID() int
}
```

---

## 差分2: Collector に共通インターフェースがない

### 現状 (gosearch)

`TopKCollector` と `TopFieldCollector` はまったく別の型で、共通インターフェースを持たない。

```go
// search/collector.go:20
func (c *TopKCollector) Collect(result SearchResult)

// search/top_field_collector.go:67
func (c *TopFieldCollector) Collect(localDocID int, score float64, globalDocID int)
```

シグネチャが異なるため、`IndexSearcher` に2つの search メソッドが必要:

```go
// search/searcher.go:23
func (s *IndexSearcher) Search(q Query, topK int) []SearchResult

// search/searcher.go:47
func (s *IndexSearcher) SearchWithSort(q Query, topK int, sort *Sort) []SearchResult
```

2つのメソッドの内部ループはほぼ同じ構造だが、Collector の API が違うために重複している。

### Lucene の設計

```java
// Collector.java:46-58
public interface Collector {
    LeafCollector getLeafCollector(LeafReaderContext context) throws IOException;
    ScoreMode scoreMode();
}

// LeafCollector.java:64-85
public interface LeafCollector {
    void setScorer(Scorable scorer) throws IOException;
    void collect(int doc) throws IOException;
}
```

- **Collector** がトップレベルのインターフェース
- **LeafCollector** がセグメント単位で `collect(int doc)` を受ける
- `TopScoreDocCollector` も `TopFieldCollector` も同じ `Collector` インターフェースを実装
- `IndexSearcher.search(Query, Collector)` が**1つ**で済む

参照: `lucene/.../Collector.java:46-64`, `lucene/.../LeafCollector.java:64-85`

### 影響

| 項目 | 説明 |
|------|------|
| 検索メソッドの重複 | `Search()` と `SearchWithSort()` のループがほぼ同一 |
| 拡張性 | 新しい collect 方式 (例: グルーピング、ファセット) を追加するたびに新しい search メソッドが必要 |
| deleted docs 処理 | 本来 Scorer/BulkScorer が `liveDocs` を使って内部処理すべきだが、Searcher のループに漏れている (`search/searcher.go:29-31`, `search/searcher.go:53-55`) |

### 修正方針

```go
type Collector interface {
    GetLeafCollector(leaf LeafReaderContext) LeafCollector
    ScoreMode() ScoreMode
}

type LeafCollector interface {
    SetScorer(scorer Scorable)
    Collect(doc int)
}

// IndexSearcher は1つの search メソッドに統一
func (s *IndexSearcher) Search(q Query, collector Collector) {
    weight := q.CreateWeight(s, collector.ScoreMode())
    for _, leaf := range s.reader.Leaves() {
        leafCollector := collector.GetLeafCollector(leaf)
        scorer := weight.Scorer(leaf)
        liveDocs := leaf.Segment.LiveDocs()
        for doc := scorer.Iterator().NextDoc(); doc != NO_MORE_DOCS; doc = scorer.Iterator().NextDoc() {
            if liveDocs != nil && !liveDocs.Get(doc) {
                continue
            }
            leafCollector.Collect(doc)
        }
    }
}
```

---

## 差分3: StoredFields の取得タイミング

### 現状 (gosearch)

```go
// search/searcher.go:33
stored, _ := leaf.Segment.StoredFields(ds.DocID)
```

`Search()` の collect ループ内で、top-K に入るかどうかに関係なく**全マッチ**に対して `StoredFields()` を呼んでいる。
`SearchWithSort()` ではこの問題はないが、結果に `Fields` が含まれない。

### Lucene の設計

collect 時には docID のみを扱い、StoredFields は**結果確定後**に必要なドキュメントだけ取得する。

```java
// LeafCollector.java:84-85 のコメント:
// NOTE: This is called in an inner search loop. For good search performance,
// implementations of this method should not call StoredFields.document on every hit.
```

### 影響

top-K = 10 でマッチが 10,000 件あった場合、9,990 回の不要なディスク I/O が発生する。

### 修正方針

Collect ループでは docID とスコアのみを扱い、`Results()` で最終的な top-K が確定した後に StoredFields を取得する。

```go
// 結果確定後に取得
func (s *IndexSearcher) storedFieldsFor(results []ScoreDoc) []SearchResult {
    out := make([]SearchResult, len(results))
    for i, sd := range results {
        // globalDocID → leaf + localDocID に逆引き
        leaf, localDoc := s.reader.ResolveDocID(sd.Doc)
        stored, _ := leaf.Segment.StoredFields(localDoc)
        out[i] = SearchResult{
            DocID:  sd.Doc,
            Score:  sd.Score,
            Fields: stored,
        }
    }
    return out
}
```

---

## 差分4: FieldComparator が1層のみ → 対応済み

Lucene に倣い `FieldComparator`（グローバル）と `LeafFieldComparator`（セグメントローカル）に分離済み。

```go
// search/field_comparator.go
type FieldComparator interface {
    CompareSlots(slot1, slot2 int) int
    Value(slot int) interface{}
    GetLeafComparator(seg index.SegmentReader) LeafFieldComparator
}

type LeafFieldComparator interface {
    SetBottom(slot int)
    CompareBottom(docID int) int
    Copy(slot int, docID int)
    SetScorer(score float64)
}
```

### 残課題

| 項目 | 説明 |
|------|------|
| `CompareTop` | deep paging (`searchAfter`) の実装時に `LeafFieldComparator` へ追加 |
| `CompetitiveIterator` | 非競合ドキュメントのスキップ最適化が必要になった段階で `LeafFieldComparator` へ追加 |

---

## 差分5: ScoreMode がない

### 現状 (gosearch)

スコアが必要かどうかを Query / Scorer に伝える仕組みがない。
フィールドソートのみの場合でも、`Execute()` 内で BM25 スコアが計算される。

参照: `search/term_query.go:21-35` (常に BM25 スコアを計算)

### Lucene の設計

```java
// ScoreMode は Collector が宣言する
public enum ScoreMode {
    COMPLETE,           // 全ドキュメントのスコアが必要
    COMPLETE_NO_SCORES, // スコア不要
    TOP_SCORES,         // 上位のスコアのみ (非競合ドキュメントはスキップ可能)
    TOP_DOCS            // スコア不要、docID 順でもよい
}
```

`Collector.scoreMode()` を `Query.createWeight()` に渡すことで、スコア不要な場合は計算をスキップする。

### 修正方針

Collector 統一インターフェース導入時に `ScoreMode()` を含め、`Weight.Scorer()` に伝播させる。

---

## 差分6: CollectorManager (並列検索) がない

### Lucene の設計

```java
// CollectorManager.java
public interface CollectorManager<C extends Collector, T> {
    C newCollector() throws IOException;          // スレッドごとに Collector を生成
    T reduce(Collection<C> collectors) throws IOException;  // 結果をマージ
}
```

`IndexSearcher` はリーフスライスごとに別スレッドで検索し、最後に `reduce()` でマージする。

参照: `lucene/.../CollectorManager.java`, `lucene/.../IndexSearcher.java:739`

### gosearch の現状

並列検索の仕組みはない。現時点では優先度は低いが、将来の拡張ポイントとして認識しておく。

---

## 修正の優先度

| 優先度 | 差分 | 理由 |
|--------|------|------|
| **1** | Collector 統一インターフェース (差分2) | Search メソッドの統一。変更範囲が比較的小さく効果が大きい |
| **2** | StoredFields の遅延取得 (差分3) | パフォーマンス改善。独立して対応可能 |
| **3** | Weight/Scorer 導入 (差分1) | イテレータベース化。最も大きなリファクタだが、early termination・skip の基盤になる |
| **4** | ScoreMode (差分5) | Weight/Scorer 導入と同時に対応 |
| **5** | FieldComparator 分離 (差分4) | competitiveIterator / searchAfter が必要になった段階で |
| **6** | CollectorManager (差分6) | 並列検索が必要になった段階で |
