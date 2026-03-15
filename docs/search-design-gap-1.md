# Search Design Gap

## Status Summary

現在の実装では `Query.CreateScorer(ctx, scoreMode)` が `IndexSearcher` を受け取らず、各セグメント内の統計のみでスコアリングを行っている。マルチセグメント環境では IDF や平均フィールド長がセグメントごとに異なり、スコアの一貫性が失われる。Lucene の `Weight` 中間層パターンを導入することでこの問題を解決する。

---

## 問題: セグメントローカルな統計によるスコアリングの不正確さ

### 現在の実装

```
IndexSearcher.Search(query, collector)
  └─ for each leaf:
       scorer := query.CreateScorer(leaf, scoreMode)   // セグメント内の統計のみ使用
       collector.Collect(scorer)
```

- `Query` インターフェース (`search/query.go`):
  ```go
  type Query interface {
      CreateScorer(ctx index.LeafReaderContext, scoreMode ScoreMode) Scorer
  }
  ```
- `TermQuery.CreateScorer` (`search/term_query.go`) は `ctx.Segment.DocFreq()`, `ctx.Segment.LiveDocCount()`, `ctx.Segment.TotalFieldLength()` でセグメント内の統計を取得し、BM25 の IDF と平均ドキュメント長を計算している
- `PhraseQuery.CreateScorer` (`search/phrase_query.go`) も同様

### 問題点

マルチセグメント環境では以下が発生する:
1. **IDF の不一致**: セグメント A (docFreq=1, docCount=100) とセグメント B (docFreq=50, docCount=100) で同じタームの IDF が異なる
2. **平均ドキュメント長の不一致**: セグメントごとにフィールドの平均長が異なる
3. **スコアの比較不能**: セグメント間でスコアの基準が異なるため、TopK のマージ結果が不正確になる

---

## 解決策: Weight 中間層の導入

Lucene の3層分離パターンを採用する:

```
Query  → 不変のクエリ構造。検索間で再利用可能
Weight → 検索ごとに生成。コレクションレベルの統計を保持
Scorer → セグメントごとに生成。Weight の事前計算値を使ってスコアリング
```

### Lucene のリファレンス実装

#### Query.createWeight

`lucene/core/src/java/org/apache/lucene/search/Query.java`:
```java
public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
```

#### IndexSearcher.createWeight

`lucene/core/src/java/org/apache/lucene/search/IndexSearcher.java`:
```java
public Weight createWeight(Query query, ScoreMode scoreMode, float boost) throws IOException {
    Weight weight = query.createWeight(this, scoreMode, boost);
    // キャッシュ処理...
    return weight;
}
```

IndexSearcher は自身 (`this`) を Query に渡すことで、Query が統計情報にアクセスできるようにする。

#### IndexSearcher のコレクション統計メソッド

`lucene/core/src/java/org/apache/lucene/search/IndexSearcher.java`:
```java
// 全セグメントを横断して集計
public CollectionStatistics collectionStatistics(String field) throws IOException {
    long docCount = 0;
    long sumTotalTermFreq = 0;
    long sumDocFreq = 0;
    for (LeafReaderContext leaf : reader.leaves()) {
        final Terms terms = Terms.getTerms(leaf.reader(), field);
        docCount += terms.getDocCount();
        sumTotalTermFreq += terms.getSumTotalTermFreq();
        sumDocFreq += terms.getSumDocFreq();
    }
    return new CollectionStatistics(field, reader.maxDoc(), docCount, sumTotalTermFreq, sumDocFreq);
}

public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq) {
    return new TermStatistics(term.bytes(), docFreq, totalTermFreq);
}
```

#### TermQuery.TermWeight (Weight の具体例)

`lucene/core/src/java/org/apache/lucene/search/TermQuery.java`:
```java
final class TermWeight extends Weight {
    private final Similarity.SimScorer simScorer;

    public TermWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost, TermStates termStates) {
        // コレクション全体の統計を取得
        CollectionStatistics collectionStats = searcher.collectionStatistics(term.field());
        TermStatistics termStats = searcher.termStatistics(term, termStates.docFreq(), termStates.totalTermFreq());
        // 統計から事前にスコアリングファクターを計算 (IDF, avgdl 等)
        this.simScorer = similarity.scorer(boost, collectionStats, termStats);
    }

    public ScorerSupplier scorerSupplier(LeafReaderContext context) {
        // simScorer は事前計算済み。セグメントローカルな postings を使ってスコアリング
    }
}
```

#### BooleanQuery.BooleanWeight (複合 Weight の例)

`lucene/core/src/java/org/apache/lucene/search/BooleanWeight.java`:
```java
BooleanWeight(BooleanQuery query, IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    for (BooleanClause c : query) {
        // 再帰的に子クエリの Weight を生成
        Weight w = searcher.createWeight(c.query(), c.isScoring() ? scoreMode : ScoreMode.COMPLETE_NO_SCORES, boost);
        weightedClauses.add(new WeightedBooleanClause(c, w));
    }
}
```

#### BM25Similarity の統計利用

`lucene/core/src/java/org/apache/lucene/search/similarities/BM25Similarity.java`:
```java
public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
    // IDF: コレクション全体の docCount と docFreq から計算
    float idf = (float) Math.log(1 + (docCount - docFreq + 0.5D) / (docFreq + 0.5D));
    // avgdl: コレクション全体の sumTotalTermFreq / docCount
    float avgdl = (float) (collectionStats.sumTotalTermFreq() / (double) collectionStats.docCount());
    return new BM25Scorer(boost, k1, b, idf, avgdl, cache);
}
```

---

## 実装計画

### Step 1: 統計データ型の定義

コレクションレベルとタームレベルの統計を表す型を追加する。

```go
// search/statistics.go (新規)

// CollectionStatistics はフィールド全体のコレクションレベル統計を保持する
type CollectionStatistics struct {
    Field            string
    MaxDoc           int64  // インデックス内の最大ドキュメント数 (削除済み含む)
    DocCount         int64  // フィールドに値を持つドキュメント数
    SumTotalTermFreq int64  // フィールド内の全ターム出現回数の合計
    SumDocFreq       int64  // フィールド内の全タームの docFreq の合計
}

// TermStatistics はターム固有の統計を保持する
type TermStatistics struct {
    Term          string
    DocFreq       int64  // タームを含むドキュメント数 (全セグメント合計)
    TotalTermFreq int64  // タームの出現回数 (全セグメント合計)
}
```

### Step 2: IndexSearcher に統計メソッドを追加

```go
// search/searcher.go に追加

// CollectionStatistics は全セグメントを横断してフィールドの統計を集計する
func (s *IndexSearcher) CollectionStatistics(field string) *CollectionStatistics {
    var docCount, sumTotalTermFreq, sumDocFreq int64
    for _, leaf := range s.reader.Leaves() {
        seg := leaf.Segment
        docCount += int64(seg.LiveDocCount())
        sumTotalTermFreq += int64(seg.TotalFieldLength(field))
        // sumDocFreq は必要に応じて SegmentReader に追加
    }
    if docCount == 0 {
        return nil
    }
    return &CollectionStatistics{
        Field:            field,
        MaxDoc:           int64(s.reader.MaxDoc()),
        DocCount:         docCount,
        SumTotalTermFreq: sumTotalTermFreq,
        SumDocFreq:       sumDocFreq,
    }
}

// TermStatistics は全セグメントを横断してタームの統計を集計する
func (s *IndexSearcher) TermStatistics(field, term string) *TermStatistics {
    var docFreq, totalTermFreq int64
    for _, leaf := range s.reader.Leaves() {
        seg := leaf.Segment
        docFreq += int64(seg.DocFreq(field, term))
        // totalTermFreq は必要に応じて SegmentReader に追加
    }
    if docFreq == 0 {
        return nil
    }
    return &TermStatistics{
        Term:          term,
        DocFreq:       docFreq,
        TotalTermFreq: totalTermFreq,
    }
}
```

### Step 3: Weight インターフェースの導入

```go
// search/weight.go (新規)

// Weight はコレクションレベルの事前計算を保持し、セグメントごとの Scorer を生成する
type Weight interface {
    // Query は元のクエリを返す
    Query() Query
    // Scorer は指定されたセグメントの Scorer を生成する
    Scorer(ctx index.LeafReaderContext) Scorer
}
```

### Step 4: Query インターフェースの変更

```go
// search/query.go

type Query interface {
    // CreateWeight はコレクションレベルの統計を使って Weight を生成する
    CreateWeight(searcher *IndexSearcher, scoreMode ScoreMode) Weight
}
```

`CreateScorer(ctx, scoreMode)` を `CreateWeight(searcher, scoreMode)` に置き換える。

### Step 5: TermQuery の Weight 実装

```go
// search/term_query.go

type termWeight struct {
    query     *TermQuery
    scoreMode ScoreMode
    // 事前計算されたスコアリングファクター (コレクション全体の統計から算出)
    idf       float64
    avgDocLen float64
    bm25      *BM25Scorer
}

func (q *TermQuery) CreateWeight(searcher *IndexSearcher, scoreMode ScoreMode) Weight {
    w := &termWeight{query: q, scoreMode: scoreMode}
    if scoreMode != ScoreModeNone {
        collStats := searcher.CollectionStatistics(q.Field)
        termStats := searcher.TermStatistics(q.Field, q.Term)
        if collStats != nil && termStats != nil {
            w.bm25 = NewBM25Scorer()
            w.idf = w.bm25.IDF(int(collStats.DocCount), int(termStats.DocFreq))
            w.avgDocLen = float64(collStats.SumTotalTermFreq) / float64(collStats.DocCount)
        }
    }
    return w
}

func (w *termWeight) Query() Query { return w.query }

func (w *termWeight) Scorer(ctx index.LeafReaderContext) Scorer {
    // PostingsIterator はセグメントローカル
    // idf, avgDocLen は Weight で事前計算済み (コレクション全体の統計)
    // ...
}
```

### Step 6: BooleanQuery の Weight 実装

```go
// search/boolean_query.go

type booleanWeight struct {
    query         *BooleanQuery
    clauseWeights []clauseWeight // 各子クエリの Weight を再帰的に保持
}

type clauseWeight struct {
    weight Weight
    occur  Occur
}

func (q *BooleanQuery) CreateWeight(searcher *IndexSearcher, scoreMode ScoreMode) Weight {
    w := &booleanWeight{query: q}
    for _, clause := range q.Clauses {
        childScoreMode := scoreMode
        if clause.Occur == OccurMustNot {
            childScoreMode = ScoreModeNone
        }
        // 再帰的に子クエリの Weight を生成
        cw := clause.Query.CreateWeight(searcher, childScoreMode)
        w.clauseWeights = append(w.clauseWeights, clauseWeight{weight: cw, occur: clause.Occur})
    }
    return w
}
```

### Step 7: IndexSearcher.Search の変更

```go
func (s *IndexSearcher) Search(q Query, c Collector) []SearchResult {
    scoreMode := c.ScoreMode()
    weight := q.CreateWeight(s, scoreMode)  // Weight を生成 (統計の事前計算)

    for _, leaf := range s.reader.Leaves() {
        scorer := weight.Scorer(leaf)       // セグメントごとの Scorer を生成
        if scorer == nil {
            continue
        }
        lc := c.GetLeafCollector(leaf)
        lc.SetScorer(scorer)
        // ... iterate and collect
    }
    return c.Results()
}
```

### 移行時の注意点

- 既存のテストは `Query.CreateScorer` を直接呼んでいるため、`Weight.Scorer` 経由に変更が必要
- シングルセグメントのテストケースでは結果が変わらないが、マルチセグメントのテストケースを新たに追加してコレクション統計の正確性を検証すべき
- `index.SegmentReader` に `SumDocFreq` (タームの docFreq 合計) や `TotalTermFreq` (タームの出現回数合計) のメソッドが不足している場合はインデックス層の拡張が先に必要
