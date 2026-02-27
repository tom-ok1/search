# Step 4: Boolean & Phrase Query

## 学ぶ概念

実際の検索では単一 term だけでなく、複数の条件を組み合わせます。

### Boolean Query

ES でよく使う `bool` クエリの基礎です：

```json
{
  "bool": {
    "must":     [{"match": {"title": "fox"}}],
    "should":   [{"match": {"title": "quick"}}],
    "must_not": [{"match": {"title": "lazy"}}]
  }
}
```

Lucene では以下の3つの **Occur** で条件を組み合わせます：

| Occur | ES 相当 | 意味 |
|-------|---------|------|
| MUST | `must` | 必ずマッチしなければならない（AND） |
| SHOULD | `should` | マッチするとスコアが上がる（OR） |
| MUST_NOT | `must_not` | マッチしてはいけない（NOT） |

### アルゴリズム：PostingsList のマージ

Boolean Query の実装は、本質的には **ソート済みリストのマージ操作** です。

```
AND (Conjunction):
  "fox"   → [0, 2, 5, 8]
  "brown" → [0, 1, 5, 7]
  結果     → [0, 5]         // 共通する DocID

OR (Disjunction):
  "fox"   → [0, 2, 5, 8]
  "brown" → [0, 1, 5, 7]
  結果     → [0, 1, 2, 5, 7, 8]  // いずれかに含まれる DocID
```

DocID が昇順にソートされているため、2つのポインタを進めるだけで O(n+m) で処理できます。

### Phrase Query

`"quick fox"` のようなフレーズ検索は、term のマッチだけでなく **position が連続しているか** をチェックします。

```
"quick" → Doc0: positions=[1]
"fox"   → Doc0: positions=[3]

"quick fox" → position の差が 1 であるペアがあるか？
  1 と 3 → 差は 2 → マッチしない

"brown fox" の場合:
"brown" → Doc0: positions=[2]
"fox"   → Doc0: positions=[3]
  2 と 3 → 差は 1 → マッチ！
```

---

## Lucene ソースを読む

### 読むべきファイル（`lucene/lucene/core/src/java/org/apache/lucene/search/`）

| ファイル | ポイント |
|----------|----------|
| `BooleanQuery.java` | Boolean クエリの定義。`BooleanClause` のリストを持つ |
| `BooleanWeight.java` | Boolean クエリのスコア計算準備 |
| `ConjunctionScorer.java` | AND（MUST）の実装。複数の Scorer の共通 DocID を見つける |
| `DisjunctionScorer.java` | OR（SHOULD）の実装。いずれかの Scorer にマッチする DocID |
| `ReqExclScorer.java` | MUST + MUST_NOT の組み合わせ |
| `PhraseQuery.java` | フレーズ検索の定義 |
| `ExactPhraseMatcher.java` | フレーズの位置マッチング実装 |
| `Query.java` | 全クエリの基底クラス |
| `Weight.java` | クエリの「重み」計算。Scorer を生成する |
| `Scorer.java` | DocID を走査しながらスコアを計算する |
| `DocIdSetIterator.java` | DocID の走査の基底。`nextDoc()`, `advance()` |

### Lucene の Conjunction（AND）アルゴリズム

```java
// ConjunctionScorer の本質（簡略化）
while (true) {
    // 全 scorer の現在の DocID の最大値を求める
    int target = maxDocID(scorers);

    // 全 scorer を target まで advance
    boolean allMatch = true;
    for (Scorer s : scorers) {
        if (s.advance(target) != target) {
            allMatch = false;
            break;
        }
    }

    if (allMatch) {
        // 全 scorer が同じ DocID → マッチ！
        return target;
    }
}
```

---

## Go で実装する

### 1. Query インターフェース

```go
// search/query.go

package search

import "gosearch/index"

// Query は検索クエリを表す。
type Query interface {
    // Execute はクエリを実行し、マッチしたドキュメントとスコアのペアを返す。
    Execute(idx *index.InMemoryIndex) []DocScore
}

// DocScore はドキュメントID とスコアのペア。
type DocScore struct {
    DocID int
    Score float64
}
```

### 2. TermQuery

```go
// search/term_query.go

package search

import "gosearch/index"

// TermQuery は単一 term の検索。
type TermQuery struct {
    Field string
    Term  string
}

func NewTermQuery(field, term string) *TermQuery {
    return &TermQuery{Field: field, Term: term}
}

func (q *TermQuery) Execute(idx *index.InMemoryIndex) []DocScore {
    pl := idx.GetPostings(q.Field, q.Term)
    if pl == nil {
        return nil
    }

    scorer := NewBM25Scorer()
    docCount := idx.DocCount()
    docFreq := len(pl.Postings)
    idf := scorer.IDF(docCount, docFreq)
    avgDocLen := idx.AvgFieldLength(q.Field)

    var results []DocScore
    for _, posting := range pl.Postings {
        docLen := float64(idx.FieldLength(q.Field, posting.DocID))
        score := scorer.Score(float64(posting.Freq), docLen, avgDocLen, idf)
        results = append(results, DocScore{DocID: posting.DocID, Score: score})
    }
    return results
}
```

### 3. BooleanQuery

```go
// search/boolean_query.go

package search

import (
    "gosearch/index"
    "sort"
)

// Occur はクエリ条件の種類。
type Occur int

const (
    OccurMust    Occur = iota // AND
    OccurShould               // OR
    OccurMustNot              // NOT
)

// BooleanClause は Boolean クエリの1つの条件。
type BooleanClause struct {
    Query Query
    Occur Occur
}

// BooleanQuery は複数の条件を組み合わせるクエリ。
type BooleanQuery struct {
    Clauses []BooleanClause
}

func NewBooleanQuery() *BooleanQuery {
    return &BooleanQuery{}
}

func (q *BooleanQuery) Add(query Query, occur Occur) *BooleanQuery {
    q.Clauses = append(q.Clauses, BooleanClause{Query: query, Occur: occur})
    return q
}

func (q *BooleanQuery) Execute(idx *index.InMemoryIndex) []DocScore {
    // 各 clause の結果を取得
    var mustResults [][]DocScore
    var shouldResults [][]DocScore
    var mustNotResults [][]DocScore

    for _, clause := range q.Clauses {
        results := clause.Query.Execute(idx)
        switch clause.Occur {
        case OccurMust:
            mustResults = append(mustResults, results)
        case OccurShould:
            shouldResults = append(shouldResults, results)
        case OccurMustNot:
            mustNotResults = append(mustNotResults, results)
        }
    }

    // MUST の intersection を計算
    candidates := intersectAll(mustResults)

    // MUST が無い場合は SHOULD の union が候補
    if len(mustResults) == 0 && len(shouldResults) > 0 {
        candidates = unionAll(shouldResults)
    } else if len(shouldResults) > 0 {
        // MUST がある場合、SHOULD はスコアの加算のみ
        candidates = addShouldScores(candidates, shouldResults)
    }

    // MUST_NOT の除外
    if len(mustNotResults) > 0 {
        excludeSet := make(map[int]bool)
        for _, results := range mustNotResults {
            for _, ds := range results {
                excludeSet[ds.DocID] = true
            }
        }
        var filtered []DocScore
        for _, ds := range candidates {
            if !excludeSet[ds.DocID] {
                filtered = append(filtered, ds)
            }
        }
        candidates = filtered
    }

    return candidates
}

// intersectAll は複数の DocScore リストの共通 DocID を見つけ、スコアを合算する。
func intersectAll(lists [][]DocScore) []DocScore {
    if len(lists) == 0 {
        return nil
    }
    if len(lists) == 1 {
        return lists[0]
    }

    result := lists[0]
    for i := 1; i < len(lists); i++ {
        result = intersectTwo(result, lists[i])
    }
    return result
}

// intersectTwo は2つの DocScore リストの共通 DocID を見つける。
// 両方のリストは DocID 昇順であること。
func intersectTwo(a, b []DocScore) []DocScore {
    var result []DocScore
    i, j := 0, 0
    for i < len(a) && j < len(b) {
        if a[i].DocID == b[j].DocID {
            result = append(result, DocScore{
                DocID: a[i].DocID,
                Score: a[i].Score + b[j].Score,
            })
            i++
            j++
        } else if a[i].DocID < b[j].DocID {
            i++
        } else {
            j++
        }
    }
    return result
}

// unionAll は複数の DocScore リストの和集合を返す。
func unionAll(lists [][]DocScore) []DocScore {
    scoreMap := make(map[int]float64)
    for _, list := range lists {
        for _, ds := range list {
            scoreMap[ds.DocID] += ds.Score
        }
    }

    var result []DocScore
    for docID, score := range scoreMap {
        result = append(result, DocScore{DocID: docID, Score: score})
    }
    sort.Slice(result, func(i, j int) bool {
        return result[i].DocID < result[j].DocID
    })
    return result
}

// addShouldScores は MUST 結果に SHOULD のスコアを加算する。
func addShouldScores(must []DocScore, shouldLists [][]DocScore) []DocScore {
    shouldScores := make(map[int]float64)
    for _, list := range shouldLists {
        for _, ds := range list {
            shouldScores[ds.DocID] += ds.Score
        }
    }

    for i := range must {
        if bonus, exists := shouldScores[must[i].DocID]; exists {
            must[i].Score += bonus
        }
    }
    return must
}
```

### 4. PhraseQuery

```go
// search/phrase_query.go

package search

import "gosearch/index"

// PhraseQuery はフレーズ検索。term が指定された順序で連続して出現するドキュメントを検索する。
type PhraseQuery struct {
    Field string
    Terms []string // フレーズを構成する term のリスト
}

func NewPhraseQuery(field string, terms ...string) *PhraseQuery {
    return &PhraseQuery{Field: field, Terms: terms}
}

func (q *PhraseQuery) Execute(idx *index.InMemoryIndex) []DocScore {
    if len(q.Terms) == 0 {
        return nil
    }

    // 各 term の PostingsList を取得
    var postingsLists []*index.PostingsList
    for _, term := range q.Terms {
        pl := idx.GetPostings(q.Field, term)
        if pl == nil {
            return nil // いずれかの term が存在しなければマッチなし
        }
        postingsLists = append(postingsLists, pl)
    }

    // 全 term が存在する DocID を見つける
    commonDocs := findCommonDocs(postingsLists)

    scorer := NewBM25Scorer()
    docCount := idx.DocCount()
    avgDocLen := idx.AvgFieldLength(q.Field)

    var results []DocScore
    for _, docID := range commonDocs {
        // position が連続しているかチェック
        if q.matchPositions(postingsLists, docID) {
            // フレーズ全体のスコア（簡易版：各 term の IDF の合計）
            totalScore := 0.0
            docLen := float64(idx.FieldLength(q.Field, docID))
            for i, pl := range postingsLists {
                posting := findPosting(pl, docID)
                if posting != nil {
                    idf := scorer.IDF(docCount, len(postingsLists[i].Postings))
                    totalScore += scorer.Score(float64(posting.Freq), docLen, avgDocLen, idf)
                }
            }
            results = append(results, DocScore{DocID: docID, Score: totalScore})
        }
    }

    return results
}

// matchPositions は指定 DocID で term の position が連続しているかチェックする。
func (q *PhraseQuery) matchPositions(postingsLists []*index.PostingsList, docID int) bool {
    // 各 term の positions を取得
    var positionSets [][]int
    for _, pl := range postingsLists {
        posting := findPosting(pl, docID)
        if posting == nil {
            return false
        }
        positionSets = append(positionSets, posting.Positions)
    }

    // 最初の term の各 position を起点に、連続するかチェック
    for _, startPos := range positionSets[0] {
        matched := true
        for i := 1; i < len(positionSets); i++ {
            expectedPos := startPos + i
            if !containsInt(positionSets[i], expectedPos) {
                matched = false
                break
            }
        }
        if matched {
            return true
        }
    }
    return false
}

// findCommonDocs は全 PostingsList に共通する DocID を返す。
func findCommonDocs(lists []*index.PostingsList) []int {
    if len(lists) == 0 {
        return nil
    }

    // 最初のリストの DocID をセットにする
    docSet := make(map[int]bool)
    for _, p := range lists[0].Postings {
        docSet[p.DocID] = true
    }

    // 残りのリストとの共通部分を取る
    for _, pl := range lists[1:] {
        newSet := make(map[int]bool)
        for _, p := range pl.Postings {
            if docSet[p.DocID] {
                newSet[p.DocID] = true
            }
        }
        docSet = newSet
    }

    var result []int
    for docID := range docSet {
        result = append(result, docID)
    }
    return result
}

func findPosting(pl *index.PostingsList, docID int) *index.Posting {
    for i := range pl.Postings {
        if pl.Postings[i].DocID == docID {
            return &pl.Postings[i]
        }
    }
    return nil
}

func containsInt(slice []int, val int) bool {
    for _, v := range slice {
        if v == val {
            return true
        }
    }
    return false
}
```

---

## 確認・テスト

```go
// search/query_test.go

package search

import (
    "testing"

    "gosearch/analysis"
    "gosearch/document"
    "gosearch/index"
)

func setupTestIndex() *index.InMemoryIndex {
    analyzer := analysis.NewAnalyzer(
        analysis.NewWhitespaceTokenizer(),
        &analysis.LowerCaseFilter{},
    )
    idx := index.NewInMemoryIndex(analyzer)

    docs := []string{
        "the quick brown fox",       // doc0
        "the lazy brown dog",        // doc1
        "the quick red fox jumps",   // doc2
        "brown fox brown fox",       // doc3
    }

    for _, text := range docs {
        doc := document.NewDocument()
        doc.AddField("body", text, document.FieldTypeText)
        idx.AddDocument(doc)
    }
    return idx
}

func TestBooleanMust(t *testing.T) {
    idx := setupTestIndex()

    // "brown" AND "fox"
    q := NewBooleanQuery().
        Add(NewTermQuery("body", "brown"), OccurMust).
        Add(NewTermQuery("body", "fox"), OccurMust)

    results := q.Execute(idx)

    docIDs := extractDocIDs(results)
    // doc0, doc3 にのみ brown と fox の両方がある
    if !containsDocID(docIDs, 0) || !containsDocID(docIDs, 3) {
        t.Errorf("expected doc0 and doc3, got %v", docIDs)
    }
    if containsDocID(docIDs, 1) {
        t.Error("doc1 should not match (no 'fox')")
    }
}

func TestBooleanMustNot(t *testing.T) {
    idx := setupTestIndex()

    // "brown" AND NOT "fox"
    q := NewBooleanQuery().
        Add(NewTermQuery("body", "brown"), OccurMust).
        Add(NewTermQuery("body", "fox"), OccurMustNot)

    results := q.Execute(idx)

    docIDs := extractDocIDs(results)
    // doc1 のみ（brown はあるが fox はない）
    if len(docIDs) != 1 || docIDs[0] != 1 {
        t.Errorf("expected [1], got %v", docIDs)
    }
}

func TestPhraseQuery(t *testing.T) {
    idx := setupTestIndex()

    // "brown fox" というフレーズ
    q := NewPhraseQuery("body", "brown", "fox")
    results := q.Execute(idx)

    docIDs := extractDocIDs(results)
    // doc0: "... brown fox" (position 2,3) → マッチ
    // doc3: "brown fox brown fox" (position 0,1 and 2,3) → マッチ
    if !containsDocID(docIDs, 0) {
        t.Error("doc0 should match 'brown fox'")
    }
    if !containsDocID(docIDs, 3) {
        t.Error("doc3 should match 'brown fox'")
    }
    // doc1: "... brown dog" → マッチしない
    if containsDocID(docIDs, 1) {
        t.Error("doc1 should not match 'brown fox'")
    }
}

func extractDocIDs(results []DocScore) []int {
    var ids []int
    for _, r := range results {
        ids = append(ids, r.DocID)
    }
    return ids
}

func containsDocID(ids []int, target int) bool {
    for _, id := range ids {
        if id == target {
            return true
        }
    }
    return false
}
```

---

## 深掘り：なぜこう設計されているのか

### Q: なぜ Lucene は DocIdSetIterator を使うのか？

我々の実装では PostingsList をメモリ上の slice として扱い、全件見ていますが、Lucene の `DocIdSetIterator` は `advance(target)` メソッドを持ちます。これにより、**SkipList を使って不要な DocID を飛ばす** ことができます。

```
AND検索: "fox" AND "brown"
"fox"   → [0, 100, 200, 300, 500, 800]
"brown" → [0, 50, 200, 400, 800]

advance を使うと:
  fox.next()    → 0,   brown.advance(0)   → 0    → MATCH
  fox.next()    → 100, brown.advance(100) → 200
  fox.advance(200)→ 200                          → MATCH
  fox.next()    → 300, brown.advance(300) → 400
  fox.advance(400)→ 500, brown.advance(500)→ 800
  fox.advance(800)→ 800                          → MATCH
```

6+5=11回の走査が8回で済み、データが大きいほど効果が大きくなります。

### Q: Lucene の WAND アルゴリズムとは？

OR 検索で上位K件だけ欲しい場合、全ドキュメントのスコアを計算する必要はありません。**WAND (Weak AND)** アルゴリズムは、各 term の最大スコア貢献度を事前計算しておき、「このドキュメントがどう頑張っても現在のK番目のスコアを超えられない」場合にスキップします。

これは Step 10 の最適化で関連する話題です。

### Q: ES の `minimum_should_match` はどう実装されるのか？

`minimum_should_match` は「SHOULD clause のうち少なくともN個はマッチしなければならない」という制約です。実装としては、各ドキュメントについて SHOULD にマッチした clause 数をカウントし、閾値未満のものを除外します。

---

## 次のステップ

クエリが実装できたので、次は [Step 5: Segment アーキテクチャ](05-segments.md) で、Lucene の核心である immutable segment ベースの設計を学びます。
