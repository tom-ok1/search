# Step 7: Segment Merge

## 学ぶ概念

Segment が増え続けると検索性能が劣化します（各 Segment を走査する必要があるため）。**Segment Merge** は小さな Segment を統合して大きな Segment にする処理です。

### なぜマージが必要か？

```
ドキュメント追加を繰り返すと...

  [seg0: 10docs] [seg1: 10docs] [seg2: 10docs] [seg3: 10docs] ... [seg99: 10docs]

検索時: 100個の Segment を全部走査 → 遅い！

マージ後:
  [seg_merged: 1000docs]

検索時: 1つの Segment だけ → 速い！
```

### マージで起きること

1. **PostingsList の結合**: 各 Segment の PostingsList を DocID を振り直しながら結合
2. **削除ドキュメントの除外**: 削除マーク付きドキュメントを物理的に除去
3. **新しい Segment の生成**: 結合結果を新しい immutable Segment として書き出し
4. **古い Segment の削除**: マージ元の Segment を削除

### Merge Policy

「いつ」「どの Segment を」マージするかを決める戦略を **Merge Policy** と呼びます。

#### LogMergePolicy

古い Merge Policy。Segment のサイズを対数で比較し、同じ「レベル」の Segment をまとめてマージします。

```
Level 0: [10] [10] [10] [10]  → マージ → Level 1: [40]
Level 1: [40] [40] [40] [40]  → マージ → Level 2: [160]
```

#### TieredMergePolicy（Lucene のデフォルト）

より洗練された戦略。以下の方針で動きます：

1. **大きい Segment はマージしない**（`maxMergedSegmentMB` 以上）
2. **一度にマージする Segment 数を制限**（`maxMergeAtOnce`、デフォルト10）
3. **削除率が高い Segment を優先マージ**（ディスク回収のため）
4. **マージの「コスト対効果」を計算**して最適な組み合わせを選ぶ

### ES の `_forcemerge`

ES の `_forcemerge` API は、明示的にマージを実行する操作です。`max_num_segments=1` を指定すると全 Segment を1つにマージします。これはインデックスが read-only（もう書き込まない）場合に有効です。

---

## Lucene ソースを読む

### 読むべきファイル（`lucene/lucene/core/src/java/org/apache/lucene/index/`）

| ファイル | ポイント |
|----------|----------|
| `MergePolicy.java` | Merge Policy の抽象基底。`findMerges()` がコア API |
| `TieredMergePolicy.java` | デフォルトの Merge Policy。`findMerges()` の実装が興味深い |
| `LogMergePolicy.java` | 旧デフォルト。シンプルで理解しやすい |
| `MergeScheduler.java` | マージの実行タイミングを制御する抽象クラス |
| `ConcurrentMergeScheduler.java` | バックグラウンドスレッドでマージ実行 |
| `SegmentMerger.java` | 実際のマージ処理。Postings の結合、DocID の振り直し |
| `IndexWriter.java` | `maybeMerge()` → `updatePendingMerges()` → `merge()` の流れ |
| `MergeState.java` | マージ中の状態を管理 |

### TieredMergePolicy のアルゴリズム概要

```java
// TieredMergePolicy.findMerges() の概要
1. 全 Segment をサイズ降順にソート
2. maxMergedSegmentMB 以上の Segment をスキップ
3. 残りの Segment から「最もマージする価値のある」組み合わせを探す:
   a. 連続する maxMergeAtOnce 個の Segment を候補にする
   b. skew（サイズの偏り）が小さい組み合わせを優先
   c. 削除率が高い Segment を含む組み合わせを優先
4. 見つかった最良の組み合わせを返す
```

---

## Go で実装する

### 1. MergePolicy インターフェース

```go
// index/merge_policy.go

package index

// MergeCandidate はマージ候補の Segment 群。
type MergeCandidate struct {
    Segments []*Segment
}

// MergePolicy はいつどの Segment をマージするかを決定する。
type MergePolicy interface {
    // FindMerges はマージすべき Segment の組み合わせを返す。
    FindMerges(segments []*Segment) []MergeCandidate
}
```

### 2. SimpleMergePolicy（学習用）

```go
// index/simple_merge_policy.go

package index

// SimpleMergePolicy は最もシンプルな Merge Policy。
// Segment 数が maxSegments を超えたら、小さい順にマージする。
type SimpleMergePolicy struct {
    MaxSegments    int // これ以上 Segment があるとマージ
    MergeAtOnce    int // 一度にマージする Segment 数
}

func NewSimpleMergePolicy(maxSegments, mergeAtOnce int) *SimpleMergePolicy {
    return &SimpleMergePolicy{
        MaxSegments: maxSegments,
        MergeAtOnce: mergeAtOnce,
    }
}

func (p *SimpleMergePolicy) FindMerges(segments []*Segment) []MergeCandidate {
    if len(segments) <= p.MaxSegments {
        return nil
    }

    // 小さい Segment から順にマージ
    // （本来はサイズでソートすべきだが、簡易版では追加順 = サイズ小さい順と仮定）
    var candidates []MergeCandidate
    for i := 0; i+p.MergeAtOnce <= len(segments); i += p.MergeAtOnce {
        candidate := MergeCandidate{
            Segments: segments[i : i+p.MergeAtOnce],
        }
        candidates = append(candidates, candidate)
    }

    return candidates
}
```

### 3. TieredMergePolicy（Lucene 風の簡易版）

```go
// index/tiered_merge_policy.go

package index

import "sort"

// TieredMergePolicy は Lucene の TieredMergePolicy の簡易版。
type TieredMergePolicy struct {
    MaxMergeAtOnce      int     // 一度にマージする最大 Segment 数
    SegmentsPerTier     int     // 各 tier あたりの Segment 数
    MaxMergedSegmentDoc int     // この doc 数以上の Segment はマージしない
    DeletedPctAllowed   float64 // 許容する削除率
}

func NewTieredMergePolicy() *TieredMergePolicy {
    return &TieredMergePolicy{
        MaxMergeAtOnce:      10,
        SegmentsPerTier:     10,
        MaxMergedSegmentDoc: 100000,
        DeletedPctAllowed:   0.33,
    }
}

func (p *TieredMergePolicy) FindMerges(segments []*Segment) []MergeCandidate {
    if len(segments) <= p.SegmentsPerTier {
        return nil
    }

    // サイズ（doc数）でソート
    sorted := make([]*Segment, len(segments))
    copy(sorted, segments)
    sort.Slice(sorted, func(i, j int) bool {
        return sorted[i].docCount < sorted[j].docCount
    })

    // 大きすぎる Segment を除外
    var eligible []*Segment
    for _, seg := range sorted {
        if seg.docCount < p.MaxMergedSegmentDoc {
            eligible = append(eligible, seg)
        }
    }

    if len(eligible) <= p.SegmentsPerTier {
        return nil
    }

    // 最もマージする価値のある組み合わせを探す
    bestScore := float64(-1)
    var bestCandidate *MergeCandidate

    mergeSize := p.MaxMergeAtOnce
    if mergeSize > len(eligible) {
        mergeSize = len(eligible)
    }

    for i := 0; i+mergeSize <= len(eligible); i++ {
        candidate := eligible[i : i+mergeSize]
        score := p.scoreMerge(candidate)
        if score > bestScore {
            bestScore = score
            segs := make([]*Segment, mergeSize)
            copy(segs, candidate)
            bestCandidate = &MergeCandidate{Segments: segs}
        }
    }

    if bestCandidate != nil {
        return []MergeCandidate{*bestCandidate}
    }
    return nil
}

// scoreMerge はマージ候補のスコアを計算する。
// スコアが高いほどマージする価値がある。
func (p *TieredMergePolicy) scoreMerge(segments []*Segment) float64 {
    totalDocs := 0
    totalDeleted := 0
    for _, seg := range segments {
        totalDocs += seg.docCount
        totalDeleted += len(seg.deletedDocs)
    }

    // 削除率が高いほどスコアが高い（マージする価値がある）
    deletedRatio := 0.0
    if totalDocs > 0 {
        deletedRatio = float64(totalDeleted) / float64(totalDocs)
    }

    // サイズの偏り（skew）が小さいほどスコアが高い
    maxSize := 0
    for _, seg := range segments {
        if seg.docCount > maxSize {
            maxSize = seg.docCount
        }
    }
    avgSize := float64(totalDocs) / float64(len(segments))
    skew := 1.0
    if avgSize > 0 {
        skew = float64(maxSize) / avgSize // 1.0 が理想、大きいほど偏りが大きい
    }

    return deletedRatio + (1.0 / skew)
}
```

### 4. Segment マージの実装

```go
// index/merger.go

package index

import "fmt"

// MergeSegments は複数の Segment を1つにマージする。
func MergeSegments(segments []*Segment, newName string) *Segment {
    merged := newSegment(newName)

    for _, seg := range segments {
        docBase := merged.docCount

        // 各ドキュメントを処理
        for localDocID := 0; localDocID < seg.docCount; localDocID++ {
            // 削除済みドキュメントはスキップ
            if seg.IsDeleted(localDocID) {
                continue
            }

            newDocID := merged.docCount
            merged.docCount++

            // Stored fields のコピー
            if stored, ok := seg.storedFields[localDocID]; ok {
                merged.storedFields[newDocID] = make(map[string]string)
                for k, v := range stored {
                    merged.storedFields[newDocID][k] = v
                }
            }

            // フィールド長のコピー
            for fieldName, lengths := range seg.fieldLengths {
                if localDocID < len(lengths) {
                    if merged.fieldLengths[fieldName] == nil {
                        merged.fieldLengths[fieldName] = make([]int, 0)
                    }
                    for len(merged.fieldLengths[fieldName]) <= newDocID {
                        merged.fieldLengths[fieldName] = append(merged.fieldLengths[fieldName], 0)
                    }
                    merged.fieldLengths[fieldName][newDocID] = lengths[localDocID]
                }
            }
        }

        // Postings の再構築
        // DocID のマッピング: oldLocalDocID → newDocID
        docIDMap := buildDocIDMap(seg, docBase)

        for fieldName, fi := range seg.fields {
            mergedFI := merged.fields[fieldName]
            if mergedFI == nil {
                mergedFI = newFieldIndex()
                merged.fields[fieldName] = mergedFI
            }

            for term, pl := range fi.postings {
                mergedPL := mergedFI.postings[term]
                if mergedPL == nil {
                    mergedPL = &PostingsList{Term: term}
                    mergedFI.postings[term] = mergedPL
                }

                for _, posting := range pl.Postings {
                    if seg.IsDeleted(posting.DocID) {
                        continue
                    }

                    newDocID, ok := docIDMap[posting.DocID]
                    if !ok {
                        continue
                    }

                    mergedPL.Postings = append(mergedPL.Postings, Posting{
                        DocID:     newDocID,
                        Freq:      posting.Freq,
                        Positions: posting.Positions,
                    })
                }
            }
        }
    }

    return merged
}

// buildDocIDMap は元の localDocID から新しい DocID へのマッピングを作る。
// 削除済みドキュメントはマッピングに含まれない。
func buildDocIDMap(seg *Segment, docBase int) map[int]int {
    docIDMap := make(map[int]int)
    newDocID := docBase
    for localDocID := 0; localDocID < seg.docCount; localDocID++ {
        if !seg.IsDeleted(localDocID) {
            docIDMap[localDocID] = newDocID
            newDocID++
        }
    }
    return docIDMap
}
```

### 5. IndexWriter にマージ機能を追加

```go
// index/writer.go に追加

// MaybeMerge は Merge Policy に基づいてマージを実行する。
func (w *IndexWriter) MaybeMerge(policy MergePolicy) {
    candidates := policy.FindMerges(w.segments)
    for _, candidate := range candidates {
        w.executeMerge(candidate)
    }
}

// ForceMerge は全 Segment を1つにマージする。
func (w *IndexWriter) ForceMerge() {
    if len(w.segments) <= 1 {
        return
    }
    w.Flush() // バッファを先に flush

    merged := MergeSegments(w.segments, w.nextSegmentName())
    w.segments = []*Segment{merged}
}

func (w *IndexWriter) executeMerge(candidate MergeCandidate) {
    merged := MergeSegments(candidate.Segments, w.nextSegmentName())

    // マージ元 Segment を除去し、マージ後 Segment を追加
    mergeSet := make(map[*Segment]bool)
    for _, seg := range candidate.Segments {
        mergeSet[seg] = true
    }

    var newSegments []*Segment
    for _, seg := range w.segments {
        if !mergeSet[seg] {
            newSegments = append(newSegments, seg)
        }
    }
    newSegments = append(newSegments, merged)
    w.segments = newSegments
}
```

---

## 確認・テスト

```go
// index/merge_test.go

package index

import (
    "testing"

    "gosearch/analysis"
    "gosearch/document"
)

func TestMergeSegments(t *testing.T) {
    analyzer := analysis.NewAnalyzer(
        analysis.NewWhitespaceTokenizer(),
        &analysis.LowerCaseFilter{},
    )
    writer := NewIndexWriter(analyzer, 2)

    // 4つのドキュメントを追加（2つずつ flush → 2 segment）
    texts := []string{"hello world", "hello go", "world go", "hello world go"}
    for _, text := range texts {
        doc := document.NewDocument()
        doc.AddField("body", text, document.FieldTypeText)
        writer.AddDocument(doc)
    }
    writer.Flush()

    if len(writer.Segments()) != 2 {
        t.Fatalf("expected 2 segments before merge, got %d", len(writer.Segments()))
    }

    // マージ
    writer.ForceMerge()

    if len(writer.Segments()) != 1 {
        t.Fatalf("expected 1 segment after merge, got %d", len(writer.Segments()))
    }

    merged := writer.Segments()[0]
    if merged.docCount != 4 {
        t.Errorf("expected 4 docs in merged segment, got %d", merged.docCount)
    }

    // "hello" の postings が正しいか
    pl := merged.fields["body"].postings["hello"]
    if pl == nil || len(pl.Postings) != 3 {
        t.Errorf("expected 3 postings for 'hello', got %v", pl)
    }
}

func TestMergeWithDeletedDocs(t *testing.T) {
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

    // id=1 を削除
    writer.DeleteDocuments("id", "1")

    // マージで物理削除
    writer.ForceMerge()

    merged := writer.Segments()[0]
    if merged.docCount != 1 {
        t.Errorf("expected 1 doc after merge (deleted removed), got %d", merged.docCount)
    }
    if len(merged.deletedDocs) != 0 {
        t.Error("expected no deleted docs after merge")
    }
}
```

---

## 深掘り：なぜこう設計されているのか

### Q: なぜマージはバックグラウンドで行うのか？

マージは I/O 集約的な処理で時間がかかります。メインの書き込み・検索を止めないように `ConcurrentMergeScheduler` がバックグラウンドスレッドでマージを実行します。

### Q: TieredMergePolicy の「Tiered」とは？

Segment をサイズで「層（tier）」に分類し、同じ層の Segment をマージするイメージです。小さい Segment は頻繁にマージされ、大きい Segment は滅多にマージされません。これにより、書き込みコストが対数的に増加する（全ドキュメントを毎回再書き込みしない）ことが保証されます。

### Q: ES の `max_num_segments=1` の ForceMerge はなぜ要注意なのか？

ForceMerge は全 Segment を1つにまとめるため、大量の I/O とディスク使用量（一時的に倍必要）が発生します。書き込みが継続中のインデックスで実行すると、巨大な Segment ができた直後に小さな新 Segment が生まれ、再びマージが必要になる「いたちごっこ」が起きます。

---

## 次のステップ

マージが実装できたので、次は [Step 8: Doc Values & 集計](08-docvalues.md) で、列指向ストレージによるソートと集計を学びます。
