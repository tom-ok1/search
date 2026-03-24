# Sorting Optimization Gap Analysis: Lucene vs gosearch

gosearchのフィールドソート（特にnumeric doc valuesによるソート）のパフォーマンス改善に向けて、
Luceneの実装と比較した差分を整理する。

---

## 目次

1. [DocValues の圧縮](#1-docvalues-の圧縮)
2. [DocValues Skip Index](#2-docvalues-skip-index)
3. [Competitive Iterator（BKDツリー）](#3-competitive-iteratorbkdツリー)
4. [Competitive Iterator（DocValuesSkipper）](#4-competitive-iteratordocvaluesskipper)
5. [Index-time Sorting（セグメント事前ソート）](#5-index-time-sortingセグメント事前ソート)
6. [方針比較と優先度](#6-方針比較と優先度)

---

## 1. DocValues の圧縮

### 課題

doc valuesのランダムアクセス時、固定幅int64では1値あたり8バイトを消費する。
同じOSページ（4KB）に載る値は512個しかなく、飛び飛びのdocIDアクセスではページキャッシュミスが起きやすい。

### Lucene の実装

**ファイル**: `Lucene90DocValuesConsumer.java`, `Lucene90DocValuesProducer.java`

Luceneは書き込み時に値の分布を分析し、複数の圧縮戦略を適用する:

| 戦略 | 条件 | 格納内容 |
|---|---|---|
| Const | 全値が同一 | min値のみ（bitsPerValue=0） |
| Table | ユニーク値 < 256 | ルックアップテーブル + ordinal |
| GCD | 公約数 > 1 | `gcd`, `min`, 商の配列 |
| Delta + BitPacking | 通常 | `(value - min) / gcd` をbit-pack |
| Multi-Block Variable BPV | ブロック毎にBPVが異なると10%+圧縮可能 | ブロック毎に独立したbitsPerValue |

**ブロック単位の読み取り** (`VaryingBPVReader`):

```java
// Lucene90DocValuesProducer.java:1828-1836
long getLongValue(long index) throws IOException {
    final long block = index >>> shift;  // ~16K値ごとのブロック
    if (this.block != block) {
        // ブロックヘッダ（bitsPerValue + delta）を読み込み
        // DirectReaderを再構築
    }
    return mul * values.get(index & mask) + delta;
}
```

同じブロック内のアクセスはヘッダの再読み込みが不要で、データも連続領域に収まる。

### gosearch の現状

**ファイル**: `index/doc_values.go`

```go
// 固定幅int64の配列。圧縮なし。
// Format: [values: docCount * int64][docCount: uint32]
func (dv *diskNumericDocValues) Get(docID int) (int64, error) {
    return dv.data.ReadInt64At(docID * 8)
}
```

### 差分

| 観点 | Lucene | gosearch |
|---|---|---|
| 格納サイズ | 値の範囲に応じて1~64 bits/value | 常に64 bits/value |
| ブロック構造 | ~16K値ごとにヘッダ+データ | なし（フラット配列） |
| ページあたりの値数 | 数千~数万（圧縮率依存） | 512 |
| ブロックキャッシュ | 同一ブロック内はメタデータ再利用 | なし |

### 実装方針

1. 書き込み時: 値の `min`, `max`, `gcd` を計算し、`bitsPerValue = bitsRequired((max - min) / gcd)` で必要ビット数を決定
2. ブロック単位（例: 1024値）でbit-packして書き込み。各ブロックにヘッダ（bitsPerValue, min, gcd）を付与
3. 読み取り時: docIDからブロック番号を計算し、ブロックヘッダをキャッシュしつつデコード

**影響範囲**:
- `index/doc_values.go`: `writeNumericDocValues` / `diskNumericDocValues`
- `index/merger.go`: マージ時のdoc values書き込み
- `store/` パッケージ: bit-pack読み書きユーティリティの追加

**効果**: ストレージ削減 + ページあたりの値密度向上によるキャッシュヒット率改善。ただしソート性能への直接的なインパクトは限定的（ランダムアクセスパターン自体は変わらないため）。

---

## 2. DocValues Skip Index

### 課題

フィールドソートで `CompareBottom(docID)` を呼ぶ際、bottomに勝てないドキュメントにも毎回doc valuesの読み取りが発生する。
ドキュメントのブロック単位で min/max が分かれば、ブロック丸ごとスキップできる。

### Lucene の実装

**ファイル**: `DocValuesSkipper.java`, `SkipBlockRangeIterator.java`

Luceneは doc values に階層的なスキップインデックスを付与する:

```
DocValuesSkipper:
  advance(target)           — targetを含むブロックに移動
  advance(minValue, maxValue) — 値の範囲と重ならないブロックをスキップ
  minDocID(level) / maxDocID(level) — レベルごとのdocID範囲
  minValue(level) / maxValue(level) — レベルごとの値の範囲
  numLevels()               — 階層数
```

階層構造により、大きなブロック単位で「この範囲には競合するdocがない」と判定し、一気にスキップできる:

```java
// DocValuesSkipper.java:106-123
public final void advance(long minValue, long maxValue) throws IOException {
    while (minDocID(0) != NO_MORE_DOCS
        && (minValue(0) > maxValue || maxValue(0) < minValue)) {
        int maxDocID = maxDocID(0);
        int nextLevel = 1;
        // 上位レベルも範囲外なら、より大きな単位でスキップ
        while (nextLevel < numLevels()
            && (minValue(nextLevel) > maxValue || maxValue(nextLevel) < minValue)) {
            maxDocID = maxDocID(nextLevel);
            nextLevel++;
        }
        advance(maxDocID + 1);
    }
}
```

`SkipBlockRangeIterator` がこれを `DocIdSetIterator` としてラップし、コレクションループに統合される:

```java
// SkipBlockRangeIterator.java:55-69
public int advance(int target) throws IOException {
    if (target <= skipper.maxDocID(0)) {
        if (doc > -1) return doc = target; // 現在のブロック内
    } else {
        skipper.advance(target);
    }
    skipper.advance(minValue, maxValue); // 範囲外ブロックをスキップ
    return doc = Math.max(target, skipper.minDocID(0));
}
```

### gosearch の現状

doc valuesにスキップインデックスは存在しない。
`numericLeafComparator.CompareBottom` は毎回 `dvs.Get(docID)` でmmapアクセスする。

### 差分

| 観点 | Lucene | gosearch |
|---|---|---|
| ブロックメタデータ | 階層的 min/max per block | なし |
| ブロックスキップ | `advance(minValue, maxValue)` で一括 | 不可 |
| コレクションへの統合 | `DocIdSetIterator` としてAND | なし |

### 実装方針

1. **書き込み時**: doc values書き込みと同時に、N件（例: 128 or 1024）ごとのブロックで `min`, `max`, `docCount` を記録。上位レベル（例: 16ブロック単位）も同様に記録
2. **ファイルフォーマット**: `{seg}.{field}.ndvs` (numeric doc values skip) として別ファイルに格納
   ```
   Level 0: [minDocID, maxDocID, docCount, minValue, maxValue] × blockCount
   Level 1: [minDocID, maxDocID, docCount, minValue, maxValue] × (blockCount / jumpLength)
   ...
   [levelCount: uint8]
   ```
3. **読み取り時**: `DocValuesSkipper` インターフェースを実装し、`advance(minValue, maxValue)` で階層的にブロックスキップ
4. **コレクションへの統合**: `TopFieldCollector` の `compareWithBottom` の前段で、ブロック単位のスキップを適用

**影響範囲**:
- `index/doc_values.go`: スキップインデックスの書き込みと読み取り
- `index/merger.go`: マージ時のスキップインデックス再構築
- `search/numeric_field_comparator.go`: `CompareBottom` 前のブロックスキップ統合
- `search/top_field_collector.go`: コレクタへのイテレータ統合

**効果**: bottomに勝てないブロックのdoc values読み取りを完全にスキップ。ヒット数が多くcompetitive rangeが狭い場合に大きな効果。

---

## 3. Competitive Iterator（BKDツリー）

### 課題

PointValues（BKDツリー）を使えば、値の範囲に基づいてdocIDの集合を効率的に取得できる。
ソート時にbottomが更新されるたびに competitive range を絞り込み、範囲外のdocをコレクション自体から除外できる。

### Lucene の実装

**ファイル**: `NumericComparator.java` 内 `PointsCompetitiveDISIBuilder`

```java
// NumericComparator.java:337-498
private class PointsCompetitiveDISIBuilder extends CompetitiveDISIBuilder {
    @Override
    protected void doUpdateCompetitiveIterator() throws IOException {
        DocIdSetBuilder result = new DocIdSetBuilder(maxDoc);
        PointValues.IntersectVisitor visitor = new PointValues.IntersectVisitor() {
            // BKDツリーのノード単位でmin/maxを比較
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                long min = sortableBytesToLong(minPackedValue);
                long max = sortableBytesToLong(maxPackedValue);
                if (min > maxValueAsLong || max < minValueAsLong) {
                    return CELL_OUTSIDE_QUERY; // ノード丸ごとスキップ
                }
                // ...
            }
        };

        // コスト判定: 8分の1以下に絞れないなら再構築しない
        final long threshold = iteratorCost >>> 3;
        if (isEstimatedPointCountGreaterThanOrEqualTo(visitor, getPointTree(), threshold)) {
            return; // 効果が薄いのでスキップ
        }
        pointValues.intersect(visitor);
        updateCompetitiveIterator(result.build().iterator());
    }
}
```

構築されたiteratorは `ConjunctionBulkScorer` でqueryのiteratorとANDされる:

```java
// ConjunctionBulkScorer.java:95-98
DocIdSetIterator collectorIterator = collector.competitiveIterator();
if (collectorIterator != null) {
    otherIterators.add(collectorIterator);  // queryとAND
}
```

適応的サンプリングにより、更新頻度も制御される:

```java
// 256回以降は MIN_SKIP_INTERVAL(32) ~ MAX_SKIP_INTERVAL(8192) で適応的に調整
if (updateCounter > 256
    && (updateCounter & (currentSkipInterval - 1)) != currentSkipInterval - 1) {
    return;
}
```

### gosearch の現状

BKDツリー/PointValues は未実装。numeric値の範囲検索を加速する索引構造がない。

### 差分

| 観点 | Lucene | gosearch |
|---|---|---|
| PointValues/BKDツリー | 実装済み | 未実装 |
| 値の範囲 → docID集合 | BKDツリーで効率的に取得 | 不可 |
| Competitive iterator | BKDツリーから動的構築 | なし |
| コスト判定 | iteratorCost/8 で閾値判定 | — |
| 適応的サンプリング | 更新間隔を動的調整 | — |

### 実装方針

BKDツリーはそれ自体が大きな機能であり、ソート最適化だけのために導入するのは過剰。
range queryの高速化も含めた総合的な判断が必要。

もし実装する場合:
1. 1次元BKDツリーの構築（leaf block size: 512~1024）
2. `PointValues.intersect(visitor)` 相当のAPIを実装
3. `NumericComparator` にcompetitive iteratorの構築ロジックを追加
4. コレクションループにcompetitive iteratorとのconjunctionを追加

**影響範囲**:
- `index/` パッケージに新規: `bkd_tree.go`, `bkd_writer.go`, `bkd_reader.go`
- `index/segment_writer.go`: PointValues書き込み
- `index/merger.go`: PointValuesマージ
- `search/numeric_field_comparator.go`: competitive iterator構築
- `search/searcher.go`: コレクションループへのiterator統合

**効果**: ソート + range queryの両方を加速。ただし実装コストが非常に大きい。

---

## 4. Competitive Iterator（DocValuesSkipper）

### 課題

BKDツリーなしでも、DocValuesのブロックメタデータ（方針2のスキップインデックス）を使えば
competitive iteratorを構築できる。

### Lucene の実装

**ファイル**: `NumericComparator.java` 内 `DVSkipperCompetitiveDISIBuilder`

BKDツリーが存在しない場合のフォールバックとして、DocValuesSkipperを使用:

```java
// NumericComparator.java:500-525
private class DVSkipperCompetitiveDISIBuilder extends CompetitiveDISIBuilder {
    private final DocValuesSkipper skipper;

    @Override
    protected void doUpdateCompetitiveIterator() {
        // SkipBlockRangeIterator をそのまま competitive iterator として使う
        updateCompetitiveIterator(
            new SkipBlockRangeIterator(skipper, minValueAsLong, maxValueAsLong));
    }
}
```

PointsCompetitiveDISIBuilderと異なり:
- docID集合を**事前にマテリアライズしない**（ブロック単位でlazy判定）
- コスト判定が不要（イテレータの構築コストがほぼゼロ）
- bottomが更新されるたびに新しいIteratorを作るだけ

### gosearch の現状

方針2（DocValues Skip Index）が未実装のため、この最適化も不可能。

### 差分

方針2のスキップインデックスがあれば、追加の実装コストは小さい:

| 観点 | Lucene | gosearch |
|---|---|---|
| DVSkipper→Iterator変換 | `SkipBlockRangeIterator`で直接ラップ | 未実装 |
| コレクションへの統合 | `competitiveIterator()` 経由でAND | 未実装 |
| 更新コスト | ほぼゼロ（新Iteratorを作るだけ） | — |

### 実装方針

方針2が完成した前提で:

1. `DocValuesSkipper` の `advance(minValue, maxValue)` を使って `DocIdSetIterator` 相当のインターフェースを実装
2. `NumericFieldComparator` に `competitiveIterator()` メソッドを追加
3. `TopFieldCollector.GetLeafCollector` でcompetitive iteratorを取得
4. コレクションループ（`Searcher.searchLeaf` 相当）でqueryのイテレータとANDする

**影響範囲**:
- `search/numeric_field_comparator.go`: competitive iterator構築
- `search/leaf_field_comparator.go`: `CompetitiveIterator()` インターフェース追加
- `search/searcher.go`: コレクションループ改修

**効果**: 方針2の効果を最大化。ブロックスキップがcomparatorだけでなくコレクションループ全体に波及する。

---

## 5. Index-time Sorting（セグメント事前ソート）

### 課題

queryのヒット率が高い場合、competitive iteratorで絞り込んでもアクセスするdoc数が多い。
セグメント内のドキュメントがソート順に並んでいれば、top-K件を集めた時点で打ち切れる。

### Lucene の実装

IndexWriterConfigでソート順を指定:

```java
indexWriterConfig.setIndexSort(new Sort(new SortField("price", SortField.Type.LONG)));
```

これによりセグメント内のdocIDが値の昇順（or降順）に対応する。

#### 5-1. フラッシュ時: DocMap による docID の並べ替え

**ファイル**: `Sorter.java`, `IndexingChain.java`

メモリ上のセグメントをディスクに書き出す直前に、doc values の値を使って docID の並べ替え順（`DocMap`）を計算する:

```java
// Sorter.java:132-154
private static DocMap sort(int maxDoc, IndexSorter.DocComparator comparator) {
    // まず既にソート済みかチェック（ソート済みなら null を返す）
    boolean sorted = true;
    for (int i = 1; i < maxDoc; ++i) {
        if (comparator.compare(i - 1, i) > 0) { sorted = false; break; }
    }
    if (sorted) return null;

    // docIDの配列を作ってTimSortでソート
    final int[] docs = new int[maxDoc];
    for (int i = 0; i < maxDoc; i++) docs[i] = i;
    DocValueSorter sorter = new DocValueSorter(docs, comparator);
    sorter.sort(0, docs.length); // docs[] が newToOld マッピングになる

    // 逆変換（oldToNew）も構築し、両方向を PackedLongValues（圧縮済み）で保持
    // ...
    return new DocMap() {
        public int oldToNew(int docID) { return (int) oldToNew.get(docID); }
        public int newToOld(int docID) { return (int) newToOld.get(docID); }
    };
}
```

`DocMap` が生成されると、`IndexingChain.flush()` が**全てのデータの書き出しに sortMap を通す**:

```java
// IndexingChain.java:270-341
Sorter.DocMap sortMap = maybeSortSegment(state);

writeNorms(state, sortMap);                          // norms
writeDocValues(state, sortMap);                      // doc values
writePoints(state, sortMap);                         // point values
storedFieldsConsumer.flush(state, sortMap);           // stored fields
termsHash.flush(fieldsToFlush, state, sortMap, ...); // postings
```

フラッシュ後のセグメントファイルでは **docID=0 がソートキーの最小値（or最大値）に対応**する。
postings の docID リストもソート後の docID で記録される。

ポイント:
- ソートには TimSort を使用（既にほぼソート済みのデータに強い — 追記→ソート→フラッシュを繰り返すケースで有利）
- `DocMap` は `oldToNew(docID)` / `newToOld(docID)` の双方向マッピングを持ち、`PackedLongValues`（monotonic builder）で圧縮保持

#### 5-2. マージ時: MultiSorter による k-way マージソート

**ファイル**: `MultiSorter.java`, `MergeState.java`

各セグメントは既にソート済みなので、k-way マージソートで新しい docID の対応表を構築する:

```java
// MultiSorter.java:85-141
PriorityQueue<LeafAndDocID> queue = ...; // 各セグメントの先頭docを持つヒープ

// 初期化: 各セグメントの先頭docをキューに追加
for (int i = 0; i < leafCount; i++) {
    LeafAndDocID leaf = new LeafAndDocID(i, reader.getLiveDocs(), reader.maxDoc(), ...);
    leaf.valuesAsComparableLongs[j] = comparables[j][i].getAsComparableLong(leaf.docID);
    queue.add(leaf);
}

// マージソート: ヒープから最小のdocを取り出して新しいdocIDを割り当て
int mappedDocID = 0;
while (queue.size() != 0) {
    LeafAndDocID top = queue.top();
    builders[top.readerIndex].add(mappedDocID); // old docID → new docID
    if (top.liveDocs == null || top.liveDocs.get(top.docID)) {
        mappedDocID++; // 生存docだけカウント（削除docはスキップ）
    }
    top.docID++;
    if (top.docID < top.maxDoc) {
        // 次のdocの値を読んでヒープを更新
        top.valuesAsComparableLongs[j] =
            comparables[j][top.readerIndex].getAsComparableLong(top.docID);
        queue.updateTop();
    } else {
        queue.pop();
    }
}
```

各セグメントが既にソート済みなので、先頭から順に PriorityQueue で取り出すだけでグローバルにソートされた docID 順が決まる。

`MergeState.java:204-220` でこれが呼び出される:

```java
private DocMap[] buildDocMaps(List<CodecReader> readers, Sort indexSort) throws IOException {
    if (indexSort == null) {
        return buildDeletionDocMaps(readers); // ソートなし: 削除のみ考慮
    } else {
        DocMap[] result = MultiSorter.sort(indexSort, readers); // k-way マージソート
        if (result == null) {
            return buildDeletionDocMaps(readers); // 既にソート済み
        }
        // ...
    }
}
```

#### 5-3. ソート順メタデータの永続化と保護

**ファイル**: `SegmentInfo.java`, `IndexWriter.java`

ソート順は `SegmentInfo` に保存され、コミットに記録される。
一度 IndexSort を設定すると変更できない制約がある:

```java
// IndexWriter.java:1211-1225
Sort indexSort = config.getIndexSort();
if (indexSort != null) {
    for (SegmentCommitInfo info : segmentInfos) {
        Sort segmentIndexSort = info.info.getIndexSort();
        if (segmentIndexSort == null
            || isCongruentSort(indexSort, segmentIndexSort) == false) {
            throw new IllegalArgumentException(
                "cannot change previous indexSort=" + segmentIndexSort
                + " to new indexSort=" + indexSort);
        }
    }
}
```

#### 5-4. 検索時: Early Termination

**ファイル**: `TopFieldCollector.java`

TopFieldCollectorは検索ソートがインデックスソートのprefixであるかを判定:

```java
// TopFieldCollector.java:147-168
static boolean canEarlyTerminate(Sort searchSort, Sort indexSort) {
    return canEarlyTerminateOnDocId(searchSort)
        || canEarlyTerminateOnPrefix(searchSort, indexSort);
}

private static boolean canEarlyTerminateOnPrefix(Sort searchSort, Sort indexSort) {
    if (indexSort != null) {
        final SortField[] fields1 = searchSort.getSort();
        final SortField[] fields2 = indexSort.getSort();
        // searchSortがindexSortのprefixであればearly termination可能
        return Arrays.asList(fields1).equals(
            Arrays.asList(fields2).subList(0, fields1.length));
    }
    return false;
}
```

early terminationの実行:

```java
// TopFieldCollector.java:89-99
if (collectedAllCompetitiveHits || reverseMul * comparator.compareBottom(doc) <= 0) {
    if (searchSortPartOfIndexSort) {
        if (totalHits > totalHitsThreshold) {
            throw new CollectionTerminatedException(); // セグメント走査を完全に打ち切り
        } else {
            collectedAllCompetitiveHits = true;
            // docIDが昇順 = ソートキーが昇順なので、以降の全docは非競合と確定
        }
    }
}
```

早期打ち切り時は competitive iterator（BKDツリー/DocValuesSkipper）のスキップを無効化する:

```java
// TopFieldCollector.java:58-59
if (searchSortPartOfIndexSort) {
    firstComparator.disableSkipping(); // 不要なので無効化
}
```

### gosearch の現状

- セグメントのソート順メタデータは存在しない
- フラッシュ時・マージ時にソート順を指定する仕組みがない
- `TopFieldCollector` に early termination ロジックがない

### 差分

| 観点 | Lucene | gosearch |
|---|---|---|
| セグメント内ソート | `IndexWriterConfig.setIndexSort` | 未対応 |
| ソート順メタデータ | `SegmentInfo.indexSort` | なし |
| early termination判定 | `canEarlyTerminate(searchSort, indexSort)` | なし |
| 走査打ち切り | `CollectionTerminatedException` | なし |
| フラッシュ時ソート | DWPT.flush()でソート適用 | 未対応 |
| マージ時ソート順維持 | マージ時にソート順でdocIDを再割り当て | 未対応 |

### 実装方針

#### Step 1: セグメントメタデータにソート順を追加

```go
type SegmentMeta struct {
    // ... 既存フィールド
    IndexSort *Sort // nil = ソートなし
}
```

`segments_N` の JSON に `index_sort` フィールドを追加して永続化する。
`IndexWriter` にもソート順の設定と検証ロジックを追加（一度設定したら変更不可）。

#### Step 2: フラッシュ時の DocMap 生成とデータ並べ替え

Luceneの `Sorter.sort()` に相当する処理を実装する:

```go
// sortMap を生成: InMemorySegment の doc values を使ってソート
func buildSortMap(seg *InMemorySegment, indexSort *Sort) []int {
    // docs[i] = i で初期化
    // indexSort の comparator で sort.Slice
    // docs[] が newToOld マッピングになる
    // oldToNew[] も構築して返す
}
```

`WriteSegmentV2` に `sortMap` を受け取る引数を追加し、
postings, doc values, stored fields, field lengths を全て sortMap 経由で書き出す:

- **postings**: 各 term の docID リストを sortMap で変換。ソート後のdocID順で delta encoding
- **doc values**: `newDocID → oldDocID` で元の値を引き、newDocID 順に書き出す
- **stored fields**: 同様に newDocID 順で書き出す
- **field lengths**: newDocID 順に並べ替え

gosearchの既存の `DocIDMapper`（マージ時の削除doc再マッピング）と類似の考え方。

#### Step 3: マージ時の k-way マージソート

Luceneの `MultiSorter` に相当する処理:

```go
func mergeSortedSegments(segments []SegmentReader, indexSort *Sort) []DocMap {
    // 各セグメントの先頭 doc を PriorityQueue に追加
    // ヒープから最小を取り出して新 docID を割り当て
    // 各セグメントの oldDocID → newDocID マッピングを構築
}
```

現在の `merger.go` の `MergeSegmentsToDisk` は term 単位のストリーミングマージだが、
IndexSort がある場合は docID の順序が変わるため、postings の docID リストも再マッピングが必要。

#### Step 4: TopFieldCollector に early termination 追加

```go
func (c *TopFieldCollector) GetLeafCollector(ctx index.LeafReaderContext) LeafCollector {
    indexSort := ctx.Segment.Meta().IndexSort
    if canEarlyTerminate(c.sort, indexSort) {
        // early termination 有効化フラグをセット
    }
}
```

`Collect()` 内で、heapが満杯後に `compareWithBottom` で負けた場合:
- `collectedAllCompetitiveHits = true` を設定
- 以降の doc は全てスキップ
- または `CollectionTerminated` エラーを返してセグメント走査を打ち切り

#### Step 5: Searcher のコレクションループ改修

`CollectionTerminated` エラーをハンドリングし、そのセグメントの走査だけを打ち切る
（他のセグメントの走査は継続する）。

**影響範囲**:

| ファイル | 変更内容 |
|---|---|
| `index/segment_meta.go` | ソート順メタデータ追加、JSON シリアライズ |
| `index/writer.go` | IndexSort 設定、検証（変更不可制約） |
| `index/dwpt.go` | フラッシュ時に `buildSortMap` 呼び出し |
| `index/segment_writer.go` | `WriteSegmentV2` に sortMap 対応追加 |
| `index/merger.go` | `mergeSortedSegments` 追加、既存マージへの統合 |
| `search/top_field_collector.go` | `canEarlyTerminate`, `collectedAllCompetitiveHits` |
| `search/searcher.go` | `CollectionTerminated` エラーハンドリング |

**効果**: ヒット率が高いクエリに対して劇的な改善。K件集めた後のdocは全てスキップされるため、計算量がヒット数に依存しなくなる。ただしインデックス構築時のコストが増加し、1つのソート順にしか最適化できない制約がある。

---

## 6. 方針比較と優先度

### 効果 vs 実装コスト

```
効果(大)
  ^
  |  [5. IndexSort]
  |        *
  |
  |           [4. DV Competitive Iterator]
  |                *
  |     [2. DV Skip Index]
  |          *
  |
  |                          [3. BKDツリー]
  |                               *
  |  [1. DV圧縮]
  |     *
  +----------------------------------------> 実装コスト(大)
```

### 推奨優先順位

#### Phase 1: DocValues Skip Index + Competitive Iterator（方針2 + 4）

**理由**: BKDツリーなしで最大の効果が得られる組み合わせ。

- 方針2でブロックごとの min/max メタデータを記録
- 方針4でそのメタデータを使った competitive iterator を構築
- コレクションループでqueryイテレータとANDし、非競合ドキュメントの走査自体をスキップ

期待される改善:
- bottomに勝てないブロック内のdocは `CompareBottom` すら呼ばれなくなる
- doc valuesのmmapアクセスが大幅に削減
- ブロックサイズの調整でキャッシュローカリティも改善

#### Phase 2: Index-time Sorting（方針5）

**理由**: ヒット率が高いクエリに対して決定的な改善。Phase 1と組み合わせると更に効果的。

- 頻繁に使うソートキー（例: timestamp, score）をIndexSortに設定
- early terminationでK件取得後に即打ち切り

注意点:
- 全てのセグメントファイル（postings, stored fields, doc values）のdocID順がソート順に変わるため、影響範囲が大きい
- フラッシュ・マージの両方で対応が必要

#### Phase 3: DocValues 圧縮（方針1）

**理由**: ストレージ効率の改善。ソート性能への直接的効果は限定的だが、全体的なI/O削減に寄与。

#### Phase 4: BKDツリー（方針3）

**理由**: ソート最適化だけでなく range query の高速化にも寄与するが、実装コストが大きい。
Phase 1の DocValuesSkipper が十分に機能するなら、ソート用途では優先度は低い。
range query のニーズが出てきた時点で検討。
