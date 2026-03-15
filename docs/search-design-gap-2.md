# Advance の線形スキャン問題とスキップリストによる最適化

## Status Summary

現在の `PostingsDocIdSetIterator.Advance` は `NextDoc()` を繰り返し呼ぶ線形スキャンで実装されている。ConjunctionScorer のように lead iterator に合わせて follower を `Advance` するケースで、docID が大きく離れている場合にパフォーマンスが著しく劣化する。Lucene のスキップリストパターンを導入することでこの問題を解決する。

---

## 問題: Advance の線形スキャン

### 現在の実装

`search/iterators.go`:
```go
func (it *PostingsDocIdSetIterator) Advance(target int) int {
    for {
        doc := it.NextDoc()
        if doc == NoMoreDocs || doc >= target {
            return doc
        }
    }
}
```

`search/conjunction_scorer.go` の `advanceToMatch` では、lead (最小コスト) の docID に合わせて follower を `Advance` する:
```go
func (s *ConjunctionScorer) advanceToMatch(doc int) int {
    for doc != NoMoreDocs {
        allMatch := true
        for i := 1; i < len(s.iters); i++ {
            otherDoc := s.iters[i].DocID()
            if otherDoc < doc {
                otherDoc = s.iters[i].Advance(doc) // 線形スキャンで追いかける
            }
            // ...
        }
    }
}
```

### 問題点

1. **大きなギャップでの非効率性**: lead が `1 -> 800,000` に進んだ場合、follower のポスティングリスト `[2, 3, 7, 90, 120, ...]` を 800,000 に到達するまで1件ずつ辿る
2. **計算量**: `O(n)` (n = スキップするドキュメント数)。大規模インデックスでは数十万件のスキャンが発生しうる
3. **ConjunctionScorer への影響**: AND クエリで出現頻度が大きく異なるターム同士を組み合わせると、頻出タームの follower が毎回大量のドキュメントを線形スキャンする

### バイナリサーチが解決策にならない理由

インメモリ配列に対するバイナリサーチ (`O(log n)`) は一見有効だが、mmap ベースのインデックスでは問題がある:

- バイナリサーチは中間地点へのランダムアクセスを繰り返す
- mmap されたポスティングリストでは、遠いページへのアクセスのたびにページフォルトが発生する
- ページキャッシュの退避・追加のオーバーヘッドがスキャン自体のコストを上回る可能性がある

---

## 解決策: スキップリストの導入

Lucene はポスティングリストにスキップデータを埋め込み、`Advance` で前方ジャンプを可能にしている。

### スキップリストの構造

```
ポスティングリスト:
  [doc0, freq] [doc1, freq] [doc2, freq] ... [docN, freq]

スキップデータ (多段):
  Level 0: skipInterval (例: 128) 件ごとにスキップポインタ
  Level 1: skipInterval^2 件ごと
  Level 2: skipInterval^3 件ごと
  ...

スキップポインタ:
  (docID, ファイルオフセット)
```

### アクセスパターン

```
線形スキャン:    [1] [2] [3] ... [799999] [800000]  <- 全件読む
バイナリサーチ:  [400000] -> [600000] -> [700000]    <- ランダムジャンプ
スキップリスト:  [0] ->skip-> [128] ->skip-> [256]   <- 常に前方向のみ
```

スキップリストの特徴:
- **前方のみのアクセス**: バイナリサーチと違い、後方へのジャンプが発生しない
- **ファイルオフセットによるシーク**: 途中のポスティングデータを読み飛ばせる
- **ページ局所性の維持**: スキップの粒度がページサイズと合えばページフォルトが最小限

### Lucene のリファレンス実装

#### Lucene912PostingsWriter (書き込み時)

`lucene/core/src/java/org/apache/lucene/codecs/lucene912/Lucene912PostingsWriter.java`:
```java
// ブロック (128件) ごとにスキップデータを記録
if (docBufferUpto == BLOCK_SIZE) {
    skipWriter.writeSkipData(lastBlockDocID, docDeltaBuffer, freqBuffer);
    flushDocBlock();
}
```

#### Lucene912PostingsReader (読み取り時の Advance)

`lucene/core/src/java/org/apache/lucene/codecs/lucene912/Lucene912PostingsReader.java`:
```java
public int advance(int target) throws IOException {
    if (target > nextSkipDoc) {
        // スキップリストを使って target 直前のブロックまでジャンプ
        skipper.skipTo(target);
        // ブロックの先頭からデコード
        refillDocs();
    }
    // ブロック内で target を線形スキャン
    // ...
}
```

---

## 実装計画

### Step 1: インメモリバイナリサーチによる暫定対応

ディスクベースのスキップリスト導入前に、現在のインメモリポスティングリストに対してバイナリサーチを適用する。mmap 環境では最適ではないが、現時点のインメモリ実装では有効。

```go
// search/iterators.go

func (it *PostingsDocIdSetIterator) Advance(target int) int {
    // PostingsIterator がバイナリサーチ可能な Advance を提供する
    if it.postings.Advance(target) {
        it.docID = it.postings.DocID()
        return it.docID
    }
    it.docID = NoMoreDocs
    return NoMoreDocs
}
```

`index.PostingsIterator` インターフェースに `Advance(target int) bool` メソッドを追加し、内部の docID 配列に対して `sort.SearchInts` 等でバイナリサーチを行う。

### Step 2: ディスクベースのポスティングフォーマットにスキップデータを追加

インデックスをディスクベースに移行する際に、ポスティングリストの書き込み時にスキップデータを埋め込む。

```go
// index/postings_writer.go (新規)

type SkipEntry struct {
    DocID      int   // スキップポイントの docID
    FileOffset int64 // ポスティングデータ内のオフセット
}

// skipInterval (例: 128) 件ごとにスキップエントリを記録
func (w *PostingsWriter) writeBlock(docs []int, freqs []int) {
    if w.docCount % skipInterval == 0 {
        w.skipEntries = append(w.skipEntries, SkipEntry{
            DocID:      docs[0],
            FileOffset: w.currentOffset,
        })
    }
    // ポスティングデータを書き出し
}
```

### Step 3: スキップリストを使った Advance の実装

```go
// index/postings_reader.go (新規)

func (r *PostingsReader) Advance(target int) bool {
    // スキップエントリをバイナリサーチして target 直前のブロックを見つける
    idx := sort.Search(len(r.skipEntries), func(i int) bool {
        return r.skipEntries[i].DocID >= target
    })
    if idx > 0 {
        idx--
    }

    // そのブロックの先頭にシーク
    r.seekTo(r.skipEntries[idx].FileOffset)

    // ブロック内を線形スキャンして target 以上の docID を見つける
    for r.Next() {
        if r.DocID() >= target {
            return true
        }
    }
    return false
}
```

### Step 4: 多段スキップリストへの拡張

大規模インデックス向けに、スキップリストを多段化する。Level 0 は 128 件ごと、Level 1 は 128^2 件ごとにスキップポインタを配置し、上位レベルから下位レベルへ段階的に絞り込む。

### 移行時の注意点

- Step 1 のバイナリサーチは現在のインメモリ実装にのみ有効。mmap/ディスクベースに移行する場合は Step 2-3 が必須
- `PostingsIterator` インターフェースの変更は `TermQuery`, `PhraseQuery`, `BooleanQuery` の全 Scorer 実装に影響する
- ConjunctionScorer と DisjunctionScorer のテストにおいて、docID が大きく離れたケース (例: `[1, 800000]` vs `[2, 3, 7, ..., 800000]`) を追加して性能改善を検証すべき
