# GoSearch: Lucene を Go で再実装しながら学ぶロードマップ

## 概要

このドキュメントは、Apache Lucene の設計思想と実装を理解するために、Go で最小構成の検索エンジン「GoSearch」を段階的に構築していくための学習ロードマップです。

各ステップでは以下の構成で進めます：

1. **学ぶ概念** — そのステップで理解すべき検索エンジンの概念
2. **Lucene のソースを読む** — 参照すべき Java クラスとその役割
3. **Go で実装する** — 最小限のコードで概念を実装
4. **確認・テスト** — 動作確認と理解度チェック

---

## 全体アーキテクチャ

最終的に構築するシステムの全体像：

```
┌─────────────────────────────────────────────────┐
│                   IndexSearcher                  │
│  (クエリ実行・スコアリング・結果収集)              │
├─────────────────────────────────────────────────┤
│                   IndexReader                    │
│  (複数 Segment を束ねて読む)                      │
├──────────┬──────────┬──────────┬────────────────┤
│ Segment0 │ Segment1 │ Segment2 │ ...            │
│ ┌──────┐ │ ┌──────┐ │ ┌──────┐ │                │
│ │Postings│ │ │Postings│ │ │Postings│ │               │
│ │DocVals │ │ │DocVals │ │ │DocVals │ │               │
│ │Stored │ │ │Stored │ │ │Stored │ │               │
│ │Norms  │ │ │Norms  │ │ │Norms  │ │               │
│ └──────┘ │ └──────┘ │ └──────┘ │                │
├──────────┴──────────┴──────────┴────────────────┤
│                  IndexWriter                     │
│  (ドキュメント追加・セグメント管理・マージ)        │
├─────────────────────────────────────────────────┤
│                   Analyzer                       │
│  (テキスト → トークン列への変換)                  │
├─────────────────────────────────────────────────┤
│                   Directory                      │
│  (ファイル I/O 抽象化)                            │
└─────────────────────────────────────────────────┘
```

---

## 学習フェーズ

### Phase A: 検索の本質（Step 1〜5）

最短で「検索エンジンとは何か」が体感できるフェーズ。

| Step                           | テーマ                 | ゴール                             |
| ------------------------------ | ---------------------- | ---------------------------------- |
| [Step 1](01-analyzer.md)       | Analyzer & Tokenizer   | テキストをトークン列に分解できる   |
| [Step 2](02-inverted-index.md) | 転置インデックス       | term → docID リストの構築と検索    |
| [Step 3](03-scoring.md)        | BM25 スコアリング      | 検索結果に関連度スコアを付けられる |
| [Step 4](04-queries.md)        | Boolean & Phrase Query | AND/OR/NOT とフレーズ検索          |
| [Step 5](05-segments.md)       | Segment アーキテクチャ | immutable segment による追記型設計 |

### Phase B: Lucene っぽさ（Step 6〜8）

速度と実用性の根拠が見えるフェーズ。

| Step                        | テーマ            | ゴール                                   |
| --------------------------- | ----------------- | ---------------------------------------- |
| [Step 6](06-persistence.md) | ディスク永続化    | セグメントをファイルに書き出し・読み込み |
| [Step 7](07-merge.md)       | Segment Merge     | 複数セグメントの統合と Merge Policy      |
| [Step 8](08-docvalues.md)   | Doc Values & 集計 | 列指向ストレージによるソート・集計       |

### Phase C: 実用に近づける（Step 9〜10）

ES の挙動の裏側が分かるフェーズ。

| Step                         | テーマ                    | ゴール                                     |
| ---------------------------- | ------------------------- | ------------------------------------------ |
| [Step 9](09-nrt.md)          | Near Real-Time 検索       | refresh で新しいドキュメントが見える仕組み |
| [Step 10](10-compression.md) | Postings 圧縮 & Skip List | 大規模データでの高速化技術                 |

---

## プロジェクト構成（最終形）

```
gosearch/
├── docs/                    # この学習ドキュメント群
├── lucene/                  # 参照用 Lucene Java ソース
├── go.mod
├── analysis/                # Analyzer, Tokenizer
│   ├── analyzer.go
│   ├── tokenizer.go
│   └── filter.go
├── document/                # Document, Field
│   ├── document.go
│   └── field.go
├── index/                   # IndexWriter, Segment, Postings
│   ├── writer.go
│   ├── reader.go
│   ├── segment.go
│   ├── postings.go
│   └── docvalues.go
├── search/                  # IndexSearcher, Query, Scorer
│   ├── searcher.go
│   ├── query.go
│   ├── scorer.go
│   └── collector.go
└── store/                   # Directory, IO abstraction
   ├── directory.go
   └── io.go
```

---

## 各ステップの進め方

1. まずドキュメントの「学ぶ概念」セクションを読む
2. Lucene の該当ソースコードを軽く眺める（全部読む必要はない）
3. Go で実装する（ドキュメントにコード例あり）
4. テストを書いて動作確認
5. 「なぜこうなっているのか」を考える（ドキュメントの「深掘り」セクション）

各ステップは前のステップの成果物の上に積み上げる形になっています。
Step 1 から順番に進めてください。

## Rules

- 実装をする場合は、ドキュメントコードを参考にしつつテストコードを書いて動作が通ることを確認してください
- コメントやテストタイトルなどは英語で記載すること

## Context

- Luceneのソースコードは `lucene/` ディレクトリに置いてあります
