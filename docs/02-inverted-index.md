# Step 2: 転置インデックス（Inverted Index）

## 学ぶ概念

転置インデックスは検索エンジンの心臓部です。通常のデータ構造は「ドキュメント → 含まれる単語」ですが、検索では逆に「**単語 → その単語を含むドキュメント一覧**」が必要です。これが "inverted"（転置）の意味です。

```
Forward Index（通常）:
  Doc0: ["the", "quick", "brown", "fox"]
  Doc1: ["the", "lazy", "brown", "dog"]

Inverted Index（転置）:
  "the"   → [Doc0, Doc1]
  "quick" → [Doc0]
  "brown" → [Doc0, Doc1]
  "fox"   → [Doc0]
  "lazy"  → [Doc1]
  "dog"   → [Doc1]
```

### Postings List

各 term に対応するドキュメントのリストを **Postings List**（ポスティングリスト）と呼びます。
各エントリ（Posting）には以下の情報が含まれます：

| フィールド | 用途 |
|-----------|------|
| DocID | どのドキュメントか |
| Freq | そのドキュメント内での出現回数（スコアリングに使う） |
| Positions | 出現位置の列（フレーズ検索に使う） |

```
"brown" → PostingsList [
    Posting{DocID: 0, Freq: 1, Positions: [2]},
    Posting{DocID: 1, Freq: 1, Positions: [2]},
]
```

### Field（フィールド）

Lucene ではドキュメントは複数の **フィールド** を持ちます。ES のマッピングに対応する概念です。

```json
{
  "title": "The Quick Brown Fox",
  "body": "A fox jumped over the lazy dog"
}
```

転置インデックスは **フィールドごと** に作られます。`title:fox` と `body:fox` は別のポスティングリストです。

---

## Lucene ソースを読む

### 読むべきファイル（`lucene/lucene/core/src/java/org/apache/lucene/`）

| ファイル | ポイント |
|----------|----------|
| `index/PostingsEnum.java` | Postings の読み取り interface。`nextDoc()`, `freq()`, `nextPosition()` がコア API |
| `index/Terms.java` | あるフィールドの全 term を管理。`iterator()` で TermsEnum を取得 |
| `index/TermsEnum.java` | term を1つずつ走査する iterator。`seekExact()` で特定 term を検索 |
| `index/Term.java` | field名 + term のペア |
| `index/Fields.java` | ドキュメント中の全フィールドを管理 |
| `index/IndexingChain.java` | ドキュメントが追加されたときの処理の流れ。Analyzer → 転置インデックス構築のパイプライン |
| `index/FreqProxTermsWriter.java` | 頻度(Freq)と位置(Prox)をインメモリで書き込む |
| `index/TermsHash.java` | term をハッシュテーブルで管理する低レベル構造 |
| `document/Document.java` | ドキュメントの定義 |
| `document/Field.java` | フィールドの定義。TextField, StringField などの基底 |
| `document/TextField.java` | 全文検索用フィールド（Analyzer を通す） |
| `document/StringField.java` | キーワードフィールド（Analyzer を通さない、完全一致用） |

### Lucene の転置インデックス構築フロー

```
IndexWriter.addDocument(doc)
  → IndexingChain.processDocument()
    → 各フィールドに対して:
      → Analyzer.tokenStream() でトークン化
      → TermsHash にトークンを追加
        → FreqProxTermsWriter で freq/position を記録
```

---

## Go で実装する

### 1. Document & Field

```go
// document/document.go

package document

// FieldType はフィールドの種類を表す。
type FieldType int

const (
    // FieldTypeText は Analyzer を通して転置インデックスに追加するフィールド。
    FieldTypeText FieldType = iota
    // FieldTypeKeyword は Analyzer を通さず、そのまま1つの term として扱うフィールド。
    FieldTypeKeyword
    // FieldTypeStored は検索には使わず、結果取得時に返すだけのフィールド。
    FieldTypeStored
)

// Field はドキュメント内の1つのフィールドを表す。
type Field struct {
    Name  string
    Value string
    Type  FieldType
}

// Document はインデックスに追加される1つのドキュメントを表す。
type Document struct {
    Fields []Field
}

func NewDocument() *Document {
    return &Document{}
}

func (d *Document) AddField(name, value string, fieldType FieldType) {
    d.Fields = append(d.Fields, Field{
        Name:  name,
        Value: value,
        Type:  fieldType,
    })
}
```

### 2. Posting 構造体

```go
// index/postings.go

package index

// Posting は1つの term の1つのドキュメントにおける出現情報。
type Posting struct {
    DocID     int   // ドキュメントID
    Freq      int   // 出現回数
    Positions []int // 出現位置のリスト
}

// PostingsList はある term のポスティングリスト。
// DocID の昇順にソートされている。
type PostingsList struct {
    Term     string
    Postings []Posting
}
```

### 3. インメモリ転置インデックス

```go
// index/index.go

package index

import (
    "gosearch/analysis"
    "gosearch/document"
)

// FieldIndex はあるフィールドの転置インデックス。
// term → PostingsList のマッピング。
type FieldIndex struct {
    postings map[string]*PostingsList
}

func newFieldIndex() *FieldIndex {
    return &FieldIndex{
        postings: make(map[string]*PostingsList),
    }
}

// InMemoryIndex はインメモリの転置インデックス。
type InMemoryIndex struct {
    analyzer *analysis.Analyzer
    fields   map[string]*FieldIndex // fieldName → FieldIndex
    docCount int
    // stored fields: docID → fieldName → value
    storedFields map[int]map[string]string
}

func NewInMemoryIndex(analyzer *analysis.Analyzer) *InMemoryIndex {
    return &InMemoryIndex{
        analyzer:     analyzer,
        fields:       make(map[string]*FieldIndex),
        storedFields: make(map[int]map[string]string),
    }
}

// AddDocument はドキュメントをインデックスに追加する。
func (idx *InMemoryIndex) AddDocument(doc *document.Document) error {
    docID := idx.docCount
    idx.docCount++

    for _, field := range doc.Fields {
        switch field.Type {
        case document.FieldTypeText:
            if err := idx.indexTextField(docID, field); err != nil {
                return err
            }
        case document.FieldTypeKeyword:
            idx.indexKeywordField(docID, field)
        }

        // Stored fields の保存
        if field.Type == document.FieldTypeStored || field.Type == document.FieldTypeText {
            if idx.storedFields[docID] == nil {
                idx.storedFields[docID] = make(map[string]string)
            }
            idx.storedFields[docID][field.Name] = field.Value
        }
    }

    return nil
}

// indexTextField は Analyzer を通してフィールドを転置インデックスに追加する。
func (idx *InMemoryIndex) indexTextField(docID int, field document.Field) error {
    tokens, err := idx.analyzer.Analyze(field.Value)
    if err != nil {
        return err
    }

    fi := idx.getOrCreateFieldIndex(field.Name)

    // term ごとに freq と positions を集計
    termInfo := make(map[string]*Posting)
    for _, token := range tokens {
        posting, exists := termInfo[token.Term]
        if !exists {
            posting = &Posting{DocID: docID}
            termInfo[token.Term] = posting
        }
        posting.Freq++
        posting.Positions = append(posting.Positions, token.Position)
    }

    // PostingsList に追加
    for term, posting := range termInfo {
        pl, exists := fi.postings[term]
        if !exists {
            pl = &PostingsList{Term: term}
            fi.postings[term] = pl
        }
        pl.Postings = append(pl.Postings, *posting)
    }

    return nil
}

// indexKeywordField は Analyzer を通さずにフィールドを転置インデックスに追加する。
func (idx *InMemoryIndex) indexKeywordField(docID int, field document.Field) {
    fi := idx.getOrCreateFieldIndex(field.Name)

    pl, exists := fi.postings[field.Value]
    if !exists {
        pl = &PostingsList{Term: field.Value}
        fi.postings[field.Value] = pl
    }
    pl.Postings = append(pl.Postings, Posting{
        DocID:     docID,
        Freq:      1,
        Positions: []int{0},
    })
}

func (idx *InMemoryIndex) getOrCreateFieldIndex(fieldName string) *FieldIndex {
    fi, exists := idx.fields[fieldName]
    if !exists {
        fi = newFieldIndex()
        idx.fields[fieldName] = fi
    }
    return fi
}

// GetPostings はある field の term に対するポスティングリストを返す。
func (idx *InMemoryIndex) GetPostings(fieldName, term string) *PostingsList {
    fi, exists := idx.fields[fieldName]
    if !exists {
        return nil
    }
    return fi.postings[term]
}

// GetStoredFields はドキュメントの stored fields を返す。
func (idx *InMemoryIndex) GetStoredFields(docID int) map[string]string {
    return idx.storedFields[docID]
}

// DocCount はインデックス内のドキュメント数を返す。
func (idx *InMemoryIndex) DocCount() int {
    return idx.docCount
}
```

### 4. 簡易検索（単一 term 検索）

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

// SimpleSearch は単一 term で検索し、マッチしたドキュメントを返す。
// この段階ではスコアリングなし（Step 3 で追加）。
func SimpleSearch(idx *index.InMemoryIndex, field, term string) []SearchResult {
    pl := idx.GetPostings(field, term)
    if pl == nil {
        return nil
    }

    var results []SearchResult
    for _, posting := range pl.Postings {
        results = append(results, SearchResult{
            DocID:  posting.DocID,
            Score:  1.0, // 暫定スコア
            Fields: idx.GetStoredFields(posting.DocID),
        })
    }
    return results
}
```

---

## 確認・テスト

```go
// index/index_test.go

package index

import (
    "gosearch/analysis"
    "gosearch/document"
    "testing"
)

func TestInvertedIndex(t *testing.T) {
    analyzer := analysis.NewAnalyzer(
        analysis.NewWhitespaceTokenizer(),
        &analysis.LowerCaseFilter{},
    )
    idx := NewInMemoryIndex(analyzer)

    // ドキュメント追加
    doc0 := document.NewDocument()
    doc0.AddField("title", "The Quick Brown Fox", document.FieldTypeText)
    idx.AddDocument(doc0)

    doc1 := document.NewDocument()
    doc1.AddField("title", "The Lazy Brown Dog", document.FieldTypeText)
    idx.AddDocument(doc1)

    // "brown" は両方のドキュメントにある
    pl := idx.GetPostings("title", "brown")
    if pl == nil {
        t.Fatal("expected postings for 'brown'")
    }
    if len(pl.Postings) != 2 {
        t.Fatalf("expected 2 postings, got %d", len(pl.Postings))
    }

    // "fox" は doc0 のみ
    pl = idx.GetPostings("title", "fox")
    if pl == nil {
        t.Fatal("expected postings for 'fox'")
    }
    if len(pl.Postings) != 1 {
        t.Fatalf("expected 1 posting, got %d", len(pl.Postings))
    }
    if pl.Postings[0].DocID != 0 {
        t.Errorf("expected docID 0, got %d", pl.Postings[0].DocID)
    }

    // 存在しない term
    pl = idx.GetPostings("title", "cat")
    if pl != nil {
        t.Error("expected nil for 'cat'")
    }
}

func TestPostingFreqAndPositions(t *testing.T) {
    analyzer := analysis.NewAnalyzer(
        analysis.NewWhitespaceTokenizer(),
        &analysis.LowerCaseFilter{},
    )
    idx := NewInMemoryIndex(analyzer)

    doc := document.NewDocument()
    doc.AddField("body", "the fox and the fox", document.FieldTypeText)
    idx.AddDocument(doc)

    pl := idx.GetPostings("body", "fox")
    if pl.Postings[0].Freq != 2 {
        t.Errorf("expected freq 2, got %d", pl.Postings[0].Freq)
    }
    // "fox" は position 1 と 4 に出現
    if pl.Postings[0].Positions[0] != 1 || pl.Postings[0].Positions[1] != 4 {
        t.Errorf("expected positions [1,4], got %v", pl.Postings[0].Positions)
    }
}
```

---

## 深掘り：なぜこう設計されているのか

### Q: なぜ PostingsList は DocID 昇順なのか？

理由は2つ：

1. **Boolean クエリの効率化**: AND 検索は「2つの PostingsList の共通 DocID を見つける」操作。両方がソート済みなら、マージソートのようにO(n)で処理できる
2. **圧縮効率**: DocID を差分（delta）で保存すると、昇順なら差分が常に正の小さい値になり、可変長整数で効率よく圧縮できる（Step 10 で詳しく扱う）

### Q: Lucene の TermsHash は何をしているのか？

`TermsHash` は、インメモリで term → postings を管理するハッシュテーブルです。我々の実装では Go の `map[string]*PostingsList` で代用していますが、Lucene ではメモリ効率のために `ByteBlockPool`（バイト配列のプール）を使って独自のメモリ管理をしています。これは GC の負荷を減らすための最適化です。

### Q: なぜフィールドごとに転置インデックスを分けるのか？

フィールドごとに分けることで：
- `title` フィールドだけを検索する、といったことが可能
- フィールドごとに異なる Analyzer を適用できる（例: `title` は standard、`email` は keyword）
- フィールドごとに異なるスコアリング設定が可能（例: `title` のマッチは `body` の2倍重要）

これは ES の mapping で `"type": "text"` や `"type": "keyword"` を設定するのと同じ構造です。

---

## 次のステップ

転置インデックスができたので、次は [Step 3: BM25 スコアリング](03-scoring.md) で、検索結果に「どれだけ関連性が高いか」のスコアを付けます。
