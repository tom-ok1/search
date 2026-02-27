# Step 1: Analyzer & Tokenizer

## 学ぶ概念

検索エンジンの最初の仕事は、人間が書いた自然言語テキストを **検索可能な単位（トークン）** に分解することです。

例えば `"The Quick Brown Fox"` というテキストは、以下のように処理されます：

```
入力: "The Quick Brown Fox"
  ↓ Tokenizer（空白で分割）
  ["The", "Quick", "Brown", "Fox"]
  ↓ LowerCaseFilter（小文字化）
  ["the", "quick", "brown", "fox"]
  ↓ StopWordFilter（ストップワード除去）※オプション
  ["quick", "brown", "fox"]
```

### なぜこれが必要か？

ユーザが `"fox"` と検索したとき、本文中の `"Fox"` にもマッチさせたい。
そのために、**インデックス時と検索時の両方で同じ正規化処理**を適用します。

### Lucene の用語

| 用語 | 意味 |
|------|------|
| **Analyzer** | Tokenizer + TokenFilter のパイプライン全体 |
| **Tokenizer** | テキストを最初のトークン列に分割する（入力は文字列） |
| **TokenFilter** | トークン列を変換するフィルタ（入力はトークン列） |
| **CharFilter** | Tokenizer の前に文字列自体を変換する（HTMLタグ除去など） |
| **Token** | 1つの検索可能な単位。位置情報（position）やオフセット（元テキスト中の位置）を持つ |

---

## Lucene ソースを読む

### 読むべきファイル（`lucene/lucene/core/src/java/org/apache/lucene/`）

| ファイル | ポイント |
|----------|----------|
| `analysis/Analyzer.java` | `tokenStream()` メソッドが entry point。Tokenizer + Filter のパイプラインを組み立てる |
| `analysis/Tokenizer.java` | `Reader` から文字を読んでトークンを生成する抽象クラス |
| `analysis/TokenStream.java` | `incrementToken()` で1トークンずつ進める Iterator パターン |
| `analysis/TokenFilter.java` | 別の `TokenStream` をラップして変換する |
| `analysis/LowerCaseFilter.java` | 最もシンプルな TokenFilter の実装例 |
| `analysis/standard/StandardAnalyzer.java` | デフォルト Analyzer の実装（Tokenizer + Filter の組み立て方が分かる） |
| `analysis/tokenattributes/CharTermAttribute.java` | トークンのテキスト内容を保持する Attribute |

### 特に注目すべきパターン

Lucene の Analyzer は **Attribute パターン** を使っています：

```java
// TokenStream から属性を取得
CharTermAttribute termAttr = tokenStream.addAttribute(CharTermAttribute.class);
PositionIncrementAttribute posAttr = tokenStream.addAttribute(PositionIncrementAttribute.class);

while (tokenStream.incrementToken()) {
    String term = termAttr.toString();
    int posIncr = posAttr.getPositionIncrement();
    // ...
}
```

Go ではこの Attribute パターンは不要です。シンプルに struct で表現します。

---

## Go で実装する

### 1. Token 構造体

```go
// analysis/token.go

package analysis

// Token はテキストから抽出された1つの検索単位を表す。
type Token struct {
    // Term はトークンの文字列（正規化後）。
    Term string
    // Position はドキュメント内でのトークン位置（0始まり）。
    // フレーズ検索で使う。
    Position int
    // StartOffset は元テキスト中の開始バイト位置。
    StartOffset int
    // EndOffset は元テキスト中の終了バイト位置。
    EndOffset int
}
```

### 2. Tokenizer インターフェース

```go
// analysis/tokenizer.go

package analysis

import "io"

// Tokenizer はテキストをトークン列に分割する。
type Tokenizer interface {
    // Tokenize はテキストをトークン列に変換する。
    Tokenize(reader io.Reader) ([]Token, error)
}

// WhitespaceTokenizer は空白文字でテキストを分割する最もシンプルな Tokenizer。
type WhitespaceTokenizer struct{}

func NewWhitespaceTokenizer() *WhitespaceTokenizer {
    return &WhitespaceTokenizer{}
}

func (t *WhitespaceTokenizer) Tokenize(reader io.Reader) ([]Token, error) {
    data, err := io.ReadAll(reader)
    if err != nil {
        return nil, err
    }
    text := string(data)

    var tokens []Token
    position := 0
    start := 0
    inToken := false

    for i, ch := range text {
        if isWhitespace(ch) {
            if inToken {
                tokens = append(tokens, Token{
                    Term:        text[start:i],
                    Position:    position,
                    StartOffset: start,
                    EndOffset:   i,
                })
                position++
                inToken = false
            }
        } else {
            if !inToken {
                start = i
                inToken = true
            }
        }
    }
    // 最後のトークン
    if inToken {
        tokens = append(tokens, Token{
            Term:        text[start:],
            Position:    position,
            StartOffset: start,
            EndOffset:   len(text),
        })
    }

    return tokens, nil
}

func isWhitespace(ch rune) bool {
    return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}
```

### 3. TokenFilter インターフェース

```go
// analysis/filter.go

package analysis

import "strings"

// TokenFilter はトークン列を変換する。
type TokenFilter interface {
    Filter(tokens []Token) []Token
}

// LowerCaseFilter は全てのトークンを小文字に変換する。
type LowerCaseFilter struct{}

func (f *LowerCaseFilter) Filter(tokens []Token) []Token {
    for i := range tokens {
        tokens[i].Term = strings.ToLower(tokens[i].Term)
    }
    return tokens
}
```

### 4. Analyzer

```go
// analysis/analyzer.go

package analysis

import (
    "io"
    "strings"
)

// Analyzer は Tokenizer と TokenFilter のパイプラインをまとめたもの。
type Analyzer struct {
    tokenizer Tokenizer
    filters   []TokenFilter
}

func NewAnalyzer(tokenizer Tokenizer, filters ...TokenFilter) *Analyzer {
    return &Analyzer{
        tokenizer: tokenizer,
        filters:   filters,
    }
}

// Analyze はテキストをトークン列に変換する。
func (a *Analyzer) Analyze(text string) ([]Token, error) {
    tokens, err := a.tokenizer.Tokenize(strings.NewReader(text))
    if err != nil {
        return nil, err
    }
    for _, filter := range a.filters {
        tokens = filter.Filter(tokens)
    }
    return tokens, nil
}
```

---

## 確認・テスト

```go
// analysis/analyzer_test.go

package analysis

import (
    "testing"
)

func TestAnalyzer(t *testing.T) {
    analyzer := NewAnalyzer(
        NewWhitespaceTokenizer(),
        &LowerCaseFilter{},
    )

    tokens, err := analyzer.Analyze("The Quick Brown Fox")
    if err != nil {
        t.Fatal(err)
    }

    expected := []string{"the", "quick", "brown", "fox"}
    if len(tokens) != len(expected) {
        t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
    }
    for i, tok := range tokens {
        if tok.Term != expected[i] {
            t.Errorf("token[%d]: expected %q, got %q", i, expected[i], tok.Term)
        }
        if tok.Position != i {
            t.Errorf("token[%d]: expected position %d, got %d", i, i, tok.Position)
        }
    }
}

func TestWhitespaceTokenizerPositions(t *testing.T) {
    tokenizer := NewWhitespaceTokenizer()
    tokens, _ := tokenizer.Tokenize(strings.NewReader("hello world"))

    if tokens[0].StartOffset != 0 || tokens[0].EndOffset != 5 {
        t.Errorf("token 0 offsets: expected [0,5], got [%d,%d]",
            tokens[0].StartOffset, tokens[0].EndOffset)
    }
    if tokens[1].StartOffset != 6 || tokens[1].EndOffset != 11 {
        t.Errorf("token 1 offsets: expected [6,11], got [%d,%d]",
            tokens[1].StartOffset, tokens[1].EndOffset)
    }
}
```

---

## 深掘り：なぜこう設計されているのか

### Q: なぜ Lucene は Attribute パターンを使うのか？

Lucene の TokenStream は Java のジェネリクスや構造体の制約を回避するため、`AttributeSource` という仕組みで動的に属性を追加できるようにしています。これにより、カスタム属性（ペイロード、シノニム情報など）を柔軟に追加できます。

Go では struct のフィールドで十分なので、Token struct に必要な情報を直接持たせます。

### Q: Position と Offset の違いは？

- **Position**: トークンの論理的な位置。フレーズ検索（`"quick fox"` → position が連続しているか）に使う
- **Offset**: 元テキスト中のバイト位置。ハイライト表示（検索結果のどこがマッチしたか表示する）に使う

シノニムがあると Position は同じになることがある（例: `"NYC"` と `"New York City"` は同じ Position に展開される）。

### Q: 日本語はどうするのか？

日本語は空白で単語が区切られないため、WhitespaceTokenizer では対応できません。Lucene では `JapaneseAnalyzer`（kuromoji）を使います。この学習では英語テキストで進めますが、将来的に Go の形態素解析器（kagome など）を組み合わせることも可能です。

---

## 次のステップ

Analyzer ができたので、次は [Step 2: 転置インデックス](02-inverted-index.md) で、トークンからドキュメントを逆引きできる転置インデックスを構築します。
