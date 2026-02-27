# Step 6: ディスク永続化

## 学ぶ概念

ここまではインメモリで全てを処理してきましたが、実用的な検索エンジンではデータをディスクに永続化する必要があります。Lucene の Segment はディスク上の複数のファイルとして保存されます。

### Lucene のファイル構成

1つの Segment は以下のようなファイル群で構成されます：

```
index/
├── segments_N          # コミットポイント（全 Segment の一覧）
├── _0.si               # Segment Info（メタデータ）
├── _0.doc              # Postings: DocID + Freq
├── _0.pos              # Postings: Positions
├── _0.tim              # Term Dictionary（term → postings へのポインタ）
├── _0.tip              # Term Index（term の索引、高速ルックアップ用）
├── _0.fdt              # Stored Fields Data
├── _0.fdx              # Stored Fields Index
├── _0.nvd              # Norms Values（フィールド長の正規化値）
├── _0.nvm              # Norms Metadata
├── _0.dvd              # Doc Values Data
├── _0.dvm              # Doc Values Metadata
├── _0_Lucene90_0.liv   # Live Docs（削除情報）
└── _0.cfs / _0.cfe     # Compound File（小さいファイルをまとめたもの）
```

### Directory 抽象化

Lucene ではファイル I/O を **Directory** というインターフェースで抽象化しています：

| 実装 | 用途 |
|------|------|
| `FSDirectory` | 通常のファイルシステム |
| `MMapDirectory` | メモリマップドファイル（デフォルト・最速） |
| `ByteBuffersDirectory` | インメモリ（テスト用） |
| `NRTCachingDirectory` | NRT 用キャッシュ |

### 我々の簡易フォーマット

Lucene のファイルフォーマットは非常に複雑（圧縮、ブロック構造、Skip List など）なので、まず **学習用の簡易フォーマット** で永続化を実装します。

```
_seg0/
├── segment.meta        # Segment メタデータ（JSON）
├── field_body.postings  # body フィールドの転置インデックス
├── field_body.lengths   # body フィールドのフィールド長
├── stored.data          # Stored fields
└── deleted.bits         # 削除ビットマップ
```

---

## Lucene ソースを読む

### 読むべきファイル

| ファイル | ポイント |
|----------|----------|
| `store/Directory.java` | ファイル I/O の抽象化。`createOutput()`, `openInput()` |
| `store/IndexInput.java` | 読み込みストリーム。`readByte()`, `readInt()`, `readVInt()` |
| `store/IndexOutput.java` | 書き込みストリーム。`writeByte()`, `writeInt()`, `writeVInt()` |
| `store/FSDirectory.java` | ファイルシステムベースの Directory |
| `store/MMapDirectory.java` | mmap ベース。OS のページキャッシュを最大活用 |
| `store/IOContext.java` | I/O のコンテキスト（マージ用、フラッシュ用など） |
| `codecs/Codec.java` | ファイルフォーマットの抽象化。Postings, StoredFields, DocValues など |
| `codecs/lucene104/Lucene104Codec.java` | 最新の Codec 実装 |
| `codecs/PostingsFormat.java` | Postings のファイルフォーマット定義 |
| `codecs/StoredFieldsFormat.java` | Stored Fields のファイルフォーマット定義 |
| `index/SegmentInfos.java` | `segments_N` ファイルの読み書き |
| `index/SegmentInfo.java` | `.si` ファイルの内容 |

### VInt（可変長整数）

Lucene のファイルフォーマットで最も基本的なデータ型が **VInt** です。小さい整数を少ないバイトで表現します：

```
値: 0〜127     → 1バイト
値: 128〜16383 → 2バイト
値: ...         → 最大5バイト

エンコード方式:
- 各バイトの下位7ビットがデータ
- 最上位ビットが「次のバイトがあるか」フラグ
  - 0: このバイトで終了
  - 1: 次のバイトに続く

例: 130 = 0b10000010
  → バイト1: 0b10000010 (下位7ビット: 0000010, 続くフラグ: 1)
  → バイト2: 0b00000001 (下位7ビット: 0000001, 続くフラグ: 0)
  → 復元: (0000010) | (0000001 << 7) = 2 + 128 = 130
```

---

## Go で実装する

### 1. Directory インターフェース

```go
// store/directory.go

package store

import (
    "io"
    "os"
    "path/filepath"
)

// Directory はファイル I/O の抽象化。
type Directory interface {
    // CreateOutput は書き込み用のストリームを作成する。
    CreateOutput(name string) (IndexOutput, error)
    // OpenInput は読み込み用のストリームを作成する。
    OpenInput(name string) (IndexInput, error)
    // ListAll はディレクトリ内の全ファイル名を返す。
    ListAll() ([]string, error)
    // DeleteFile はファイルを削除する。
    DeleteFile(name string) error
    // FileExists はファイルが存在するか確認する。
    FileExists(name string) bool
}

// IndexOutput は書き込み用のストリーム。
type IndexOutput interface {
    io.Writer
    // WriteVInt は可変長整数を書き込む。
    WriteVInt(v int) error
    Close() error
}

// IndexInput は読み込み用のストリーム。
type IndexInput interface {
    io.Reader
    // ReadVInt は可変長整数を読み込む。
    ReadVInt() (int, error)
    Close() error
}
```

### 2. FSDirectory 実装

```go
// store/fsdirectory.go

package store

import (
    "encoding/binary"
    "io"
    "os"
    "path/filepath"
)

// FSDirectory はファイルシステムベースの Directory。
type FSDirectory struct {
    path string
}

func NewFSDirectory(path string) (*FSDirectory, error) {
    if err := os.MkdirAll(path, 0755); err != nil {
        return nil, err
    }
    return &FSDirectory{path: path}, nil
}

func (d *FSDirectory) CreateOutput(name string) (IndexOutput, error) {
    f, err := os.Create(filepath.Join(d.path, name))
    if err != nil {
        return nil, err
    }
    return &fsIndexOutput{file: f}, nil
}

func (d *FSDirectory) OpenInput(name string) (IndexInput, error) {
    f, err := os.Open(filepath.Join(d.path, name))
    if err != nil {
        return nil, err
    }
    return &fsIndexInput{file: f}, nil
}

func (d *FSDirectory) ListAll() ([]string, error) {
    entries, err := os.ReadDir(d.path)
    if err != nil {
        return nil, err
    }
    var names []string
    for _, e := range entries {
        names = append(names, e.Name())
    }
    return names, nil
}

func (d *FSDirectory) DeleteFile(name string) error {
    return os.Remove(filepath.Join(d.path, name))
}

func (d *FSDirectory) FileExists(name string) bool {
    _, err := os.Stat(filepath.Join(d.path, name))
    return err == nil
}

// --- IndexOutput ---

type fsIndexOutput struct {
    file *os.File
}

func (o *fsIndexOutput) Write(p []byte) (int, error) {
    return o.file.Write(p)
}

func (o *fsIndexOutput) WriteVInt(v int) error {
    var buf [binary.MaxVarintLen64]byte
    n := binary.PutUvarint(buf[:], uint64(v))
    _, err := o.file.Write(buf[:n])
    return err
}

func (o *fsIndexOutput) Close() error {
    return o.file.Close()
}

// --- IndexInput ---

type fsIndexInput struct {
    file *os.File
}

func (in *fsIndexInput) Read(p []byte) (int, error) {
    return in.file.Read(p)
}

func (in *fsIndexInput) ReadVInt() (int, error) {
    val, err := binary.ReadUvarint(newByteReader(in.file))
    return int(val), err
}

func (in *fsIndexInput) Close() error {
    return in.file.Close()
}

// byteReader は io.Reader を io.ByteReader に変換するアダプタ。
type byteReader struct {
    r   io.Reader
    buf [1]byte
}

func newByteReader(r io.Reader) *byteReader {
    return &byteReader{r: r}
}

func (br *byteReader) ReadByte() (byte, error) {
    _, err := br.r.Read(br.buf[:])
    return br.buf[0], err
}
```

### 3. Segment の書き出し

```go
// index/segment_writer.go

package index

import (
    "encoding/json"
    "fmt"
    "gosearch/store"
)

// SegmentMeta は Segment のメタデータ（JSON で保存）。
type SegmentMeta struct {
    Name     string   `json:"name"`
    DocCount int      `json:"doc_count"`
    Fields   []string `json:"fields"`
}

// WriteSegment は Segment をディスクに書き出す。
func WriteSegment(dir store.Directory, seg *Segment) error {
    // 1. メタデータの書き出し
    meta := SegmentMeta{
        Name:     seg.name,
        DocCount: seg.docCount,
    }
    for fieldName := range seg.fields {
        meta.Fields = append(meta.Fields, fieldName)
    }

    metaOut, err := dir.CreateOutput(seg.name + ".meta")
    if err != nil {
        return err
    }
    metaBytes, _ := json.Marshal(meta)
    metaOut.Write(metaBytes)
    metaOut.Close()

    // 2. 各フィールドの Postings を書き出し
    for fieldName, fi := range seg.fields {
        if err := writeFieldPostings(dir, seg.name, fieldName, fi); err != nil {
            return err
        }
    }

    // 3. Stored fields の書き出し
    if err := writeStoredFields(dir, seg); err != nil {
        return err
    }

    // 4. フィールド長の書き出し
    for fieldName, lengths := range seg.fieldLengths {
        if err := writeFieldLengths(dir, seg.name, fieldName, lengths); err != nil {
            return err
        }
    }

    return nil
}

// writeFieldPostings はフィールドの転置インデックスを書き出す。
// フォーマット:
//   [term_count: VInt]
//   for each term:
//     [term_len: VInt][term_bytes: bytes]
//     [posting_count: VInt]
//     for each posting:
//       [doc_id: VInt][freq: VInt]
//       [position_count: VInt]
//       for each position:
//         [position: VInt]
func writeFieldPostings(dir store.Directory, segName, fieldName string, fi *FieldIndex) error {
    out, err := dir.CreateOutput(fmt.Sprintf("%s.field_%s.postings", segName, fieldName))
    if err != nil {
        return err
    }
    defer out.Close()

    out.WriteVInt(len(fi.postings))

    for term, pl := range fi.postings {
        // term の書き出し
        termBytes := []byte(term)
        out.WriteVInt(len(termBytes))
        out.Write(termBytes)

        // postings の書き出し
        out.WriteVInt(len(pl.Postings))
        for _, posting := range pl.Postings {
            out.WriteVInt(posting.DocID)
            out.WriteVInt(posting.Freq)
            out.WriteVInt(len(posting.Positions))
            for _, pos := range posting.Positions {
                out.WriteVInt(pos)
            }
        }
    }

    return nil
}

// writeStoredFields は Stored fields を書き出す。
func writeStoredFields(dir store.Directory, seg *Segment) error {
    out, err := dir.CreateOutput(seg.name + ".stored")
    if err != nil {
        return err
    }
    defer out.Close()

    out.WriteVInt(len(seg.storedFields))
    for docID, fields := range seg.storedFields {
        out.WriteVInt(docID)
        out.WriteVInt(len(fields))
        for name, value := range fields {
            nameBytes := []byte(name)
            out.WriteVInt(len(nameBytes))
            out.Write(nameBytes)
            valueBytes := []byte(value)
            out.WriteVInt(len(valueBytes))
            out.Write(valueBytes)
        }
    }

    return nil
}

// writeFieldLengths はフィールド長を書き出す。
func writeFieldLengths(dir store.Directory, segName, fieldName string, lengths []int) error {
    out, err := dir.CreateOutput(fmt.Sprintf("%s.field_%s.lengths", segName, fieldName))
    if err != nil {
        return err
    }
    defer out.Close()

    out.WriteVInt(len(lengths))
    for _, l := range lengths {
        out.WriteVInt(l)
    }

    return nil
}
```

### 4. Segment の読み込み

```go
// index/segment_reader.go

package index

import (
    "encoding/json"
    "fmt"
    "gosearch/store"
    "io"
)

// ReadSegment はディスクから Segment を読み込む。
func ReadSegment(dir store.Directory, segName string) (*Segment, error) {
    // 1. メタデータの読み込み
    metaIn, err := dir.OpenInput(segName + ".meta")
    if err != nil {
        return nil, err
    }
    defer metaIn.Close()

    metaBytes, _ := io.ReadAll(metaIn)
    var meta SegmentMeta
    if err := json.Unmarshal(metaBytes, &meta); err != nil {
        return nil, err
    }

    seg := newSegment(meta.Name)
    seg.docCount = meta.DocCount

    // 2. 各フィールドの Postings を読み込み
    for _, fieldName := range meta.Fields {
        fi, err := readFieldPostings(dir, segName, fieldName)
        if err != nil {
            return nil, err
        }
        seg.fields[fieldName] = fi
    }

    // 3. Stored fields の読み込み
    if err := readStoredFields(dir, seg); err != nil {
        return nil, err
    }

    // 4. フィールド長の読み込み
    for _, fieldName := range meta.Fields {
        lengths, err := readFieldLengths(dir, segName, fieldName)
        if err != nil {
            continue // フィールド長がない場合はスキップ
        }
        seg.fieldLengths[fieldName] = lengths
    }

    return seg, nil
}

func readFieldPostings(dir store.Directory, segName, fieldName string) (*FieldIndex, error) {
    in, err := dir.OpenInput(fmt.Sprintf("%s.field_%s.postings", segName, fieldName))
    if err != nil {
        return nil, err
    }
    defer in.Close()

    fi := newFieldIndex()

    termCount, err := in.ReadVInt()
    if err != nil {
        return nil, err
    }

    for i := 0; i < termCount; i++ {
        // term の読み込み
        termLen, _ := in.ReadVInt()
        termBytes := make([]byte, termLen)
        in.Read(termBytes)
        term := string(termBytes)

        // postings の読み込み
        postingCount, _ := in.ReadVInt()
        pl := &PostingsList{Term: term}

        for j := 0; j < postingCount; j++ {
            docID, _ := in.ReadVInt()
            freq, _ := in.ReadVInt()
            posCount, _ := in.ReadVInt()

            positions := make([]int, posCount)
            for k := 0; k < posCount; k++ {
                positions[k], _ = in.ReadVInt()
            }

            pl.Postings = append(pl.Postings, Posting{
                DocID:     docID,
                Freq:      freq,
                Positions: positions,
            })
        }

        fi.postings[term] = pl
    }

    return fi, nil
}

func readStoredFields(dir store.Directory, seg *Segment) error {
    in, err := dir.OpenInput(seg.name + ".stored")
    if err != nil {
        return err
    }
    defer in.Close()

    docCount, _ := in.ReadVInt()
    for i := 0; i < docCount; i++ {
        docID, _ := in.ReadVInt()
        fieldCount, _ := in.ReadVInt()

        fields := make(map[string]string)
        for j := 0; j < fieldCount; j++ {
            nameLen, _ := in.ReadVInt()
            nameBytes := make([]byte, nameLen)
            in.Read(nameBytes)

            valueLen, _ := in.ReadVInt()
            valueBytes := make([]byte, valueLen)
            in.Read(valueBytes)

            fields[string(nameBytes)] = string(valueBytes)
        }
        seg.storedFields[docID] = fields
    }

    return nil
}

func readFieldLengths(dir store.Directory, segName, fieldName string) ([]int, error) {
    in, err := dir.OpenInput(fmt.Sprintf("%s.field_%s.lengths", segName, fieldName))
    if err != nil {
        return nil, err
    }
    defer in.Close()

    count, _ := in.ReadVInt()
    lengths := make([]int, count)
    for i := 0; i < count; i++ {
        lengths[i], _ = in.ReadVInt()
    }

    return lengths, nil
}
```

---

## 確認・テスト

```go
// index/persistence_test.go

package index

import (
    "os"
    "testing"

    "gosearch/analysis"
    "gosearch/document"
    "gosearch/store"
)

func TestWriteAndReadSegment(t *testing.T) {
    // テスト用の一時ディレクトリ
    tmpDir, err := os.MkdirTemp("", "gosearch-test-*")
    if err != nil {
        t.Fatal(err)
    }
    defer os.RemoveAll(tmpDir)

    dir, _ := store.NewFSDirectory(tmpDir)

    // Segment を作成
    analyzer := analysis.NewAnalyzer(
        analysis.NewWhitespaceTokenizer(),
        &analysis.LowerCaseFilter{},
    )
    writer := NewIndexWriter(analyzer, 100)

    doc0 := document.NewDocument()
    doc0.AddField("title", "The Quick Brown Fox", document.FieldTypeText)
    writer.AddDocument(doc0)

    doc1 := document.NewDocument()
    doc1.AddField("title", "The Lazy Dog", document.FieldTypeText)
    writer.AddDocument(doc1)

    writer.Flush()

    // 書き出し
    seg := writer.Segments()[0]
    if err := WriteSegment(dir, seg); err != nil {
        t.Fatal(err)
    }

    // 読み込み
    readSeg, err := ReadSegment(dir, seg.name)
    if err != nil {
        t.Fatal(err)
    }

    // 検証
    if readSeg.docCount != 2 {
        t.Errorf("expected 2 docs, got %d", readSeg.docCount)
    }

    // "fox" の postings が正しく読めるか
    pl := readSeg.fields["title"].postings["fox"]
    if pl == nil {
        t.Fatal("expected postings for 'fox'")
    }
    if len(pl.Postings) != 1 || pl.Postings[0].DocID != 0 {
        t.Errorf("unexpected postings for 'fox': %+v", pl.Postings)
    }

    // stored fields が正しく読めるか
    stored := readSeg.storedFields[0]
    if stored["title"] != "The Quick Brown Fox" {
        t.Errorf("expected original text, got %q", stored["title"])
    }
}
```

---

## 深掘り：なぜこう設計されているのか

### Q: なぜ Lucene は MMapDirectory をデフォルトにしているのか？

`MMapDirectory` はファイルをメモリにマップ（mmap）することで、OS のページキャッシュを直接活用します。これにより：

- 明示的なキャッシュ管理が不要（OS に任せる）
- 読み込みがメモリアクセスと同じ速度になる（ページがキャッシュにある場合）
- Java のヒープを消費しない（GC の負荷が減る）

Go で同様のことを実現するには `syscall.Mmap` または `golang.org/x/exp/mmap` を使います。

### Q: なぜ Lucene は VInt を多用するのか？

転置インデックスでは DocID や Position が大量に記録されます。これらの値は多くの場合小さい整数なので、固定長の int32（4バイト）ではなく VInt（1〜5バイト）で保存するとファイルサイズが大幅に削減されます。

### Q: Compound File System (.cfs) とは？

小さな Segment では多数のファイル（.doc, .pos, .tim, .tip, ...）が作られます。ファイル数が多いと OS のファイルディスクリプタを消費します。Compound File は全ファイルを1つにまとめることでこの問題を解決します。

大きな Segment ではファイルを分けたほうが効率的なので、Segment サイズに応じて使い分けます。

---

## 次のステップ

ディスク永続化ができたので、次は [Step 7: Segment Merge](07-merge.md) で、複数の小さな Segment を統合する仕組みを学びます。
