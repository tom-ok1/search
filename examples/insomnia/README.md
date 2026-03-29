# GoSearch API - Insomnia テスト環境

Insomnia を使って GoSearch の API を手動テストするための環境です。

## セットアップ

### 1. サーバー起動（サンプルデータ自動投入）

```bash
./start.sh
```

以下が自動で行われます:
- gosearch のビルド
- サーバー起動（port 9200）
- `books` インデックス作成（mappings 付き）
- サンプルドキュメント 5 件をバルク投入
- インデックスの refresh

### 2. Insomnia にコレクションをインポート

1. Insomnia を開く
2. **Application** → **Preferences** → **Data** → **Import Data**（または `Ctrl+Shift+I`）
3. `insomnia_collection.json` を選択

環境変数 `base_url` = `http://localhost:9200`、`index` = `books` が設定済みです。

### 3. サーバー停止 & クリーンアップ

```bash
./stop.sh
```

サーバー停止に加え、以下を自動削除します:
- `data/` (インデックスデータ)
- `gosearch` バイナリ

## サンプルデータ

`books` インデックスに 5 冊の書籍データが入ります:

| ID | Title | Author | Year |
|----|-------|--------|------|
| 1 | The Go Programming Language | Alan Donovan | 2015 |
| 2 | Designing Data-Intensive Applications | Martin Kleppmann | 2017 |
| 3 | Introduction to Information Retrieval | Christopher Manning | 2008 |
| 4 | Lucene in Action | Michael McCandless | 2010 |
| 5 | Search Engines Information Retrieval in Practice | Bruce Croft | 2009 |

## Insomnia コレクション内容

### Index Management
- **Create Index** - `PUT /books`
- **Get Index** - `GET /books`
- **Delete Index** - `DELETE /books`

### Document Operations
- **Index Document** - `PUT /books/_doc/1`
- **Get Document** - `GET /books/_doc/1`
- **Delete Document** - `DELETE /books/_doc/1`
- **Refresh Index** - `POST /books/_refresh`

### Search
- **Match** - `POST /books/_search` (全文検索)
- **Term** - `POST /books/_search` (完全一致)
- **Match All** - `POST /books/_search` (全件取得)
- **Bool Query** - `POST /books/_search` (複合クエリ)

### Bulk Operations
- **Bulk Index** - `POST /_bulk` (複数ドキュメント一括投入)
- **Bulk Delete** - `POST /_bulk` (複数ドキュメント一括削除)

## ファイル構成

```
examples/insomnia/
├── README.md                  # このファイル
├── start.sh                   # サーバー起動 + サンプルデータ投入
├── stop.sh                    # サーバー停止 + クリーンアップ
├── sample_data.ndjson         # サンプルデータ (NDJSON)
└── insomnia_collection.json   # Insomnia コレクション
```
