## Phrase Queryの改善案

理解は合っています。整理すると：

postingsLists は phrase query の各 term に対応するポスティングリストの配列（例: "hello world" なら [helloのPostingsList, worldのPostingsList]）
matchPositions でポジションが連続しているかを判定して絞り込んでいる
そして指摘の通り、今の実装には無駄があります。findPosting が 同じ docID に対して2回呼ばれている のが問題です：

1回目: matchPositions 内の66行目 で各termの posting を取得
2回目: スコアリングループの49行目 でまた同じ posting を取得
提案のように matchPositions から使った posting を返すようにすれば、2回目の findPosting を完全に省略できます。例えばこういうイメージです：

// boolではなくマッチしたpostingのスライスを返す
func (q *PhraseQuery) matchPositions(postingsLists []*index.PostingsList, docID int) []*index.Posting {
var postings []*index.Posting
for \_, pl := range postingsLists {
posting := findPosting(pl, docID)
if posting == nil {
return nil
}
postings = append(postings, posting)
}

    for _, startPos := range postings[0].Positions {
        matched := true
        for i := 1; i < len(postings); i++ {
            if !containsInt(postings[i].Positions, startPos+i) {
                matched = false
                break
            }
        }
        if matched {
            return postings // マッチした posting をそのまま返す
        }
    }
    return nil

}
呼び出し側はこうなります：

for \_, docID := range commonDocs {
matchedPostings := q.matchPositions(postingsLists, docID)
if matchedPostings == nil {
continue
}
totalScore := 0.0
docLen := float64(idx.FieldLength(q.Field, docID))
for i, posting := range matchedPostings {
idf := scorer.IDF(docCount, len(postingsLists[i].Postings))
totalScore += scorer.Score(float64(posting.Freq), docLen, avgDocLen, idf)
}
results = append(results, DocScore{DocID: docID, Score: totalScore})
}
matchedPostings[i] が postingsLists[i] に対応するので、IDF計算の len(postingsLists[i].Postings) もそのまま使えます。findPosting の呼び出し回数が term数 × doc数 分だけ半減するので、特にフレーズが長い場合やマッチするドキュメントが多い場合に効果があります。

良い着眼点だと思います。実装しますか？

---

このphrase queryはもしphraseが長い場合全てのtermに対して、postingsをまず集めてその後にcommonを探す感じになる？

はい、その通りです。現在の実装は以下の流れになっています：

全termのpostingsを先に全部集める (phrase_query.go:25-32) — q.Terms をループして、各termの PostingsList をすべてメモリ上に取得
共通DocIDを探す (phrase_query.go:35) — findCommonDocs で全PostingsListに共通するDocIDをmap intersectionで算出
位置チェック (phrase_query.go:43) — 共通DocIDに対してのみ matchPositions で連続位置を確認
phraseが長い場合の問題点：

メモリ: 全termのPostingsListを同時に保持する。頻出語（"the", "of" など）が含まれると、そのpostingsリストが巨大になる
findCommonDocsの効率: map[int]bool によるintersectionなので、ソート済みリストのマージと比べると効率が悪い。特にterm数が多いとmapの生成・破棄が繰り返される
findPostingの線形探索: phrase_query.go:116-121 で毎回線形探索しているため、postingsが大きいと遅い
一般的な最適化としては：

最もレアなtermから始める — postingsが最小のtermを先頭にしてintersectionすれば、候補を早く絞れる
ソート済みリストのマージ — mapではなく、DocIDでソートされたpostingsリストを2-pointer mergeで交差させる（O(n)）
段階的フィルタリング — 全termのpostingsを集めてからではなく、2つずつintersection → position checkを進めていく方法もある

matchPositionsの今の実装
ここでは
the: [0, 5, 12, 105, ...]
brown: [3, 106 ...]
fox: [4, 14, 107 ...]

みたいな例だとして、theの次に今は
lead.posision = 0から始める
1をbrownが持っているか（線形探索） -> ないならlead.position = 5から

だけどlead.position = 0で
brown.advance(0)みたいな感じで advance value > 0 + 1　みたいなのを特定して進めると早い
brown.advanceの結果が例えば100だった場合、the.advance(100) -> the.postiion(105)とかまで飛ばせる
brown.advance(105) = the.position + 1 -> ok
foxに対して同じことをする
これだと計算量がかなり減る

---

fsDirectory では毎回syscallしてfileからユーザー空間にメモリをマップしているけど、Mmapして参照更新すればsyscallのオーバーヘッドを減らせる
