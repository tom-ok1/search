---
title: Understanding BM25 Scoring
tags: search, bm25, scoring, information-retrieval
category: concepts
priority: 6
---
BM25 (Best Matching 25) is a ranking function used by search engines to estimate the relevance of documents to a given query. It is an evolution of the TF-IDF weighting scheme.

The BM25 formula considers three factors:

1. Term frequency (TF): how often the term appears in the document, with diminishing returns for repeated occurrences
2. Inverse document frequency (IDF): terms that appear in fewer documents are weighted higher
3. Document length normalization: longer documents are slightly penalized to avoid bias

The two tuning parameters are k1 (controls term frequency saturation, typically 1.2) and b (controls length normalization, typically 0.75).

BM25 works well out of the box for most text retrieval tasks. It remains competitive with more complex neural ranking models for keyword-based search, especially when combined with good text analysis.
