---
title: Full-Text Search Fundamentals
tags: search, indexing, information-retrieval
category: concepts
priority: 7
---
Full-text search allows users to find documents by matching words or phrases in their content, rather than requiring exact field matches.

The core data structure is the inverted index, which maps each term to the list of documents containing it. When a user searches for "error handling", the engine looks up both terms in the index and intersects the results.

Key components of a search engine include:

- Analyzer: tokenizes and normalizes text (lowercasing, stemming, stop-word removal)
- Inverted index: maps terms to document postings
- Scoring: ranks results by relevance using algorithms like BM25 or TF-IDF
- Query parser: translates user queries into structured search operations

BM25 is the most widely used scoring function. It considers term frequency, document length, and collection-wide statistics to produce relevance scores.
