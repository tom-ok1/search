---
title: Building an Inverted Index
tags: search, indexing, data-structures
category: concepts
priority: 6
---
An inverted index stores a mapping from terms to the documents that contain them. Each entry in the index is called a posting, and the list of postings for a term is called a postings list.

A simple in-memory inverted index can be implemented with a map:

    type Index map[string][]Posting

    type Posting struct {
        DocID     int
        Frequency int
        Positions []int
    }

Positions enable phrase queries by checking that terms appear consecutively in a document.

For persistence, postings lists are typically serialized to disk using variable-length encoding (varint) to compress document IDs and offsets. Delta encoding further reduces size by storing differences between consecutive document IDs rather than absolute values.

Segment-based architectures write immutable index segments and merge them periodically, similar to how LSM-trees work in databases.
