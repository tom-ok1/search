---
title: Legacy System Migration Notes
tags: legacy, migration, deprecated
category: internal
priority: 2
---
The old search service uses Elasticsearch 5.x which reached end of life. We need to migrate to the new Go-based search engine.

Migration steps:
1. Export existing index data from Elasticsearch
2. Transform documents to the new schema format
3. Re-index all documents using the new engine
4. Run parallel queries to verify result parity
5. Switch traffic with a feature flag

Known issues with the legacy system:
- Memory usage spikes during bulk indexing
- Analyzer configuration is inconsistent across indices
- No support for custom scoring functions

Target completion: end of quarter. The legacy endpoints will be deprecated once the new system is validated in production.
